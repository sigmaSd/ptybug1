use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use std::{io::Read, sync::mpsc::Receiver};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Pty {
    reader: PtyReader,
}

struct PtyReader {
    rx_read: Receiver<Message>,
}
impl PtyReader {
    fn new(rx_read: Receiver<Message>) -> PtyReader {
        Self { rx_read }
    }
    //NOTE: this function should not block
    fn read(&self) -> Result<Message> {
        self.rx_read.recv().map_err(|e| e.into())
    }
}

pub struct Command {
    cmd: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    cwd: Option<String>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Message {
    Data(String),
    End,
}

impl Pty {
    pub fn create(command: Command) -> Result<Self> {
        // Use the native pty implementation for the system
        let pty_system = native_pty_system();
        dbg!("a pty_system");

        // Create a new pty
        let pair = pty_system.openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        dbg!("a pair");

        let mut cmd = CommandBuilder::new(command.cmd);
        // https://github.com/wez/wezterm/issues/4205
        cmd.env("PATH", std::env::var("PATH")?);
        cmd.args(&command.args);
        match command.cwd {
            Some(cwd) => cmd.cwd(cwd),
            None => cmd.cwd(std::env::current_dir()?),
        }
        for env in command.env {
            cmd.env(env.0, env.1);
        }

        let (tx_read, rx_read) = std::sync::mpsc::channel();
        let tx_read_c = tx_read.clone();

        let mut child = pair.slave.spawn_command(cmd)?;
        drop(pair.slave);
        dbg!("after spawn command");

        // Read the output in another thread.
        // This is important because it is easy to encounter a situation
        // where read/write buffers fill and block either your process
        // or the spawned process.
        let mut reader = pair.master.try_clone_reader()?;
        std::thread::spawn(move || {
            let mut buf = [0; 512];
            loop {
                let n = reader.read(&mut buf).expect("failed to read data");
                dbg!("read n", n);
                if n == 0 {
                    // the pty has already exited
                    // so no need to send the end message?
                    break;
                };
                let d = String::from_utf8(buf[0..n].to_vec()).expect("data is not valid utf8");
                tx_read.send(Message::Data(d)).ok(); // the sender closed (the program finished ?);
            }
        });

        // If we do a pty.read after the process exit, read will hang
        // Thats why we spawn another thread to wait for the child
        // and signal its exit
        std::thread::spawn(move || {
            let _ = child.wait();
            // drop the master only after the child processe exits, otherwise issues will happen
            drop(pair.master);
            let _ = tx_read_c.send(Message::End);
        });

        Ok(Self {
            reader: PtyReader::new(rx_read),
        })
    }

    pub fn read(&self) -> Result<Message> {
        self.reader.read()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn it_works() {
        let pty = Pty::create(Command {
            cmd: if cfg!(windows) {
                "cd".into()
            } else {
                "pwd".into()
            },
            args: vec![],
            env: vec![],
            cwd: None,
        })
        .unwrap();
        loop {
            if dbg!(pty.read().unwrap()) == Message::End {
                break;
            }
        }
    }
}
