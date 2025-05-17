use crossbeam::channel::{Receiver, Sender, unbounded};
use portable_pty::{CommandBuilder, MasterPty, PtySize, SlavePty, native_pty_system};
use serde::{Deserialize, Serialize};
use std::{cell::Cell, io::Read, time::Duration};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Pty {
    reader: PtyReader,
    tx_write: Sender<String>,
    // keep the slave alive
    // so windows works
    // https://github.com/wez/wezterm/issues/4206
    _slave: Box<dyn SlavePty + Send>,
    master: Box<dyn MasterPty + Send>,
}

#[derive(Clone)]
struct PtyReader {
    rx_read: Receiver<Message>,
    done: Cell<bool>,
}
impl PtyReader {
    fn new(rx_read: Receiver<Message>) -> PtyReader {
        Self {
            rx_read,
            done: Cell::new(false),
        }
    }
    //NOTE: this function should not block
    fn read(&self) -> Result<Message> {
        if self.done.get() {
            return Ok(Message::End);
        }

        let mut msgs: Vec<_> = self.rx_read.try_iter().collect();

        if msgs.contains(&Message::End) {
            self.done.set(true);

            // NOTE: We received the END message, this means that the process has exited
            // But there could be some pending messages in the read channel, this is especisally true in windows
            // So sleep a bit and check the channel again
            std::thread::sleep(Duration::from_millis(100));
            msgs.extend(self.rx_read.try_iter());

            if msgs.len() == 1 {
                return Ok(Message::End);
            }

            // we might have some msgs here
            // we should send them to the user
            msgs.retain(|msg| !matches!(msg, Message::End));
        }

        let msg = msgs
            .iter()
            .map(|msg| {
                if let Message::Data(data) = msg {
                    data.as_str()
                } else {
                    unreachable!()
                }
            })
            .collect::<Vec<_>>()
            .join("");

        Ok(Message::Data(msg))
    }
}

#[derive(Serialize, Deserialize)]
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
            // Not all systems support pixel_width, pixel_height,
            // but it is good practice to set it to something
            // that matches the size of the selected font.  That
            // is more complex than can be shown here in this
            // brief example though!
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

        let (tx_read, rx_read) = unbounded();

        let mut child = pair.slave.spawn_command(cmd)?;
        dbg!("after spawn command");

        // If we do a pty.read after the process exit, read will hang
        // Thats why we spawn another thread to wait for the child
        // and signal its exit
        let tx_read_c = tx_read.clone();
        std::thread::spawn(move || {
            let _ = child.wait();
            let _ = tx_read_c.send(Message::End);
        });

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

        let mut writer = pair.master.take_writer()?;
        let (tx_write, rx_write): (Sender<String>, _) = unbounded();
        std::thread::spawn(move || {
            while let Ok(buf) = rx_write.recv() {
                writer
                    .write_all(&buf.into_bytes())
                    .expect("failed to write data");
            }
        });

        Ok(Self {
            reader: PtyReader::new(rx_read),
            tx_write,
            _slave: pair.slave,
            master: pair.master,
        })
    }

    #[allow(dead_code)]
    fn clone_reader(&self) -> PtyReader {
        self.reader.clone()
    }

    pub fn read(&self) -> Result<Message> {
        self.reader.read()
    }

    pub fn write(&self, data: String) -> Result<()> {
        Ok(self.tx_write.send(data)?)
    }

    pub fn resize(&self, size: PtySize) -> Result<()> {
        self.master.resize(size).map_err(Into::into)
    }

    pub fn get_size(&self) -> Result<PtySize> {
        self.master.get_size().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn it_works() {
        dbg!("here");
        let pty = Pty::create(Command {
            cmd: "deno".into(),
            args: vec!["repl".into()],
            env: vec![("NO_COLOR".into(), "1".into())],
            cwd: None,
        })
        .unwrap();
        dbg!("after");

        // read header
        dbg!(pty.read().unwrap());
        std::thread::sleep(std::time::Duration::from_millis(500));
        dbg!(pty.read().unwrap());
        std::thread::sleep(std::time::Duration::from_millis(500));
        dbg!(pty.read().unwrap());
        std::thread::sleep(std::time::Duration::from_millis(500));
        dbg!(pty.read().unwrap());
        std::thread::sleep(std::time::Duration::from_millis(500));
        dbg!(pty.read().unwrap());

        // test size, resize
        assert!(matches!(
            pty.get_size(),
            Ok(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
        ));

        pty.resize(PtySize {
            rows: 50,
            cols: 120,
            pixel_width: 1,
            pixel_height: 1,
        })
        .unwrap();
        assert!(matches!(
            pty.get_size(),
            Ok(PtySize {
                rows: 50,
                cols: 120,
                pixel_width: 1,
                pixel_height: 1,
            })
        ));
    }
}
