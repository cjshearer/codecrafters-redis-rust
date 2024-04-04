mod command;
mod connection;
mod db;
mod frame;

use crate::command::Command;
use connection::Connection;
use db::Db;
use tokio::{self, net::TcpListener};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = Db::new();
    loop {
        let (mut stream, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            // TODO(cjshearer): pipelining https://redis.io/topics/pipelining
            let mut connection = Connection::new(&mut stream);
            loop {
                let frame = match connection.read_frame().await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break, // disconnect
                    Err(e) => {
                        println!("{:?}", e);
                        continue;
                        // todo!("send frame parsing error back to client");
                    }
                };
                let command: Command = match frame.try_into() {
                    Ok(command) => command,
                    Err(e) => {
                        println!("{:?}", e);
                        continue;
                        // todo!("send command parsing error back to client")
                    }
                };
                let result = db.apply(command);
                let _ = connection.write_frame(result).await;
            }
        });
    }
}
