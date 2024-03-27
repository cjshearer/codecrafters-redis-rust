mod command;
mod connection;
mod frame;

use crate::command::Command;
use connection::Connection;
use tokio::{self, net::TcpListener};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
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
                connection.write_frame(command.apply()).await;

                // // TODO(cjshearer): create a db object that this command is applied to
                // writer.write_all_buf(&mut command.apply());
                // // frame_result.write(&mut stream).await;
                // TODO(cjshearer): pipelining https://redis.io/topics/pipelining
                // writer.flush().await;

                // println!("{:?}", command);
            }
        });
    }
}
