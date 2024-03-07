use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    let mut bytes_read = 1;

    while bytes_read != 0 {
        bytes_read = stream.read(&mut buffer).unwrap_or(0);
        let _ = stream.write_all(b"+PONG\r\n");
    }
}
