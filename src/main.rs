 use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}   

fn handle_client(mut stream: TcpStream) {
    let mut read_buffer = [0 as u8;512];

    loop {
        let Ok(_read_buffer) = stream.read(&mut read_buffer) else {
            continue;
        };
        let _ = stream.write_all(b"+PONG\r\n");
    }
}