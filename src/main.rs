 use std::{io::Write, net::{TcpListener, TcpStream}};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_stream(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}   

fn handle_stream(mut stream: TcpStream) {
    stream.write_all(b"+PONG\r\n").unwrap();
}