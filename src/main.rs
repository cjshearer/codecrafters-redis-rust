use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpListener,
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut connections = Vec::new();
    let mut buffer: Vec<u8> = Vec::new();

    loop {
        listener.set_nonblocking(connections.len() != 0).unwrap();
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true).unwrap();
                connections.push(BufReader::new(stream));
                println!("there are now {} connections", connections.len());
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => thread::yield_now(),
            Err(e) => {
                println!("connection failed: {}", e);
            }
        }

        connections.retain_mut(|reader| {
            buffer.clear();
            return match reader.read_until(b'\n', &mut buffer) {
                Ok(0) => false,
                Ok(_bytes_read) => {
                    println!("{}", String::from_utf8(buffer.clone()).unwrap());
                    reader.get_ref().write_all(b"+PONG\r\n").unwrap();
                    true
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::yield_now();
                    true
                }
                Err(e) => {
                    println!("{}", e);
                    false
                }
            };
        });
    }
}
