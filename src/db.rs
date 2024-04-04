use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::{command::Command, frame::Frame};

pub struct Db {
    state: Arc<Mutex<State>>,
}

struct State {
    keystore: HashMap<Bytes, Bytes>,
}

impl Db {
    /// Creates a new database
    pub fn new() -> Self {
        Db {
            state: Arc::new(Mutex::new(State {
                keystore: HashMap::new(),
            })),
        }
    }
}

impl Db {
    pub fn apply(&self, command: Command) -> Frame {
        match command {
            Command::Ping => Frame::Bulk(Some("PONG".into())),
            Command::Echo(s) => Frame::Bulk(Some(s.clone())),
            Command::Set([k, v]) => {
                let _ = self.state.lock().unwrap().keystore.insert(k, v);
                Frame::Bulk(Some("OK".into()))
            }
            Command::Get(k) => Frame::Bulk(
                self.state
                    .lock()
                    .unwrap()
                    .keystore
                    .get(&k)
                    .map(|b| b.clone()),
            ),
        }
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Db {
            state: self.state.clone(),
        }
    }
}
