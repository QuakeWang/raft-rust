use std::sync::{Arc, Mutex};

fn main() {
    println!("Hello Raft 01!");
    let consensus: Arc<Mutex<raft_rust::consensus::Consensus>> = raft_rust::server::start();
    println!("Hello Raft 02!");
    consensus.lock().unwrap().replicate();
    loop {

    }
}
