fn main() {
    println!("Hello World");
    let peers = vec![
        raft_rust::peer::Peer::new(2, "http://[::1]:9092".to_string()),
        raft_rust::peer::Peer::new(3, "http://[::1]:9093".to_string()),
    ];
    let consensus = raft_rust::start(1, 9091, peers);
    loop {

    }
}