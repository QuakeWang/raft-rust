fn main() {
    println!("Hello World");
    let peers = vec![
        raft_rust::peer::Peer::new(2, "http://[::1]:9092".to_string()),
        raft_rust::peer::Peer::new(3, "http://[::1]:9093".to_string()),
    ];
    let consensus = raft_rust::start(3, 9093, peers);
    let mut count = 0;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(20));
        count += 1;

        let mut consensus = consensus.lock().unwrap();
        if consensus.state == raft_rust::consensus::State::Leader {
            consensus
                .replicate(raft_rust::proto::EntryType::Data, format!("{}", count))
                .unwrap();
        }
        println!("Consensus detail: {:#?}.", consensus);
    }
}
