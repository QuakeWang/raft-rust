#[derive(Debug)]
struct MyStateMachine {
    data: Vec<Vec<u8>>,
}

impl raft_rust::state_machine::StateMachine for MyStateMachine {
    fn apply(&mut self, data: &Vec<u8>) {
        self.data.push(data.clone());
    }
}

fn main() {
    println!("Hello World");
    let peers = vec![
        raft_rust::peer::Peer::new(2, "http://[::1]:9092".to_string()),
        raft_rust::peer::Peer::new(3, "http://[::1]:9093".to_string()),
    ];
    let state_machine = Box::new(MyStateMachine { data: Vec::new() });
    let consensus = raft_rust::start(3, 9093, peers, state_machine);
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
