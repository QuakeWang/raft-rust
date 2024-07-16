use std::io::{Read, Write};

#[derive(Debug)]
struct MyStateMachine {
    data: Vec<Vec<u8>>,
}

impl raft_rust::state_machine::StateMachine for MyStateMachine {
    fn apply(&mut self, data: &[u8]) {
        self.data.push(Vec::from(data));
    }

    fn take_snapshot(&mut self, snapshot_filepath: String) {
        let mut snapshot_file = std::fs::File::create(snapshot_filepath.clone()).unwrap();
        let snapshot_json = serde_json::to_string(&self.data).unwrap();
        if let Err(e) = snapshot_file.write(snapshot_json.as_bytes()) {
            panic!("Failed to write snapshot file, err: {}.", e);
        }
    }

    fn restore_snapshot(&mut self, snapshot_filepath: String) {
        if std::path::Path::new(&snapshot_filepath).exists() {
            let mut snapshot_file = std::fs::File::open(snapshot_filepath).unwrap();
            let mut snapshot_json = String::new();
            snapshot_file
                .read_to_string(&mut snapshot_json)
                .expect("Failed to read snapshot.");
            self.data = serde_json::from_str(snapshot_json.as_str()).unwrap();
        }
    }
}

fn main() {
    println!("Hello World");
    let peers = vec![
        raft_rust::peer::Peer::new(1, "http://[::1]:9091".to_string()),
        raft_rust::peer::Peer::new(3, "http://[::1]:9093".to_string()),
    ];

    let state_machine = Box::new(MyStateMachine { data: Vec::new() });
    let snapshot_dir = format!(
        "{}/{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        ".snapshot/app_server2"
    );
    let metadata_dir = format!(
        "{}/{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        ".metadata/app_server2"
    );
    let consensus = raft_rust::start(2, 9092, peers, state_machine, snapshot_dir, metadata_dir);
    let mut count = 0;
    loop {
        std::thread::sleep(std::time::Duration::from_secs(20));
        count += 1;

        let mut consensus = consensus.lock().unwrap();

        if consensus.state == raft_rust::consensus::State::Leader {
            consensus
                .replicate(
                    raft_rust::proto::EntryType::Data,
                    format!("{}", count).as_bytes().to_vec(),
                )
                .unwrap();
        }
        println!("Consensus detail: {:#?}.", consensus);
    }
}
