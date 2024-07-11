use std::io::{Read, Write};

#[derive(Debug)]
struct MyStateMachine {
    data: Vec<Vec<u8>>,
}

impl raft_rust::state_machine::StateMachine for MyStateMachine {
    fn apply(&mut self, data: &Vec<u8>) {
        self.data.push(data.clone());
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
            let mut snapshot_file = std::fs::File::open(snapshot_filepath.clone()).unwrap();
            let mut snapshot_json = String::new();
            if let Err(e) = snapshot_file.read_to_string(&mut snapshot_json) {
                panic!("Failed to read snapshot file, err: {}.", e);
            }
            self.data = serde_json::from_str(&snapshot_json).unwrap();
        }
    }
}

fn main() {
    println!("Hello World");

    let peers = vec![];
    let state_machine = Box::new(MyStateMachine { data: Vec::new() });
    let snapshot_dir = format!(
        "{}/{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        ".snapshot/app_server4"
    );
    let metadata_dir = format!(
        "{}/{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        ".metadata/app_server4"
    );
    let consensus = raft_rust::start(4, 9094, peers, state_machine, snapshot_dir, metadata_dir);

    let mut count = 0;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(20));
        count += 1;

        let consensus = consensus.lock().unwrap();
        println!("consensus details: {:#?}", consensus);
    }
}
