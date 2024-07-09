use std::io::Write;

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

    fn restore_snapshot(&mut self, _snapshot_filepath: String) {}
}

fn main() {
    println!("Hello World");

    let peers = vec![];
    let state_machine = Box::new(MyStateMachine { data: Vec::new() });
    let snapshot_dir = format!(
        "{}/{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        "./app_server4/".to_string()
    );
    let consensus = raft_rust::start(4, 9094, peers, state_machine, snapshot_dir);

    let mut count = 0;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(20));
        count += 1;

        let consensus = consensus.lock().unwrap();
        println!("consensus details: {:#?}", consensus);
    }
}
