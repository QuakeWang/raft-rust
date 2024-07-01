#[derive(Debug)]
struct MyStateMachine {
    data: Vec<Vec<u8>>,
}

impl raft_rust::state_machine::StateMachine for MyStateMachine {
    fn apply(&mut self, data: &Vec<u8>) {
        self.data.push(data.clone());
    }

    fn take_snapshot(&mut self) {}

    fn restore_snapshot(&mut self) {}
}

fn main() {
    println!("Hello World");

    let peers = vec![];
    let state_machine = Box::new(MyStateMachine { data: Vec::new() });
    let consensus = raft_rust::start(4, 9094, peers, state_machine);

    let mut count = 0;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(20));
        count += 1;

        let mut consensus = consensus.lock().unwrap();
        println!("consensus details: {:#?}", consensus);
    }
}
