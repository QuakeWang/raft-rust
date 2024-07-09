use std::fmt::Debug;

pub trait StateMachine: Debug + Send + 'static {
    fn apply(&mut self, data: &Vec<u8>);

    fn take_snapshot(&mut self, snapshot_path: String);

    fn restore_snapshot(&mut self, snapshot_path: String);
}
