use std::fmt::Debug;

pub trait StateMachine: Debug + Send + 'static {
    fn apply(&mut self, data: &Vec<u8>);

    fn take_snapshot(&mut self);

    fn restore_snapshot(&mut self);
}
