fn main () {
    println!("Hello Raft!");
    raft_rust::server::start("[::1]:9091");
}