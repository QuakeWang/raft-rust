use crate::consensus::Consensus;
use std::sync::{Arc, Mutex};

mod config;
pub mod consensus;
mod log;
pub mod peer;
pub mod proto;
mod rpc;
pub mod state_machine;
mod timer;
mod util;

pub fn start(
    server_id: u64,
    port: u32,
    peers: Vec<peer::Peer>,
    state_machine: Box<dyn state_machine::StateMachine>,
) -> Arc<Mutex<Consensus>> {
    // TODO: Add config log
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let consensus = Consensus::new(server_id, port, peers, state_machine);

    let server = rpc::Server {
        consensus: consensus.clone(),
    };

    std::thread::spawn(move || {
        let addr = format!("[::1]:{}", port);
        if let Err(_) = rpc::start_server(addr.as_str(), server) {
            panic!("tonic rpc server started failed.");
        }
    });

    // Start timer
    let weak_consensus = Arc::downgrade(&consensus);
    consensus
        .lock()
        .unwrap()
        .heartbeat_timer
        .lock()
        .unwrap()
        .schedule(config::HEARTBEAT_INTERVAL, move || {
            weak_consensus
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .handle_heartbeat_timeout();
        });
    let weak_consensus = Arc::downgrade(&consensus);
    consensus
        .lock()
        .unwrap()
        .election_timer
        .lock()
        .unwrap()
        .schedule(util::rand_election_timeout(), move || {
            weak_consensus
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .handle_election_timeout();
        });
    let weak_consensus = Arc::downgrade(&consensus);
    consensus
        .lock()
        .unwrap()
        .snapshot_timer
        .lock()
        .unwrap()
        .schedule(config::SNAPSHOT_INTERVAL, move || {
            weak_consensus
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .handle_snapshot_timeout();
        });

    consensus
}

pub fn stop() {
    todo!()
}
