use crate::consensus::Consensus;
use ::log::error;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::writer::MakeWriterExt;

mod config;
pub mod consensus;
mod log;
mod metadata;
pub mod peer;
pub mod proto;
mod rpc;
pub mod snapshot;
pub mod state_machine;
mod timer;
mod util;

pub fn start(
    server_id: u64,
    port: u32,
    peers: Vec<peer::Peer>,
    state_machine: Box<dyn state_machine::StateMachine>,
    snapshot_dir: String,
    metadata_dir: String,
) -> Arc<Mutex<Consensus>> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }

    let file_appender = tracing_appender::rolling::hourly("./logs", "application.log");
    let all_appender = file_appender.and(std::io::stdout);

    tracing_subscriber::fmt().with_writer(all_appender).init();

    if !std::path::Path::new(&metadata_dir).exists() {
        if let Err(e) = std::fs::create_dir_all(metadata_dir.clone()) {
            panic!("Create metadata dir failed, error: {}", e);
        }
    }

    // If snapshot dir does not exist, create it.
    if !std::path::Path::new(&snapshot_dir).exists() {
        if let Err(e) = std::fs::create_dir_all(snapshot_dir.clone()) {
            panic!("Create snapshot dir failed, error: {}", e);
        }
    }

    let consensus = Arc::new(Mutex::new(Consensus::new(
        server_id,
        port,
        peers,
        state_machine,
        snapshot_dir,
        metadata_dir,
    )));

    let consensus_clone = consensus.clone();
    std::thread::spawn(move || {
        let addr = format!("[::1]:{}", port);
        if rpc::start_server(addr.as_str(), consensus_clone).is_err() {
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
            if let Some(consensus) = weak_consensus.upgrade() {
                consensus.lock().unwrap().handle_heartbeat_timeout();
            } else {
                error!("Heartbeat timer can not call after consensus is dropped.")
            }
        });
    let weak_consensus = Arc::downgrade(&consensus);
    consensus
        .lock()
        .unwrap()
        .election_timer
        .lock()
        .unwrap()
        .schedule(util::rand_election_timeout(), move || {
            if let Some(consensus) = weak_consensus.upgrade() {
                consensus.lock().unwrap().handle_election_timeout();
            } else {
                error!("Election timer can not call after consensus is dropped.")
            }
        });
    let weak_consensus = Arc::downgrade(&consensus);
    consensus
        .lock()
        .unwrap()
        .snapshot_timer
        .lock()
        .unwrap()
        .schedule(config::SNAPSHOT_INTERVAL, move || {
            if let Some(consensus) = weak_consensus.upgrade() {
                consensus.lock().unwrap().handle_snapshot_timeout();
            } else {
                error!("Snapshot timer can not call after consensus is dropped.")
            }
        });

    consensus
}

pub fn stop() {
    todo!()
}
