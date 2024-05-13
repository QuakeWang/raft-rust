use crate::log::Log;
use crate::peer::PeerManager;
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::timer::Timer;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
enum State {
    Unknown,
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct Consensus {
    server_id: u64,
    server_addr: String,
    current_term: u64,
    state: State,
    pub election_timer: Arc<Mutex<Timer>>,
    pub heartbeat_timer: Arc<Mutex<Timer>>,
    pub snapshot_timer: Arc<Mutex<Timer>>,
    voted_for: u64,
    commit_index: u64,
    last_applied: u64,
    peer_manager: PeerManager,
    log: Log,
}

impl Consensus {
    pub fn new(port: u32) -> Arc<Mutex<Self>> {
        let consensus = Consensus {
            server_id: 1,
            server_addr: format!("127.0.0.1:{}", port),
            current_term: 1,
            state: State::Follower,
            election_timer: Arc::new(Mutex::new(Timer::new("election"))),
            heartbeat_timer: Arc::new(Mutex::new(Timer::new("heartbeat"))),
            snapshot_timer: Arc::new(Mutex::new(Timer::new("snapshot"))),
            voted_for: 0,
            commit_index: 0,
            last_applied: 0,
            peer_manager: PeerManager::new(),
            log: Log::new(1),
        };
        Arc::new(Mutex::new(consensus))
    }

    pub fn replicate(&self) {
        println!("replicate");
    }

    fn request_vote() {
        todo!()
    }

    fn install_snapshot() {
        todo!()
    }

    fn append_entries() {
        todo!()
    }

    pub fn handle_heartbeat_timeout(&mut self) {
        println!("handle heartbeat timeout");
    }

    pub fn handle_election_timeout(&mut self) {
        println!("handle election timeout at {:?}", Instant::now());
        // TODO: Set the random election timeout for next time
        self.election_timer
            .lock()
            .unwrap()
            .reset(Duration::from_secs(15));
    }

    pub fn handle_append_entries(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        println!("handle append entries");
        let reply = AppendEntriesResponse {
            success: true,
            term: 1,
        };
        reply
    }

    pub fn handle_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        println!("handle request vote");
        let reply = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        reply
    }

    pub fn handle_install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        println!("handle install snapshot");
        let reply = InstallSnapshotResponse { term: 1 };
        reply
    }
}
