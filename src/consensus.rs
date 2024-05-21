use crate::log::Log;
use crate::peer::{Peer, PeerManager};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, RequestVoteRequest, RequestVoteResponse,
};
use crate::rpc::Client;
use crate::timer::Timer;
use crate::{proto, rpc, util};
use log::{error, info};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
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
    rpc_client: Client,
    tokio_runtime: tokio::runtime::Runtime,
}

impl Consensus {
    pub fn new(server_id: u64, port: u32, peers: Vec<Peer>) -> Arc<Mutex<Self>> {
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let mut consensus = Consensus {
            server_id: 1,
            server_addr: format!("127.0.0.1:{}", port),
            current_term: 0,
            state: State::Follower,
            election_timer: Arc::new(Mutex::new(Timer::new("election"))),
            heartbeat_timer: Arc::new(Mutex::new(Timer::new("heartbeat"))),
            snapshot_timer: Arc::new(Mutex::new(Timer::new("snapshot"))),
            voted_for: 0,
            commit_index: 0,
            last_applied: 0,
            peer_manager: PeerManager::new(),
            log: Log::new(1),
            rpc_client: Client {},
            tokio_runtime,
        };

        consensus.peer_manager.add_peers(peers);
        Arc::new(Mutex::new(consensus))
    }

    pub fn replicate(&mut self, data: String) {
        info!("Replicate data: {}", &data);

        let log_entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            r#entry_type: proto::EntryType::Data.into(),
            data: data.as_bytes().to_vec(),
        };

        // Save the log entry
        self.log.append_entries(vec![log_entry]);

        // TODO: Send the log entry to other peers
        self.append_entries();
    }

    // Append the log to other nodes
    fn append_entries(&mut self) {
        // Check the state
        if self.state != State::Leader {
            error!("State is {:?}, cannot append entries!", self.state);
            return;
        }

        let mut peers = self.peer_manager.peers();
        for peer in peers.iter() {
            let request = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.server_id,
                prev_log_term: peer.next_index - 1,
                prev_log_index: 0,
                entries: vec![],
                leader_commit: self.commit_index,
            };

            if let Err(_) = self.tokio_runtime.block_on(
                self.rpc_client
                    .append_entries(request, peer.server_addr.clone()),
            ) {
                error!("Append entries to {} failed", &peer.server_addr);
            }
        }
    }

    // Request vote from other nodes
    fn request_vote(&mut self) {
        info!("Start request vote");
        let mut vote_granted_count = 0;

        let mut peers = self.peer_manager.peers();
        for peer in peers.iter() {
            info!("Request vote to {:?}.", &peer.server_addr);
            let request = RequestVoteRequest {
                term: self.current_term,
                candidate_id: self.server_id,
                last_log_term: self.log.last_term(),
                last_log_index: self.log.last_index(),
            };
            if let Ok(response) = self.tokio_runtime.block_on(
                self.rpc_client
                    .request_vote(request, peer.server_addr.clone()),
            ) {
                // If the response term is greater than the current term, then become follower
                if response.term > self.current_term {
                    info!(
                        "Peer {} has greater term {} than self {}.",
                        &peer.server_addr, &response.term, &self.current_term
                    );
                    self.state = State::Follower;
                    return;
                }

                // If the response is granted, then increment the vote count
                if response.vote_granted {
                    info!("Peer {} vote granted.", &peer.server_addr);
                    vote_granted_count += 1;
                }

                // If the vote count is greater than half of the peers, then become leader
                if vote_granted_count + 1 > (peers.len() / 2) {
                    info!("Vote count is greater than half of the peers, become leader.");
                    self.state = State::Leader;
                    self.append_entries();
                    return;
                }
            } else {
                error!("Request vote to {} failed.", &peer.server_addr);
            }
        }
    }

    fn install_snapshot(&self) {
        todo!()
    }

    fn step_down(&mut self) {
        todo!()
    }

    // Term update
    fn term_update(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.voted_for = 0;
    }
    pub fn handle_heartbeat_timeout(&mut self) {
        if self.state == State::Leader {
            info!("Handle heartbeat timeout at {:?}", Instant::now());
            self.append_entries();
        }
    }

    pub fn handle_election_timeout(&mut self) {
        match self.state {
            State::Leader => {}
            State::Follower => {
                info!("Start election");

                self.state = State::Candidate; // Become candidate
                self.current_term += 1; // Increment the term
                self.voted_for = self.server_id; // Vote for self

                // TODO : Reset the election timer
                self.election_timer
                    .lock()
                    .unwrap()
                    .reset(util::rand_election_timeout());

                // Request vote from other nodes
                self.request_vote();
            }
            State::Candidate => {
                info!("Start election again!");

                self.current_term += 1; // Increment the term
                self.voted_for = self.server_id; // Vote for self

                // TODO : Reset the election timer
                self.election_timer
                    .lock()
                    .unwrap()
                    .reset(util::rand_election_timeout());

                // Request vote from other nodes
                self.request_vote();
            }
            _ => {
                error!("Invalid state {:?}", self.state);
            }
        }
    }

    pub fn handle_snapshot_timeout(&mut self) {
        info!("Handle snapshot timeout");
    }

    pub fn handle_append_entries(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        info!("Handle append entries");
        match self.state {
            State::Follower => {
                self.election_timer
                    .lock()
                    .unwrap()
                    .reset(util::rand_election_timeout());
            }
            State::Candidate => {
                self.state = State::Follower;
                self.election_timer
                    .lock()
                    .unwrap()
                    .reset(util::rand_election_timeout());
            }
            State::Leader => {}
            _ => {
                error!("Invalid state {:?}", self.state)
            }
        }
        let reply = AppendEntriesResponse {
            term: 1,
            success: true,
        };
        reply
    }

    pub fn handle_request_vote(
        &mut self,
        request: &proto::RequestVoteRequest,
    ) -> RequestVoteResponse {
        info!("Handle request vote");
        let refuse_reply = RequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
        };

        if self.state == State::Leader {
            return refuse_reply;
        } else if self.state == State::Candidate {
            // The candidate refuses to vote
            return refuse_reply;
        } else if self.state == State::Follower {
            if request.term < self.current_term {
                info!(
                    "Refuse vote for {} due to candidate's term is smaller than self's term.",
                    request.candidate_id
                );
                return refuse_reply;
            }

            if self.voted_for != 0 && self.voted_for != request.candidate_id {
                info!(
                    "Refuse vote for {} due to self has already voted for {}.",
                    request.candidate_id, self.voted_for
                );
                return refuse_reply;
            }

            info!("Agree vote for server id {}", request.candidate_id);
            self.voted_for = request.candidate_id;
            // TODO: Reset the election timer
            self.election_timer
                .lock()
                .unwrap()
                .reset(util::rand_election_timeout());
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: true,
            };
        }
        error!("Sate is {:?}, cannot handle request vote", self.state);
        refuse_reply
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
