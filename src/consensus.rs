use crate::log::Log;
use crate::peer::{Peer, PeerManager};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, RequestVoteRequest, RequestVoteResponse,
};
use crate::rpc::Client;
use crate::timer::Timer;
use crate::{config, proto, rpc, util};
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
    leader_id: u64,
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
            voted_for: config::NONE_SERVER_ID,
            commit_index: 0,
            last_applied: 0,
            leader_id: config::NONE_SERVER_ID,
            peer_manager: PeerManager::new(),
            log: Log::new(1),
            rpc_client: Client {},
            tokio_runtime,
        };

        consensus.peer_manager.add_peers(peers);
        Arc::new(Mutex::new(consensus))
    }

    pub fn replicate(&mut self, data: String) -> Result<(), Box<dyn std::error::Error>> {
        if self.state != State::Leader {
            error!("Replicate should be processed by leader!");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Not leader!",
            )));
        }
        info!("Replicate data: {}", &data);

        // Save the log entry
        self.log.append(
            self.current_term,
            vec![(proto::EntryType::Data, data.as_bytes().to_vec())],
        );

        // TODO: Send the log entry to other peers
        self.append_entries();
        Ok(())
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
            let prev_log = self.log.prev_entry(self.log.last_index()).unwrap();
            let request = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.server_id,
                prev_log_term: prev_log.index,
                prev_log_index: prev_log.term,
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

    // Step down to state
    fn step_down(&mut self, new_term: u64) {
        info!(
            "Step down to term: {}, current term: {}.",
            new_term, self.current_term
        );
        if new_term < self.current_term {
            error!(
                "Step down failed because new term {} is smaller than current term {}.",
                new_term, self.current_term
            );
            return;
        }
        if new_term > self.current_term {
            self.state = State::Follower;
            self.current_term = new_term;
            self.voted_for = config::NONE_SERVER_ID;
            self.leader_id = config::NONE_SERVER_ID;
        } else {
            // If the leader get the same term, then become follower
            self.state = State::Follower;
        }

        // Reset the election timer
        self.election_timer
            .lock()
            .unwrap()
            .reset(util::rand_election_timeout());
    }

    // Election for leader
    fn become_leader(&mut self) {
        if self.state != State::Candidate {
            error!("Become leader failed because state is not candidate!");
            return;
        }
        self.state = State::Leader;
        self.leader_id = self.server_id;
        self.append_entries();
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
        let refuse_resp = AppendEntriesResponse {
            term: self.current_term,
            success: false,
        };

        if request.term < self.current_term {
            return refuse_resp;
        }
        self.step_down(request.term);

        match self.state {
            State::Follower => {}
            State::Candidate => {
                panic!(
                    "Candidate {} received append entries from {}",
                    self.server_id, request.leader_id
                );
            }
            State::Leader => {
                panic!(
                    "Leader {} received append entries from {}",
                    self.server_id, request.leader_id
                );
            }
            _ => {
                error!("");
            }
        }
        let reply = AppendEntriesResponse {
            term: 1,
            success: true,
        };
        reply
    }

    pub fn handle_request_vote(&mut self, request: &RequestVoteRequest) -> RequestVoteResponse {
        if request.term > self.current_term {
            self.step_down(request.term);
        }

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

            if self.voted_for != config::NONE_SERVER_ID && self.voted_for != request.candidate_id {
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
        info!("handle install snapshot");
        let reply = InstallSnapshotResponse { term: 1 };
        reply
    }
}
