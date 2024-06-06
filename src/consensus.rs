use crate::log::Log;
use crate::peer::{Peer, PeerManager};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::rpc::Client;
use crate::timer::Timer;
use crate::{config, proto, util};
use log::{error, info};
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq)]
enum State {
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
        let mut consensus = Self {
            server_id,
            server_addr: format!("127.0.0.1:{}", port),
            current_term: 0,
            state: State::Follower,
            election_timer: Arc::new(Mutex::new(Timer::new("ElectionTimer"))),
            heartbeat_timer: Arc::new(Mutex::new(Timer::new("HeartbeatTimer"))),
            snapshot_timer: Arc::new(Mutex::new(Timer::new("SnapshotTimer"))),
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

    pub fn replicate(
        &mut self,
        r#type: proto::EntryType,
        data: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.state != State::Leader {
            error!("Replicate should be processed by leader!");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No leader!",
            )));
        }
        info!("Replicate data: {}.", &data);

        // Save the log entry
        self.log
            .append(self.current_term, vec![(r#type, data.as_bytes().to_vec())]);

        // TODO: Send the log entry to other peers
        self.append_entries(false);

        Ok(())
    }

    fn append_entries(&mut self, heartbeat: bool) {
        // Check the current state
        if self.state != State::Leader {
            error!(
                "The current state is {:?}, can not append entries.",
                self.state
            );
            return;
        }

        let mut peers = self.peer_manager.peers();
        for peer in peers.iter() {
            let prev_log = self.log.entry(peer.next_index - 1).unwrap();
            let request = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.leader_id,
                prev_log_term: prev_log.term,
                prev_log_index: prev_log.index,
                entries: vec![],
                leader_commit: self.commit_index,
            };
            if let Err(_) = self.tokio_runtime.block_on(
                self.rpc_client
                    .append_entries(request, peer.server_addr.clone()),
            ) {
                error!("Append entries to {} failed.", &peer.server_addr);
            }
        }
    }

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
                info!(
                    "Request vote to {:?}, response: {:?}",
                    &peer.server_addr, &response
                );

                // If the peer has bigger term, then become a follower.
                if response.term > self.current_term {
                    info!(
                        "Peer {} has bigger term {} than self {}.",
                        &peer.server_addr, &response.term, self.current_term
                    );
                    self.state = State::Follower;
                    return;
                }
                // If the peer vote granted, then vote granted count + 1.
                if response.vote_granted {
                    info!("Peer {} vote granted.", &peer.server_addr);
                    vote_granted_count += 1;
                }

                if vote_granted_count + 1 > (peers.len() / 2) {
                    info!("Become leader.");
                    self.become_leader();
                    return;
                }
            } else {
                error!("Request vote to {} failed.", &peer.server_addr);
            }
        }
    }

    // TODO Install snapshot to other peers
    fn install_snapshot(&mut self) {
        todo!()
    }

    fn step_down(&mut self, new_term: u64) {
        info!(
            "Step down to term {}, current term: {}",
            new_term, self.current_term
        );

        if new_term < self.current_term {
            error!(
                "New term {} is smaller than current term {}.",
                new_term, self.current_term
            );
            return;
        }
        self.state = State::Follower;
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = config::NONE_SERVER_ID;
            self.leader_id = config::NONE_SERVER_ID;
        }

        self.election_timer
            .lock()
            .unwrap()
            .reset(util::rand_election_timeout());
    }

    fn become_leader(&mut self) {
        if self.state != State::Candidate {
            error!(
                "The current state is {:?}, can't become leader (the state must be candidate).",
                self.state
            );
            return;
        }
        self.state = State::Leader;
        self.leader_id = self.server_id;

        // Add NOOP log
        if let Err(e) = self.replicate(proto::EntryType::Noop, config::NONE_DATA.to_string()) {
            error!("Add NOOP entry failed after becoming leader, error: {}", e);
            return;
        }
    }

    fn advance_commit_index(&mut self) {}

    pub fn handle_heartbeat_timeout(&mut self) {
        if self.state == State::Leader {
            info!("Handle heartbeat timeout.");
            self.append_entries(true);
        }
    }

    pub fn handle_election_timeout(&mut self) {
        match self.state {
            State::Leader => {}
            State::Candidate => {
                // Candidate election again
                info!("Start election again.");

                self.current_term += 1;
                self.voted_for = self.server_id; // Vote for self

                self.election_timer
                    .lock()
                    .unwrap()
                    .reset(util::rand_election_timeout());

                // Request vote
                self.request_vote();
            }
            State::Follower => {
                if self.voted_for == config::NONE_SERVER_ID {
                    info!("Start election.");

                    self.state = State::Candidate; // Become candidate
                    self.current_term += 1; // Increase term
                    self.voted_for = self.server_id; // Vote for self

                    self.election_timer
                        .lock()
                        .unwrap()
                        .reset(util::rand_election_timeout());

                    self.request_vote();
                }
            }
        }
    }

    pub fn handle_snapshot_timeout(&mut self) {
        if self.state == State::Leader {
            info!("Handle snapshot timeout.");
            self.install_snapshot();
        }
    }

    pub fn handle_append_entries(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let refuse_response = AppendEntriesResponse {
            term: self.current_term,
            success: false,
        };

        // If the request term is smaller than the current term, then refuse.
        if request.term < self.current_term {
            info!(
                "Refuse append entries, because request term {} is smaller than current term {}.",
                request.term, self.current_term
            );
            return refuse_response;
        }

        // Step down to the request term.
        self.step_down(request.term);

        if self.leader_id == config::NONE_SERVER_ID {
            info!("Update leader id to {}.", request.leader_id);
            self.leader_id = request.leader_id;
        }
        if self.leader_id != request.leader_id {
            error!(
                "Leader id {} is not equal to request leader id {}.",
                self.leader_id, request.leader_id
            );
            // TODO
        }

        match self.state {
            State::Leader => {
                panic!(
                    "Leader {} receive append entries from {}.",
                    self.server_id, request.leader_id
                );
            }
            State::Candidate => {
                panic!(
                    "Candidate {} receive append entries from {}.",
                    self.server_id, request.leader_id
                );
            }
            State::Follower => {}
        }
        AppendEntriesResponse {
            term: 1,
            success: true,
        }
    }

    pub fn handle_request_vote(&mut self, request: &RequestVoteRequest) -> RequestVoteResponse {
        let refuse_response = RequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
        };

        if request.term < self.current_term {
            info!(
                "Refuse request vote, because request term {} is smaller than current term {}.",
                request.term, self.current_term
            );
            return refuse_response;
        }

        if request.term > self.current_term {
            self.step_down(request.term);
        }

        let log_is_ok = request.term > self.log.last_term()
            || (request.term == self.log.last_term()
                && request.last_log_index >= self.log.last_index());
        if !log_is_ok {
            return refuse_response;
        }

        if self.voted_for != config::NONE_SERVER_ID && self.voted_for != request.candidate_id {
            info!(
                "Refuse request vote, because candidate {} has already voted for {}.",
                request.candidate_id, self.voted_for
            );
            return refuse_response;
        }

        match self.state {
            State::Leader => {
                panic!(
                    "Leader {} receive request vote from {}.",
                    self.server_id, request.candidate_id
                );
            }
            State::Candidate => {
                panic!(
                    "Candidate {} receive request vote from {}.",
                    self.server_id, request.candidate_id
                );
            }
            State::Follower => {}
        }

        info!("Agree vote for server id {}.", request.candidate_id);
        self.voted_for = request.candidate_id;

        self.election_timer
            .lock()
            .unwrap()
            .reset(util::rand_election_timeout());

        RequestVoteResponse {
            term: 1,
            vote_granted: true,
        }
    }

    pub fn handle_install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        info!("Handle install snapshot");
        let reply = InstallSnapshotResponse { term: 1 };
        reply
    }
}
