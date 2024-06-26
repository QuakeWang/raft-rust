use crate::log::{Configuration, Log, ServerInfo};
use crate::peer::{self, Peer, PeerManager};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, GetConfigurationRequest, GetConfigurationResponse,
    GetLeaderRequest, GetLeaderResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SetConfigurationRequest, SetConfigurationResponse,
};
use crate::rpc::Client;
use crate::timer::Timer;
use crate::{config, proto, state_machine, util};
use log::{error, info, warn};
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct Consensus {
    pub server_id: u64,
    pub server_addr: String,
    pub current_term: u64,
    pub state: State,
    pub voted_for: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: u64,
    pub peer_manager: PeerManager,
    pub log: Log,
    pub election_timer: Arc<Mutex<Timer>>,
    pub heartbeat_timer: Arc<Mutex<Timer>>,
    pub snapshot_timer: Arc<Mutex<Timer>>,
    rpc_client: Client,
    tokio_runtime: tokio::runtime::Runtime,
    pub state_machine: Box<dyn state_machine::StateMachine>,
}

impl Consensus {
    pub fn new(
        server_id: u64,
        port: u32,
        peers: Vec<Peer>,
        state_machine: Box<dyn state_machine::StateMachine>,
    ) -> Arc<Mutex<Self>> {
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
            state_machine,
        };

        consensus.peer_manager.add_peers(peers);
        Arc::new(Mutex::new(consensus))
    }

    pub fn replicate(
        &mut self,
        r#type: proto::EntryType,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.state != State::Leader {
            error!("Replicate should be processed by leader!");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No leader!",
            )));
        }
        info!("Replicate data: {:?}.", &data);

        // Save the log entry
        self.log
            .append_data(self.current_term, vec![(r#type, data)]);

        // Send the log entry to other peers
        self.append_entries(false);

        Ok(())
    }

    fn append_entries(&mut self, heartbeat: bool) -> bool {
        // Check the current state
        if self.state != State::Leader {
            error!(
                "The current state is {:?}, can not append entries.",
                self.state
            );
            return false;
        }

        // TODO: Send the log entries to other peers in parallel.
        let peer_ids = self.peer_manager.peer_ids();
        for peer_server_id in peer_ids.iter() {
            self.append_entries_to_peer(peer_server_id.clone(), heartbeat);
        }

        true
    }

    fn append_entries_to_peer(&mut self, peer_server_id: u64, heartbeat: bool) -> bool {
        let peer = self.peer_manager.peer(peer_server_id).unwrap();
        let entries = match heartbeat {
            true => self.log.pack_entries(peer.next_index),
            false => Vec::new(),
        };

        let entries_num = entries.len();

        let prev_log_index = peer.next_index - 1;
        let prev_log = self.log.entry(prev_log_index).unwrap();
        let request = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.server_id,
            prev_log_term: prev_log.term,
            prev_log_index,
            entries,
            leader_commit: self.commit_index,
        };

        let response = match self.tokio_runtime.block_on(
            self.rpc_client
                .append_entries(request, peer.server_addr.clone()),
        ) {
            Ok(response) => response,
            Err(_) => {
                error!("Append entries to {} failed.", &peer.server_addr);
                return false;
            }
        };

        if response.term > self.current_term {
            self.step_down(response.term);
            return false;
        }

        return match response.success {
            true => {
                peer.match_index = prev_log_index + entries_num as u64;
                peer.next_index = peer.match_index + 1;
                self.leader_advance_commit_index();
                true
            }
            false => {
                if peer.next_index > 1 {
                    peer.next_index -= 1;
                }
                false
            }
        };
    }

    fn request_vote(&mut self) {
        info!("Start request vote");
        let mut vote_granted_count = 0;

        let peer_ids = self.peer_manager.peer_ids();
        for peer_server_id in peer_ids.iter() {
            let peer = self.peer_manager.peer(peer_server_id.clone()).unwrap();
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

                if vote_granted_count + 1 > (peer_ids.len() / 2) {
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
        if let Err(e) = self.replicate(
            proto::EntryType::Noop,
            config::NONE_DATA.as_bytes().to_vec(),
        ) {
            error!("Add NOOP entry failed after becoming leader, error: {}", e);
            return;
        }
    }

    fn leader_advance_commit_index(&mut self) {
        let new_commit_index = self.peer_manager.quorum_match_index(self.log.last_index());
        if new_commit_index <= self.commit_index {
            return;
        }
        info!(
            "Advance commit index from {} to {}.",
            self.commit_index, new_commit_index
        );

        for index in self.commit_index + 1..new_commit_index + 1 {
            let entry = self.log.entry(index).unwrap();
            if entry.entry_type() == proto::EntryType::Data {
                info!("Apply data entry: {:?}", entry);
                self.state_machine.apply(&entry.data);
            }
        }

        self.commit_index = new_commit_index;
    }

    fn follower_advance_commit_index(&mut self, leader_commit_index: u64) {
        if self.commit_index < leader_commit_index {
            info!(
                "Follower advance commit index from {} to {}.",
                self.commit_index, leader_commit_index
            );
            for index in self.commit_index + 1..leader_commit_index + 1 {
                let entry = self.log.entry(index).unwrap();
                if entry.entry_type() == proto::EntryType::Data {
                    info!("Apply data entry: {:?}", entry);
                    self.state_machine.apply(&entry.data);
                }
            }
            self.commit_index = leader_commit_index;
        }
    }

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

    pub fn handle_append_entries_bak(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let refuse_response = AppendEntriesResponse {
            term: self.current_term,
            success: false,
        };

        // Compare the request term with the current term.
        if request.term < self.current_term {
            return refuse_response;
        }

        // If the leader or candidate receives a request
        // that the term is bigger than the current term,
        // then step down to follower.
        self.step_down(request.term);

        // Update leader id.
        if self.leader_id == config::NONE_SERVER_ID {
            info!("Update leader id to {}.", request.leader_id);
            self.leader_id = request.leader_id;
        }

        if self.leader_id != request.leader_id {
            error!(
                "There are more than one leader id, current: {}, new: {}",
                self.leader_id, request.leader_id
            );
        }

        if request.prev_log_index > self.log.last_index() {
            warn!(
                "Reject append entries because prev_log_index {} is greater than last index {}.",
                request.prev_log_index,
                self.log.last_index()
            );
            return refuse_response;
        }

        // Compare the prev_log_term with the log term.
        if request.prev_log_index > self.log.start_index() {
            let log_entry = self.log.entry(request.prev_log_index).unwrap();
            if request.prev_log_term != log_entry.term {
                info!(
                    "Reject append entries because prev_log_term {} is not equal to log term {}.",
                    request.prev_log_term, log_entry.term
                );
                return refuse_response;
            }
        }

        // If the entries are empty, then it's a heartbeat.
        if request.entries.is_empty() {
            info!("Receive heartbeat from leader {}.", request.leader_id);
            self.follower_advance_commit_index(request.leader_commit);
            return AppendEntriesResponse {
                term: self.current_term,
                success: true,
            };
        }

        let mut entries_to_be_replicated = Vec::new();
        let mut index = request.prev_log_index;
        for entry in request.entries.iter() {
            index += 1;
            if entry.index != index {
                error!("Request entries index is not incremental.");
                return refuse_response;
            }
            if index < self.log.start_index() {
                continue;
            }
            if self.log.last_index() >= index {
                let log_entry = self.log.entry(index).unwrap();
                if log_entry.term == entry.term {
                    continue;
                }
                info!(
                    "Delete conflict log entry, index: {}, term: {}.",
                    index, log_entry.term
                );
                let last_index_kept = index - 1;
                self.log.truncate_suffix(last_index_kept);
            }
            entries_to_be_replicated.push(entry.clone());
        }
        self.log.append_entries(entries_to_be_replicated);
        self.follower_advance_commit_index(request.leader_commit);

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
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

    pub fn handle_get_leader(&mut self, request: &GetLeaderRequest) -> GetLeaderResponse {
        if self.state == State::Leader {
            return GetLeaderResponse {
                leader: Some(proto::Server {
                    server_id: self.server_id,
                    server_addr: self.server_addr.clone(),
                }),
            };
        }

        for peer in self.peer_manager.peers() {
            if peer.server_id == self.server_id {
                return GetLeaderResponse {
                    leader: Some(proto::Server {
                        server_id: peer.server_id,
                        server_addr: peer.server_addr.clone(),
                    }),
                };
            }
        }
        let reply = GetLeaderResponse { leader: None };
        reply
    }

    pub fn handle_get_configuration(
        &mut self,
        request: &GetConfigurationRequest,
    ) -> GetConfigurationResponse {
        let mut servers = Vec::new();
        for peer in self.peer_manager.peers() {
            servers.push(proto::Server {
                server_id: peer.server_id,
                server_addr: peer.server_addr.clone(),
            })
        }
        servers.push(proto::Server {
            server_id: self.server_id,
            server_addr: self.server_addr.clone(),
        });

        let reply = GetConfigurationResponse { servers };
        reply
    }

    pub fn handle_set_configuration(
        &mut self,
        request: &SetConfigurationRequest,
    ) -> SetConfigurationResponse {
        info!("Handle set configuration!");

        let mut old_new_configuration = Configuration::new();
        old_new_configuration.append_new_servers(request.servers.as_ref());
        old_new_configuration.append_old_servers(self.peer_manager.peers());
        old_new_configuration
            .old_servers
            .push(ServerInfo(self.server_id, self.server_addr.clone()));

        self.replicate(
            proto::EntryType::Configuration,
            old_new_configuration.to_data(),
        )
        .unwrap();

        let reply = SetConfigurationResponse { success: true };
        reply
    }
}
