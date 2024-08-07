use crate::config::{Configuration, ConfigurationState};
use crate::log::Log;
use crate::metadata::Metadata;
use crate::peer::{Peer, PeerManager};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, EntryType, GetConfigurationRequest,
    GetConfigurationResponse, GetLeaderRequest, GetLeaderResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, Server,
    SetConfigurationRequest, SetConfigurationResponse, SnapshotDataType,
};
use crate::rpc::Client;
use crate::timer::Timer;
use crate::{config, proto, snapshot, state_machine, util};
use log::{error, info, warn};
use std::io::{Read, Seek, Write};
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
    pub metadata: Metadata, // load raft metadata
    pub state: State,
    pub voted_for: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: u64,
    pub peer_manager: PeerManager,
    pub log: Log,
    snapshot: snapshot::Snapshot,
    pub configuration_state: ConfigurationState,
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
        snapshot_dir: String,
        metadata_dir: String,
    ) -> Self {
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let mut consensus = Self {
            server_id,
            server_addr: format!("127.0.0.1:{}", port),
            metadata: Metadata::new(metadata_dir.clone()),
            state: State::Follower,
            election_timer: Arc::new(Mutex::new(Timer::new("ElectionTimer"))),
            heartbeat_timer: Arc::new(Mutex::new(Timer::new("HeartbeatTimer"))),
            snapshot_timer: Arc::new(Mutex::new(Timer::new("SnapshotTimer"))),
            voted_for: config::NONE_SERVER_ID,
            commit_index: 0,
            last_applied: 0,
            leader_id: config::NONE_SERVER_ID,
            peer_manager: PeerManager::new(),
            log: Log::new(1, metadata_dir),
            snapshot: snapshot::Snapshot::new(snapshot_dir),
            configuration_state: ConfigurationState::new(),
            rpc_client: Client {},
            tokio_runtime,
            state_machine,
        };

        // load raft metadata
        consensus.metadata.reload();

        // load raft log
        consensus.log.reload();

        // load snapshot metadata
        consensus.snapshot.reload_metadata();

        // load snapshot to state machine
        if let Some(snapshot_filepath) = consensus.snapshot.latest_snapshot_filepath() {
            consensus.state_machine.restore_snapshot(snapshot_filepath);
        }

        consensus.peer_manager.add_peers(
            peers,
            consensus
                .log
                .last_index(consensus.snapshot.last_included_index),
        );

        consensus
    }

    pub fn replicate(
        &mut self,
        r#type: EntryType,
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
            .append_data(self.metadata.current_term, vec![(r#type, data.clone())]);

        if r#type == EntryType::Configuration {
            self.apply_configuration(Configuration::from_data(&data), false);
        }

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
        let peer_server_ids = self.peer_manager.peer_server_ids();
        if peer_server_ids.is_empty() {
            self.leader_advance_commit_index();
        }
        for peer_server_id in peer_server_ids.iter() {
            self.append_entries_to_peer(*peer_server_id, heartbeat);
        }

        true
    }

    fn append_entries_to_peer(&mut self, peer_id: u64, heartbeat: bool) -> bool {
        let peer = match self.peer_manager.peer(peer_id) {
            None => {
                warn!(
                    "Peer {} not found in peer_manager when append entries.",
                    peer_id
                );
                return false;
            }
            Some(peer) => peer,
        };

        let need_install_snapshot = !heartbeat && peer.next_index < self.log.start_index();
        if need_install_snapshot {
            info!("Change to install snapshot for peer {}.", peer_id);
            self.install_snapshot(peer_id);
            return true;
        }

        let entries = match heartbeat {
            true => self.log.pack_entries(peer.next_index),
            false => Vec::with_capacity(0),
        };

        let entries_num = entries.len();

        let prev_log_index = peer.next_index - 1;
        let prev_log_term = self.log.prev_log_term(
            prev_log_index,
            self.snapshot.last_included_index,
            self.snapshot.last_included_term,
        );
        let request = AppendEntriesRequest {
            term: self.metadata.current_term,
            leader_id: self.server_id,
            prev_log_term,
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

        if response.term > self.metadata.current_term {
            self.step_down(response.term);
            return false;
        }

        match response.success {
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
        }
    }

    fn request_vote(&mut self) {
        info!("Start request vote");
        // Reset the vote status
        self.peer_manager.reset_vote();
        let peer_server_ids = self.peer_manager.peer_server_ids();
        for peer_server_id in peer_server_ids.iter() {
            let peer = self.peer_manager.peer(*peer_server_id).unwrap();
            info!("Request vote to {:?}.", &peer.server_addr);
            let request = RequestVoteRequest {
                term: self.metadata.current_term,
                candidate_id: self.server_id,
                last_log_term: self.log.last_term(self.snapshot.last_included_term),
                last_log_index: self.log.last_index(self.snapshot.last_included_index),
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
                if response.term > self.metadata.current_term {
                    info!(
                        "Peer {} has bigger term {} than self {}.",
                        &peer.server_addr, &response.term, self.metadata.current_term
                    );
                    self.state = State::Follower;
                    return;
                }
                // Get a vote
                if response.vote_granted {
                    info!("Peer {} vote granted.", &peer.server_addr);
                    peer.vote_granted = true;
                }
                // Get the more vote from the quorum, then become a leader.
                if self
                    .peer_manager
                    .quorum_vote_granted(&self.configuration_state)
                {
                    info!("Become leader.");
                    self.become_leader();
                    return;
                }
            } else {
                error!("Request vote to {} failed.", &peer.server_addr);
            }
        }

        // Get the more vote from the quorum, then become a leader.
        if self
            .peer_manager
            .quorum_vote_granted(&self.configuration_state)
        {
            info!("Become leader.");
            self.become_leader();
        }
    }

    fn install_snapshot(&mut self, peer_id: u64) {
        let peer = self.peer_manager.peer(peer_id).unwrap();

        let metadata_filepath = match self.snapshot.latest_metadata_filepath() {
            None => {
                return;
            }
            Some(filepath) => filepath,
        };

        let snapshot_filepath = match self.snapshot.latest_snapshot_filepath() {
            None => {
                return;
            }
            Some(filepath) => filepath,
        };

        let mut medata_file = std::fs::File::open(metadata_filepath.clone()).unwrap();
        let mut snapshot_file = std::fs::File::open(snapshot_filepath.clone()).unwrap();
        let metadata_size = medata_file.metadata().unwrap().len();
        let snapshot_size = snapshot_file.metadata().unwrap().len();
        info!(
            "Install snapshot to peer {}, metadata_filepath: {}, metadata_size: {}, snapshot_filepath: {}, snapshot_size: {}.",
            peer_id, &metadata_filepath, metadata_size, &snapshot_filepath, snapshot_size
        );

        let mut offset = 0;
        let mut is_metadata_done = false;
        let mut is_done = false;

        // Loop to send snapshot metadata.
        loop {
            let mut data: Vec<u8> = {
                if offset + (config::SNAPSHOT_TRUNK_SIZE as u64) < metadata_size {
                    vec![0u8; config::SNAPSHOT_TRUNK_SIZE]
                } else {
                    is_metadata_done = true;
                    vec![0u8; (metadata_size - offset) as usize]
                }
            };

            let data_cap = data.capacity();

            if let Err(e) = medata_file.seek(std::io::SeekFrom::Start(offset)) {
                error!(
                    "Install snapshot to {:?}, failed to seek, error: {}.",
                    &peer.server_addr, e
                );
                break;
            }

            if let Err(e) = medata_file.read_exact(&mut data) {
                error!(
                    "Install snapshot to {:?}, failed to read_exact, error : {}",
                    &peer.server_addr, e
                );
                break;
            }

            let request = InstallSnapshotRequest {
                term: self.metadata.current_term,
                leader_id: self.server_id,
                last_included_index: self.snapshot.last_included_index,
                last_included_term: self.snapshot.last_included_term,
                offset,
                data,
                r#type: proto::SnapshotDataType::Metadata.into(),
                done: is_done,
            };

            match self.tokio_runtime.block_on(
                self.rpc_client
                    .install_snapshot(request, peer.server_addr.clone()),
            ) {
                Ok(response) => {
                    if response.term > self.metadata.current_term {
                        self.step_down(response.term);
                        return;
                    }
                }
                Err(e) => {
                    error!(
                        "Install snapshot to {:?}, failed to send rpc, error: {}.",
                        &peer.server_addr, e
                    );
                    return;
                }
            }

            // Update offset
            offset += data_cap as u64;
            if is_metadata_done && is_done {
                break;
            }
        }

        // Loop to send snapshot.
        loop {
            let mut data: Vec<u8> = {
                if offset + (config::SNAPSHOT_TRUNK_SIZE as u64) < snapshot_size {
                    vec![0u8; config::SNAPSHOT_TRUNK_SIZE]
                } else {
                    is_done = true;
                    vec![0u8; (snapshot_size - (offset - metadata_size)) as usize]
                }
            };

            let data_cap = data.capacity();

            if let Err(e) = snapshot_file.seek(std::io::SeekFrom::Start(offset - metadata_size)) {
                error!(
                    "Install snapshot to {:?}, failed to seek, error: {}.",
                    &peer.server_addr, e
                );
                break;
            }
            if let Err(e) = snapshot_file.read_exact(&mut data) {
                error!(
                    "Install snapshot to {:?} failed to read_exact, error : {}",
                    &peer.server_addr, e
                );
            }

            let request = InstallSnapshotRequest {
                term: self.metadata.current_term,
                leader_id: self.server_id,
                last_included_index: self.snapshot.last_included_index,
                last_included_term: self.snapshot.last_included_term,
                offset,
                data,
                r#type: proto::SnapshotDataType::Snapshot.into(),
                done: is_done,
            };

            match self.tokio_runtime.block_on(
                self.rpc_client
                    .install_snapshot(request, peer.server_addr.clone()),
            ) {
                Ok(response) => {
                    // If the peer has bigger term, then become a follower.
                    if response.term > self.metadata.current_term {
                        self.step_down(response.term);
                        return;
                    }
                }
                Err(e) => {
                    error!(
                        "Install snapshot to {:?} failed to send rpc, error: {}.",
                        &peer.server_addr, e
                    );
                    return;
                }
            }
            if is_done {
                // Set next_index to the last index of the snapshot
                peer.next_index = self.snapshot.last_included_index + 1;
                break;
            }
            offset += data_cap as u64;
        }
    }

    fn step_down(&mut self, new_term: u64) {
        info!(
            "Step down to term {}, current term: {}",
            new_term, self.metadata.current_term
        );

        if new_term < self.metadata.current_term {
            error!(
                "New term {} is smaller than current term {}.",
                new_term, self.metadata.current_term
            );
            return;
        }
        self.state = State::Follower;
        if new_term > self.metadata.current_term {
            self.metadata.update_current_term(new_term);
            self.metadata.update_voted_for(config::NONE_SERVER_ID);
            self.leader_id = config::NONE_SERVER_ID;
        } else {
            // Case: new_term == self.current_term
            // 1. Leader receives AppendEntries RPC => fallback to Follower
            // 2. Leader receives RequestVote RPC => no fallback to Follower
            // 3. Candidate receives AppendEntries RPC => fallback to Follower
            // 4. Candidate receives RequestVote RPC => no fallback to Follower
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
        if let Err(e) = self.replicate(EntryType::Noop, config::NONE_DATA.as_bytes().to_vec()) {
            error!("Add NOOP entry failed after becoming leader, error: {}", e);
        }
    }

    fn leader_advance_commit_index(&mut self) {
        let new_commit_index = self.peer_manager.quorum_match_index(
            &self.configuration_state,
            self.log.last_index(self.snapshot.last_included_index),
        );
        if new_commit_index <= self.commit_index {
            return;
        }
        info!(
            "Advance commit index from {} to {}.",
            self.commit_index, new_commit_index
        );

        let prev_commit_index = self.commit_index;

        for index in prev_commit_index + 1..new_commit_index + 1 {
            let entry = self.log.entry(index).unwrap();
            match entry.entry_type() {
                // Append new_configuration entry
                EntryType::Configuration => {
                    self.commit_index += 1;

                    let configuration = Configuration::from_data(&entry.data);

                    self.apply_configuration(configuration.clone(), true);

                    if configuration.is_configuration_old_new() {
                        info!("Append new_configuration entry when old_new_configuration commited, old_new_configuration: {:?}", &configuration);
                        self.append_configuration(None);
                    }
                }
                // Apply for StateMachine
                EntryType::Data => {
                    info!("Apply data entry: {:?}", entry);
                    self.state_machine.apply(&entry.data);
                    self.commit_index += 1;
                    self.last_applied = entry.index;
                }
                EntryType::Noop => {
                    self.commit_index += 1;
                }
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
            let prev_commit_index = self.commit_index;
            for index in prev_commit_index + 1..leader_commit_index + 1 {
                if let Some(entry) = self.log.entry(index) {
                    match entry.entry_type() {
                        EntryType::Configuration => {
                            let configuration = Configuration::from_data(&entry.data);
                            self.apply_configuration(configuration, true);
                        }
                        EntryType::Data => {
                            info!("Apply data entry: {:?}", entry);
                            self.state_machine.apply(&entry.data);
                            self.last_applied = entry.index;
                        }
                        EntryType::Noop => {}
                    }
                    self.commit_index += 1;
                } else {
                    // The entry does not exist, break the loop
                    break;
                }
            }
        }
    }

    fn apply_configuration(&mut self, configuration: Configuration, commited: bool) {
        // Configuration-old-new
        if configuration.is_configuration_old_new() {
            if commited {
                info!("Apply configuration-old-new when configuration commited.");
            } else {
                info!("Apply configuration-old-new when configuration appended.");

                // Add a new peer node
                let mut new_peers = Vec::new();
                for server_info in configuration.new_servers.iter() {
                    if !self.peer_manager.contains(server_info.server_id)
                        && server_info.server_id != self.server_id
                    {
                        new_peers.push(Peer::new(
                            server_info.server_id,
                            server_info.server_addr.clone(),
                        ));
                    }
                }
                self.peer_manager.add_peers(
                    new_peers,
                    self.log.last_index(self.snapshot.last_included_index),
                );

                // Update the peers' configuration state
                for peer in self.peer_manager.peers_mut().iter_mut() {
                    peer.configuration_state =
                        configuration.query_configuration_state(peer.server_id);
                }
                self.configuration_state = configuration.query_configuration_state(self.server_id);
            }
        } else if configuration.is_configuration_new() {
            if commited {
                info!("Apply configuration-new when configuration commited.");
                // Shutdown the leader node
                if !self.configuration_state.in_new && self.state == State::Leader {
                    self.shutdown()
                }
            } else {
                info!("Apply configuration-new when configuration appended.");

                // Update the peers' configuration state
                for peer in self.peer_manager.peers_mut().iter_mut() {
                    peer.configuration_state =
                        configuration.query_configuration_state(peer.server_id);
                }
                self.configuration_state = configuration.query_configuration_state(self.server_id);

                // Remove the old node
                let mut peer_ids_to_be_removed = Vec::new();
                for peer in self.peer_manager.peers() {
                    if !peer.configuration_state.in_new {
                        peer_ids_to_be_removed.push(peer.server_id);
                    }
                }
                self.peer_manager.remove_peers(peer_ids_to_be_removed);

                // Shutdown the non-leader node
                if !self.configuration_state.in_new && self.state != State::Leader {
                    self.shutdown();
                }
            }
        }
    }

    pub fn shutdown(&self) {
        info!("Shutdown this node.");
        self.heartbeat_timer.lock().unwrap().stop();
        self.election_timer.lock().unwrap().stop();
        self.snapshot_timer.lock().unwrap().stop();
    }

    // Append configuration item
    fn append_configuration(&mut self, new_servers: Option<&Vec<Server>>) -> bool {
        return match new_servers {
            // Append old_new_configuration
            Some(servers) => {
                let mut old_new_configuration = Configuration::new();
                old_new_configuration.append_new_servers(servers);
                old_new_configuration.append_old_servers(self.peer_manager.peers());
                old_new_configuration.old_servers.push(Server {
                    server_id: self.server_id,
                    server_addr: self.server_addr.clone(),
                });

                self.replicate(EntryType::Configuration, old_new_configuration.to_data())
                    .is_ok()
            }
            // Append new_configuration
            None => {
                let old_new_configuration = self.log.last_configuration();

                match old_new_configuration {
                    None => {
                        panic!(
                            "There is no old_new_configuration before append new configuration."
                        );
                    }
                    Some(old_new_configuration) => {
                        if !old_new_configuration.is_configuration_old_new() {
                            panic!("There is no old_new_configuration before append new configuration.")
                        }
                        let new_configuration = old_new_configuration.gen_new_configuration();
                        return self
                            .replicate(EntryType::Configuration, new_configuration.to_data())
                            .is_ok();
                    }
                }
            }
        };
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

                self.metadata
                    .update_current_term(self.metadata.current_term + 1);
                self.metadata.update_voted_for(self.server_id); // Vote for self

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
                    self.metadata
                        .update_current_term(self.metadata.current_term + 1); // Increase term
                    self.metadata.update_voted_for(self.server_id); // Vote for self

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
        if self.log.committed_entries_len(self.commit_index) > config::SNAPSHOT_LOG_LENGTH_THRESHOLD
        {
            info!("Start to take snapshot.");
            let last_included_index = self.log.last_index(self.snapshot.last_included_index);
            let last_included_term = self.log.last_term(self.snapshot.last_included_term);
            let configuration = self.log.last_configuration();

            // Write snapshot
            let snapshot_filepath = self
                .snapshot
                .gen_snapshot_filepath(last_included_index, last_included_term);
            info!("Snapshot filepath: {}", &snapshot_filepath);
            self.state_machine.take_snapshot(snapshot_filepath.clone());
            if !std::path::Path::new(&snapshot_filepath).exists() {
                error!("State machine failed to take snapshot.");
                return;
            }
            info!("Success to take snapshot, filepath: {}.", snapshot_filepath);
            self.snapshot.take_snapshot_metadata(
                last_included_index,
                last_included_term,
                configuration,
            );

            self.log.truncate_prefix(last_included_index);
        }
    }

    pub fn handle_append_entries(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let refuse_response = AppendEntriesResponse {
            term: self.metadata.current_term,
            success: false,
        };

        // Compare the request term with the current term.
        if request.term < self.metadata.current_term {
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

        if request.prev_log_index > self.log.last_index(self.snapshot.last_included_index) {
            warn!(
                "Reject append entries because prev_log_index {} is greater than last index {}.",
                request.prev_log_index,
                self.log.last_index(self.snapshot.last_included_index)
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
                term: self.metadata.current_term,
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
            if self.log.last_index(0) >= index {
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

        let mut configuration_entries = Vec::new();
        for entry in entries_to_be_replicated.iter() {
            if entry.entry_type() == EntryType::Configuration {
                configuration_entries.push(entry.clone());
            }
        }
        // Update the lo
        self.log.append_entries(entries_to_be_replicated);
        // Apply the configuration
        for configuration_entry in configuration_entries.iter() {
            self.apply_configuration(
                Configuration::from_data(configuration_entry.data.as_ref()),
                false,
            );
        }
        // Update the commit index
        self.follower_advance_commit_index(request.leader_commit);

        AppendEntriesResponse {
            term: self.metadata.current_term,
            success: true,
        }
    }

    pub fn handle_request_vote(&mut self, request: &RequestVoteRequest) -> RequestVoteResponse {
        let refuse_response = RequestVoteResponse {
            term: self.metadata.current_term,
            vote_granted: false,
        };

        if !self.peer_manager.contains(request.candidate_id) {
            return refuse_response;
        }

        if request.term < self.metadata.current_term {
            info!(
                "Refuse request vote, because request term {} is smaller than current term {}.",
                request.term, self.metadata.current_term
            );
            return refuse_response;
        }

        if let Some(last_reset_at) = self.election_timer.lock().unwrap().last_reset_at {
            if last_reset_at.elapsed() < config::ELECTION_TIMEOUT_MIN {
                return refuse_response;
            }
        }

        if request.term > self.metadata.current_term {
            self.step_down(request.term);
        } else if self.state == State::Leader || self.state == State::Candidate {
            return refuse_response;
        }

        let log_is_ok = request.term > self.log.last_term(self.snapshot.last_included_term)
            || (request.term == self.log.last_term(self.snapshot.last_included_term)
                && request.last_log_index
                    >= self.log.last_index(self.snapshot.last_included_index));
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
        self.metadata.update_voted_for(request.candidate_id);

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
        if request.term < self.metadata.current_term {
            info!(
                "Refuse snapshot from {} due to leader's term is smaller.",
                request.leader_id
            );
            return InstallSnapshotResponse {
                term: self.metadata.current_term,
            };
        }

        self.step_down(request.term);

        // Update the leader
        if self.leader_id == config::NONE_SERVER_ID {
            info!("Update leader id to {}.", request.leader_id);
            self.leader_id = request.leader_id;
        }
        if self.leader_id != request.leader_id {
            error!(
                "There are more than one leader id, current: {}, new: {}.",
                self.leader_id, request.leader_id
            );
        }

        // Write to the tmp file
        let tmp_metadata_filepath = self.snapshot.gen_snapshot_metadata_filepath(
            request.last_included_index,
            request.last_included_term,
        );
        let tmp_snapshot_filepath = self
            .snapshot
            .gen_tmp_snapshot_filepath(request.last_included_index, request.last_included_term);
        let mut tmp_metadata_file;
        let mut tmp_snapshot_file;

        if request.offset == 0 {
            // Create the tmp file
            tmp_metadata_file = std::fs::File::create(&tmp_metadata_filepath).unwrap();
            tmp_snapshot_file = std::fs::File::create(&tmp_snapshot_filepath).unwrap();
        } else {
            // Open the file
            tmp_metadata_file = std::fs::OpenOptions::new()
                .append(true)
                .open(&tmp_metadata_filepath)
                .unwrap();
            tmp_snapshot_file = std::fs::OpenOptions::new()
                .append(true)
                .open(&tmp_snapshot_filepath)
                .unwrap();
        }

        match request.r#type() {
            SnapshotDataType::Metadata => {
                tmp_metadata_file.write_all(&request.data).unwrap();
            }
            SnapshotDataType::Snapshot => {
                tmp_snapshot_file.write_all(&request.data).unwrap();
            }
        }

        if request.done {
            let metadata_filepath = self.snapshot.gen_snapshot_metadata_filepath(
                request.last_included_index,
                request.last_included_term,
            );
            let snapshot_filepath = self
                .snapshot
                .gen_snapshot_filepath(request.last_included_index, request.last_included_term);
            if let Err(e) = std::fs::rename(tmp_metadata_filepath, metadata_filepath) {
                error!("Failed to rename tmp file to snapshot file, error: {}.", e);
                return InstallSnapshotResponse {
                    term: self.metadata.current_term,
                };
            }
            if let Err(e) = std::fs::rename(tmp_snapshot_filepath, snapshot_filepath) {
                error!("Failed to rename tmp file to snapshot file, error: {}.", e);
                return InstallSnapshotResponse {
                    term: self.metadata.current_term,
                };
            }

            // Reload the metadata
            self.snapshot.reload_metadata();

            self.state_machine
                .restore_snapshot(self.snapshot.latest_snapshot_filepath().unwrap());

            self.log.truncate_prefix(self.snapshot.last_included_index);
        }

        InstallSnapshotResponse {
            term: self.snapshot.last_included_term,
        }
    }

    pub fn handle_get_leader(&mut self, _request: &GetLeaderRequest) -> GetLeaderResponse {
        if self.state == State::Leader {
            return GetLeaderResponse {
                leader: Some(Server {
                    server_id: self.server_id,
                    server_addr: self.server_addr.clone(),
                }),
            };
        }

        for peer in self.peer_manager.peers() {
            if peer.server_id == self.server_id {
                return GetLeaderResponse {
                    leader: Some(Server {
                        server_id: peer.server_id,
                        server_addr: peer.server_addr.clone(),
                    }),
                };
            }
        }
        GetLeaderResponse { leader: None }
    }

    pub fn handle_get_configuration(
        &mut self,
        _request: &GetConfigurationRequest,
    ) -> GetConfigurationResponse {
        let mut servers = Vec::new();
        for peer in self.peer_manager.peers() {
            servers.push(Server {
                server_id: peer.server_id,
                server_addr: peer.server_addr.clone(),
            })
        }
        servers.push(Server {
            server_id: self.server_id,
            server_addr: self.server_addr.clone(),
        });

        GetConfigurationResponse { servers }
    }

    pub fn handle_set_configuration(
        &mut self,
        request: &SetConfigurationRequest,
    ) -> SetConfigurationResponse {
        let refuse_reply = SetConfigurationResponse { success: false };

        if request.servers.is_empty() {
            return refuse_reply;
        }

        let last_configuration = self.log.last_configuration();

        if last_configuration.is_some() && last_configuration.unwrap().is_configuration_old_new() {
            return refuse_reply;
        }

        let success = self.append_configuration(Some(request.servers.as_ref()));

        SetConfigurationResponse { success }
    }
}
