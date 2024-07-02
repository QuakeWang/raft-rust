use crate::config::ConfigurationState;

#[derive(Debug)]
pub struct Peer {
    pub(crate) server_id: u64,
    pub(crate) server_addr: String,
    pub(crate) next_index: u64,
    pub(crate) match_index: u64,
    pub(crate) vote_granted: bool,
    pub configuration_state: ConfigurationState,
}

impl Peer {
    pub fn new(server_id: u64, server_addr: String) -> Self {
        Self {
            server_id,
            server_addr,
            next_index: 1,
            match_index: 0,
            vote_granted: false,
            configuration_state: ConfigurationState::new(),
        }
    }
}

#[derive(Debug)]
pub struct PeerManager {
    peers: Vec<Peer>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self { peers: Vec::new() }
    }

    pub fn add_peers(&mut self, peers: Vec<Peer>) {
        self.peers.extend(peers);
    }

    pub fn peers(&self) -> Vec<&Peer> {
        self.peers.iter().collect()
    }

    pub fn peer(&mut self, server_id: u64) -> Option<&mut Peer> {
        self.peers
            .iter_mut()
            .find(|peer| peer.server_id == server_id)
    }

    pub fn peers_mut(&mut self) -> &mut Vec<Peer> {
        &mut self.peers
    }

    pub fn peers_num(&self) -> usize {
        self.peers.len()
    }

    pub fn peer_server_ids(&self) -> Vec<u64> {
        self.peers.iter().map(|peer| peer.server_id).collect()
    }

    pub fn reset_vote(&mut self) {
        self.peers_mut()
            .iter_mut()
            .for_each(|peer| peer.vote_granted = false)
    }

    pub fn contains(&self, server_id: u64) -> bool {
        self.peers
            .iter()
            .find(|peer| peer.server_id == server_id)
            .is_some()
    }

    // Get the most match index
    pub fn quorum_match_index(
        &self,
        leader_configuration_state: &ConfigurationState,
        leader_last_index: u64,
    ) -> u64 {
        let mut new_match_index = Vec::new();
        if leader_configuration_state.in_new {
            new_match_index.push(leader_last_index);
        }
        for peer in self.peers.iter() {
            if peer.configuration_state.in_new {
                new_match_index.push(peer.match_index);
            }
        }
        new_match_index.sort();
        let new_quorum_match_index = {
            if new_match_index.len() == 0 {
                u64::MAX
            } else {
                new_match_index
                    .get((new_match_index.len() - 1) / 2)
                    .unwrap()
                    .clone()
            }
        };

        let mut old_match_index = Vec::new();
        if leader_configuration_state.in_old {
            old_match_index.push(leader_last_index);
        }
        for peer in self.peers.iter() {
            if peer.configuration_state.in_old {
                old_match_index.push(peer.match_index);
            }
        }
        old_match_index.sort();
        let old_quorum_match_index = {
            if old_match_index.len() == 0 {
                u64::MAX
            } else {
                old_match_index
                    .get((old_match_index.len() - 1) / 2)
                    .unwrap()
                    .clone()
            }
        };

        new_quorum_match_index.min(old_quorum_match_index)
    }

    pub fn quorum_vote_granted(&self, leader_configuration_state: &ConfigurationState) -> bool {
        let mut total_new_servers = 0;
        let mut granted_new_servers = 0;

        let mut total_old_servers = 0;
        let mut granted_old_servers = 0;

        if leader_configuration_state.in_new {
            total_new_servers += 1;
            granted_new_servers += 1;
        }

        if leader_configuration_state.in_old {
            total_old_servers += 1;
            granted_old_servers += 1;
        }

        for peer in self.peers.iter() {
            if peer.configuration_state.in_new {
                total_new_servers += 1;
                if peer.vote_granted {
                    granted_new_servers += 1;
                }
            }
            if peer.configuration_state.in_old {
                total_old_servers += 1;
                if peer.vote_granted {
                    granted_old_servers += 1;
                }
            }
        }

        let new_server_quorum =
            { total_new_servers == 0 || granted_new_servers >= (total_new_servers / 2) };
        let old_server_quorum =
            { total_old_servers == 0 || granted_old_servers >= (total_old_servers / 2) };
        return new_server_quorum && old_server_quorum;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peers() {
        let mut peer_manager = PeerManager::new();
        let peer1 = Peer {
            server_id: 1,
            server_addr: "127.0.0.1:4001".to_string(),
            next_index: 3,
            match_index: 2,
            vote_granted: false,
            configuration_state: ConfigurationState::new(),
        };
        let peer2 = Peer {
            server_id: 2,
            server_addr: "127.0.0.1:4002".to_string(),
            next_index: 2,
            match_index: 2,
            vote_granted: false,
            configuration_state: ConfigurationState::new(),
        };
        peer_manager.add_peers(vec![peer1, peer2]);
        println!("{:?}", peer_manager);
        assert_eq!(peer_manager.peers().len(), 2);
    }
}
