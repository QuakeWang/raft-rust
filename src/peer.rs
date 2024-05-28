#[derive(Debug)]
pub struct Peer {
    server_id: u64,
    pub(crate) server_addr: String,
    pub(crate) next_index: u64,
    match_index: u64,
    vote_granted: bool,
}

impl Peer {
    pub fn new(server_id: u64, server_addr: String) -> Self {
        Self {
            server_id,
            server_addr,
            next_index: 1,
            match_index: 0,
            vote_granted: false,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peers() {
        let mut peer_manager = PeerManager::new();
        let peer1 = super::Peer {
            server_id: 1,
            server_addr: "127.0.0.1:4001".to_string(),
            next_index: 3,
            match_index: 2,
            vote_granted: false,
        };
        let peer2 = Peer {
            server_id: 2,
            server_addr: "127.0.0.1:4002".to_string(),
            next_index: 2,
            match_index: 2,
            vote_granted: false,
        };
        peer_manager.add_peers(vec![peer1, peer2]);
        println!("{:?}", peer_manager);
        assert_eq!(peer_manager.peers().len(), 2);
    }
}
