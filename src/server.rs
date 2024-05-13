use crate::consensus::Consensus;
use crate::proto;
use crate::proto::consensus_rpc_server::ConsensusRpc;
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use consensus_rpc_server::ConsensusRpcServer;
use proto::consensus_rpc_server;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Request, Response, Status};

pub struct Server {
    consensus: Arc<Mutex<Consensus>>,
}

pub fn start() -> Arc<Mutex<Consensus>> {
    // Create a new consensus instance
    let consensus = Consensus::new(9091);

    // Start the rpc server
    let server = Server {
        consensus: consensus.clone(),
    };
    std::thread::spawn(move || {
        if let Err(_) = start_server("[::1]:9091", server) {
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
        .schedule(Duration::from_secs(3), move || {
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
        .schedule(Duration::from_secs(5), move || {
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
        .schedule(Duration::from_secs(60), move || {
            weak_consensus
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .handle_election_timeout();
        });
    consensus
}

pub fn stop() {
    todo!()
}

#[tokio::main]
async fn start_server(addr: &str, server: Server) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse().unwrap();

    println!("Raft server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(ConsensusRpcServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl ConsensusRpc for Server {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        Ok(tonic::Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_append_entries(&request.into_inner()),
        ))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        Ok(tonic::Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_request_vote(request.into_inner()),
        ))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        Ok(tonic::Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_install_snapshot(request.into_inner()),
        ))
    }
}
