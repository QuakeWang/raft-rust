use crate::consensus::Consensus;
use crate::proto;
use crate::proto::consensus_rpc_client::ConsensusRpcClient;
use crate::proto::consensus_rpc_server::{ConsensusRpc, ConsensusRpcServer};
use crate::proto::{
    AppendEntriesRequest, AppendEntriesResponse, GetConfigurationRequest, GetConfigurationResponse,
    GetLeaderRequest, GetLeaderResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SetConfigurationRequest, SetConfigurationResponse,
};
use log::info;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

// RPC Server
pub struct Server {
    pub consensus: Arc<Mutex<Consensus>>,
}

#[tokio::main]
pub async fn start_server(
    addr: &str,
    consensus: Arc<Mutex<Consensus>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse().unwrap();
    info!("Raft server listening on {}", addr);
    let consensus_server = Server {
        consensus: consensus.clone(),
    };
    let management_server = Server {
        consensus: consensus.clone(),
    };
    tonic::transport::Server::builder()
        .add_service(ConsensusRpcServer::new(consensus_server))
        .add_service(ConsensusRpcServer::new(management_server))
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
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle append entries request from {:?}, request: {:?}",
            &addr, &request
        );
        let response = Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_append_entries(&request.into_inner()),
        );
        info!(
            "Handle append entries request from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle request vote request from {:?}, request: {:?}",
            &addr, &request
        );
        let response = Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_request_vote(&request.into_inner()),
        );
        info!(
            "Handle request vote request from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle install snapshot request from {:?}, request: {:?}",
            &addr, &request
        );
        let response = Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_install_snapshot(request.into_inner()),
        );
        info!(
            "Handle install snapshot request from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }
}

#[tonic::async_trait]
impl proto::management_rpc_server::ManagementRpc for Server {
    async fn get_leader(
        &self,
        request: Request<GetLeaderRequest>,
    ) -> Result<Response<GetLeaderResponse>, Status> {
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle get leader from {:?}, request: {:?}.",
            &addr, &request
        );
        let response = tonic::Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_get_leader(&request.into_inner()),
        );
        info!(
            "Handle get leader from {:?}, response: {:?}.",
            &addr, &response
        );
        Ok(response)
    }

    async fn get_configuration(
        &self,
        request: Request<GetConfigurationRequest>,
    ) -> Result<Response<GetConfigurationResponse>, Status> {
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle get configuration from {:?}, request: {:?}.",
            &addr, &request
        );
        let response = tonic::Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_get_configuration(&request.into_inner()),
        );
        info!(
            "Handle get configuration from {:?}, response: {:?}.",
            &addr, &response
        );
        Ok(response)
    }

    async fn set_configuration(
        &self,
        request: Request<SetConfigurationRequest>,
    ) -> Result<Response<SetConfigurationResponse>, Status> {
        let addr = request.remote_addr().unwrap();
        info!(
            "Handle set configuration from {:?}, request: {:?}.",
            &addr, &request
        );
        let response = Response::new(
            self.consensus
                .lock()
                .unwrap()
                .handle_set_configuration(&request.into_inner()),
        );
        info!(
            "Handle set configuration from {:?}, response: {:?}.",
            &addr, &response
        );
        Ok(response)
    }
}

// RPC Client
#[derive(Debug)]
pub struct Client {}

impl Client {
    pub async fn append_entries(
        &mut self,
        request: AppendEntriesRequest,
        addr: String,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error>> {
        let addr_clone = addr.clone();
        let request = tonic::Request::new(request);
        info!(
            "Send append entries request to {:?}, request: {:?}",
            &addr_clone, &request
        );
        let mut client = ConsensusRpcClient::connect(addr).await?;
        let response = client.append_entries(request).await?;
        info!(
            "Send append entries request to {:?}, response: {:?}",
            &addr_clone, &response
        );
        Ok(response.into_inner())
    }

    pub async fn request_vote(
        &mut self,
        request: RequestVoteRequest,
        addr: String,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error>> {
        let addr_clone = addr.clone();
        let request = tonic::Request::new(request);
        info!(
            "Send request vote request to {:?}, request: {:?}",
            &addr_clone, &request
        );
        let mut client = ConsensusRpcClient::connect(addr).await?;
        let response = client.request_vote(request).await?;
        info!(
            "Send request vote request to {:?}, response: {:?}",
            &addr_clone, &response
        );
        Ok(response.into_inner())
    }
}
