use raft_rust::proto::consensus_rpc_client::ConsensusRpcClient;
use raft_rust::proto::RequestVoteRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ConsensusRpcClient::connect("http://[::1]:9091").await?;

    let request = tonic::Request::new(RequestVoteRequest {
        term: 1,
        candidate_id: 1,
        last_log_index: 1,
        last_log_term: 1,
    });

    let response = client.request_vote(request).await?;

    println!("RESPONSE = {:?}", response);

    Ok(())
}
