#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    let mut client =
        raft_rust::proto::management_rpc_client::ManagementRpcClient::connect("http://[::1]:9091")
            .await?;
    let get_leader_req = tonic::Request::new(raft_rust::proto::GetLeaderRequest {});
    let get_leader_resp = client.get_leader(get_leader_req).await?;
    println!("GetLeader response={:?}", get_leader_resp);

    let get_configuration_request =
        tonic::Request::new(raft_rust::proto::GetConfigurationRequest {});
    let get_configuration_response = client.get_configuration(get_configuration_request).await?;
    println!("GetConfiguration response={:?}", get_configuration_response);
    Ok(())
}
