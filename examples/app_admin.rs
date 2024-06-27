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

    let set_configuration_request =
        tonic::Request::new(raft_rust::proto::SetConfigurationRequest {
            servers: vec![
                raft_rust::proto::Server {
                    server_id: 3,
                    server_addr: "127.0.0.1:9093".to_string(),
                },
                raft_rust::proto::Server {
                    server_id: 4,
                    server_addr: "127.0.0.1:9094".to_string(),
                },
            ],
        });

    let set_configuration_response = client.set_configuration(set_configuration_request).await?;
    println!("SetConfiguration response={:?}", set_configuration_response);

    Ok(())
}
