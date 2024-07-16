#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello World!");

    let mut client =
        raft_rust::proto::management_rpc_client::ManagementRpcClient::connect("http://[::1]:9091")
            .await?;

    let get_leader_req = tonic::Request::new(raft_rust::proto::GetLeaderRequest {});
    let get_leader_resp = client.get_leader(get_leader_req).await?;
    println!("GetLeader response={:?}", get_leader_resp);

    let get_configuration_req = tonic::Request::new(raft_rust::proto::GetConfigurationRequest {});
    let get_configuration_resp = client.get_configuration(get_configuration_req).await?;
    println!("GetConfiguration response={:?}", get_configuration_resp);

    let set_configuration_req = tonic::Request::new(raft_rust::proto::SetConfigurationRequest {
        servers: vec![
            raft_rust::proto::Server {
                server_id: 1,
                server_addr: "http://[::1]:9091".to_string(),
            },
            raft_rust::proto::Server {
                server_id: 2,
                server_addr: "http://[::1]:9092".to_string(),
            },
            raft_rust::proto::Server {
                server_id: 3,
                server_addr: "http://[::1]:9093".to_string(),
            },
            raft_rust::proto::Server {
                server_id: 4,
                server_addr: "http://[::1]:9094".to_string(),
            },
        ],
    });

    let set_configuration_resp = client.set_configuration(set_configuration_req).await?;
    println!("SetConfiguration response={:?}.", set_configuration_resp);

    Ok(())
}
