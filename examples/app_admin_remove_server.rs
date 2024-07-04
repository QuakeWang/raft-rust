#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello World!");

    let mut client =
        raft_rust::proto::management_rpc_client::ManagementRpcClient::connect("http://[::1]:9091")
            .await?;

    let set_configuration_request =
        tonic::Request::new(raft_rust::proto::SetConfigurationRequest {
            servers: vec![
                raft_rust::proto::Server {
                    server_id: 1,
                    server_addr: "http://[::1]:9091".to_string(),
                },
                raft_rust::proto::Server {
                    server_id: 2,
                    server_addr: "http://[::1]:9092".to_string(),
                },
            ],
        });

    let set_configuration_response = client.set_configuration(set_configuration_request).await?;

    println!(
        "Set Configuration Response: {:?}",
        set_configuration_response
    );

    Ok(())
}
