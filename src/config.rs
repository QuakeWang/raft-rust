use std::time::Duration;

// Election timeout
pub const ELECTION_TIMEOUT_MIN_MILLIS: u64 = 10000;
pub const ELECTION_TIMEOUT_MAX_MILLIS: u64 = 15000;

// Heartbeat interval
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(3000);

// Snapshot interval
pub const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(30000);

// None server id
pub const NONE_SERVER_ID: u64 = 0;

// None data
pub const NONE_DATA: &'static str = "None";
