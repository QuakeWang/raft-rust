use crate::config;
use log::info;
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Debug, Deserialize, Serialize)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub configuration: Option<config::Configuration>,
    pub snapshot_dir: String,
}

impl Snapshot {
    pub fn new(snapshot_dir: String) -> Self {
        Self {
            last_included_index: 0,
            last_included_term: 0,
            configuration: None,
            snapshot_dir,
        }
    }

    pub fn take_snapshot_metadata(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
        configuration: Option<config::Configuration>,
    ) {
        info!("Start to take snapshot metadata, last_included_index: {}, last_included_term: {}, configuration: {:?}",
            last_included_index, last_included_term, configuration.as_ref());
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        self.configuration = configuration;

        let metadata_filepath =
            self.gen_snapshot_metadata_filepath(last_included_index, last_included_term);
        let mut medata_file = std::fs::File::create(metadata_filepath.clone()).unwrap();

        let metadata_json = serde_json::to_string(self).unwrap();
        if let Err(e) = medata_file.write(metadata_json.as_bytes()) {
            panic!("Failed to write metadata file, error: {}", e);
        }
        info!(
            "Take snapshot metadata successfully, metadata_filepath: {}",
            metadata_filepath
        );
    }

    pub fn gen_snapshot_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }

    pub fn gen_snapshot_metadata_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.metadata",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }
}
