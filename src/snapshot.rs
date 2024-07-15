use crate::config;
use log::info;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

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

    pub fn reload_metadata(&mut self) {
        if let Some(filepath) = self.latest_metadata_filepath() {
            let mut metadata_file = std::fs::File::open(filepath).unwrap();
            let mut metadata_json = String::new();
            metadata_file
                .read_to_string(&mut metadata_json)
                .expect("Failed to read snapshot metadata.");
            let snapshot: Snapshot = serde_json::from_str(metadata_json.as_str()).unwrap();

            self.last_included_index = snapshot.last_included_index;
            self.last_included_term = snapshot.last_included_term;
            self.configuration = snapshot.configuration;
        } else {
            info!("No snapshot file found when reloading.")
        }
    }

    pub fn latest_snapshot_filepath(&mut self) -> Option<String> {
        let result = std::fs::read_dir(self.snapshot_dir.clone()).unwrap();
        let mut latest_index_term: (u64, u64) = (0, 0);
        for entry in result {
            let entry = entry.unwrap();
            let filename = entry.file_name();
            let filename = filename.to_str().unwrap();
            if filename.ends_with(".snapshot") {
                let re = regex::Regex::new(r"raft-(\d+)-(\d+).snapshot").unwrap();
                let cap = re.captures(filename).unwrap();

                let index = cap.get(1).unwrap().as_str().parse::<u64>().unwrap();
                let term = cap.get(2).unwrap().as_str().parse::<u64>().unwrap();

                if index > latest_index_term.0
                    || (index == latest_index_term.0 && term > latest_index_term.1)
                {
                    latest_index_term = (index, term);
                }
            }
        }
        return if latest_index_term.0 != 0 && latest_index_term.1 != 0 {
            Some(format!(
                "{}/raft-{}-{}.snapshot",
                &self.snapshot_dir, latest_index_term.0, latest_index_term.1
            ))
        } else {
            None
        };
    }

    pub fn latest_metadata_filepath(&mut self) -> Option<String> {
        let result = std::fs::read_dir(self.snapshot_dir.clone()).unwrap();
        let mut latest_index_term: (u64, u64) = (0, 0);
        for entry in result {
            let entry = entry.unwrap();
            let filename = entry.file_name();
            let filename = filename.to_str().unwrap();

            if filename.ends_with(".snapshot.metadata") {
                let re = regex::Regex::new(r"raft-(\d+)-(\d+).snapshot.metadata").unwrap();
                let cap = re.captures(filename).unwrap();

                let index = cap.get(1).unwrap().as_str().parse::<u64>().unwrap();
                let term = cap.get(2).unwrap().as_str().parse::<u64>().unwrap();

                if index > latest_index_term.0
                    || (index == latest_index_term.0 && term > latest_index_term.1)
                {
                    latest_index_term = (index, term);
                }
            }
        }
        return if latest_index_term.0 != 0 && latest_index_term.1 != 0 {
            Some(format!(
                "{}/raft-{}-{}.snapshot.metadata",
                &self.snapshot_dir, latest_index_term.0, latest_index_term.1
            ))
        } else {
            None
        };
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

    pub fn gen_tmp_snapshot_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.tmp",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }

    pub fn gen_tmp_snapshot_metadata_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.metadata.tmp",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }
}
