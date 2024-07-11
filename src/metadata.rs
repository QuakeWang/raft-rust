use crate::config;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Debug, Deserialize, Serialize)]
pub struct Metadata {
    pub current_term: u64,
    pub voted_for: u64,
    pub metadata_dir: String,
}

impl Metadata {
    pub fn reload(metadata_dir: String) -> Metadata {
        let filepath = Metadata::gen_metadata_filepath(&metadata_dir);
        return if std::path::Path::new(&filepath).exists() {
            let mut metadata_file = std::fs::File::open(filepath).unwrap();
            let mut metadata_json = String::new();
            metadata_file
                .read_to_string(&mut metadata_json)
                .expect("Failed to read raft metadata file.");
            let metadata: Metadata = serde_json::from_str(metadata_json.as_str()).unwrap();
            Metadata {
                current_term: metadata.current_term,
                voted_for: metadata.voted_for,
                metadata_dir,
            }
        } else {
            Metadata {
                current_term: 0,
                voted_for: config::NONE_SERVER_ID,
                metadata_dir,
            }
        };
    }

    pub fn gen_metadata_filepath(metadata_dir: &String) -> String {
        format!("{}/raft.metadata", metadata_dir)
    }

    pub fn update_current_term(&mut self, current_term: u64) {
        self.current_term = current_term;
        self.dump();
    }

    pub fn update_voted_for(&mut self, voted_for: u64) {
        self.voted_for = voted_for;
        self.dump();
    }

    pub fn dump(&self) {
        let metadata_filepath = Metadata::gen_metadata_filepath(&self.metadata_dir);
        let mut metadata_file = std::fs::File::create(metadata_filepath).unwrap();
        let metadata_json = serde_json::to_string(self).unwrap();
        if let Err(e) = metadata_file.write(metadata_json.as_bytes()) {
            panic!("Failed to write raft metadata file, error: {}.", e)
        }
    }
}