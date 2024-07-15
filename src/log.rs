use crate::config::Configuration;
use crate::proto;
use crate::proto::LogEntry;
use log::error;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref VIRTUAL_LOG_ENTRY: LogEntry = LogEntry {
        term: 0,
        index: 0,
        entry_type: proto::EntryType::Noop.into(),
        data: "".as_bytes().to_vec(),
    };
}

pub type LogEntryData = (proto::EntryType, Vec<u8>);

#[derive(Debug, Deserialize, Serialize)]
pub struct Log {
    entries: Vec<LogEntry>,
    start_index: u64,
    metadata_dir: String,
    #[serde(skip)]
    append_mutex: Mutex<String>,
}

impl Log {
    pub fn new(start_index: u64, metadata_dir: String) -> Self {
        Self {
            entries: Vec::new(),
            start_index,
            metadata_dir,
            append_mutex: Mutex::new("".to_string()),
        }
    }

    pub fn append_data(&mut self, term: u64, entry_data: Vec<LogEntryData>) {
        // Prevent the insertion of duplicate log entries with the same index
        if let Ok(_) = self.append_mutex.lock() {
            for entry in entry_data {
                let log_entry = LogEntry {
                    index: self.last_index(0) + 1,
                    term,
                    entry_type: entry.0.into(),
                    data: entry.1,
                };
                self.entries.push(log_entry);
            }
            self.dump();
        } else {
            error!("Append log entry failed due to lock failure!");
            return;
        }
    }

    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        // Prevent the insertion of duplicate log entries with the same index
        if let Ok(_) = self.append_mutex.lock() {
            for entry in entries {
                self.entries.push(entry);
            }
            self.dump();
        } else {
            error!("Append log entry failed due to lock failure!");
            return;
        }
    }

    pub fn entries(&self) -> &Vec<LogEntry> {
        &self.entries
    }

    pub fn entry(&self, index: u64) -> Option<&LogEntry> {
        if index < self.start_index {
            return Some(&VIRTUAL_LOG_ENTRY);
        }
        self.entries.get((index - self.start_index) as usize)
    }

    pub fn pack_entries(&self, next_index: u64) -> Vec<LogEntry> {
        let mut res = Vec::new();
        if next_index < self.start_index {
            return res;
        }

        for entry in self
            .entries
            .iter()
            .skip((next_index - self.start_index) as usize)
        {
            res.push(entry.clone());
        }

        res
    }

    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    pub fn last_index(&self, last_included_index: u64) -> u64 {
        // If the log is empty, return the last_included_index
        if self.entries.is_empty() && last_included_index > 0 {
            return last_included_index;
        }
        self.entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.start_index - 1)
    }

    pub fn prev_log_term(
        &self,
        prev_log_index: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> u64 {
        if prev_log_index == last_included_index {
            return last_included_term;
        }
        return self.entry(prev_log_index).unwrap().term;
    }

    pub fn last_term(&self, last_included_term: u64) -> u64 {
        if self.entries.is_empty() && last_included_term > 0 {
            return last_included_term;
        }
        self.entries.last().map(|entry| entry.term).unwrap_or(0)
    }

    pub fn truncate_suffix(&mut self, last_index_kept: u64) {
        if last_index_kept < self.start_index {
            return;
        }
        self.entries
            .truncate((last_index_kept - self.start_index + 1) as usize);
        self.dump();
    }

    pub fn truncate_prefix(&mut self, last_included_index: u64) {
        if last_included_index < self.start_index {
            return;
        }
        // Update index of the log
        let last_index = self.last_index(0);
        if last_index < last_included_index {
            self.entries.clear()
        } else {
            self.entries
                .drain(0..(last_included_index - self.start_index + 1) as usize);
        }
        self.start_index = last_included_index + 1;
        self.dump();
    }

    pub fn committed_entries_len(&self, commit_index: u64) -> usize {
        if commit_index < self.start_index {
            return 0;
        }
        return (commit_index - self.start_index + 1) as usize;
    }

    pub fn last_configuration(&self) -> Option<Configuration> {
        for entry in self.entries().iter().rev() {
            if entry.entry_type() == proto::EntryType::Configuration {
                return Some(Configuration::from_data(&entry.data.as_ref()));
            }
        }
        None
    }

    pub fn gen_log_filepath(metadata_dir: &String) -> String {
        format!("{}/raft.log", metadata_dir)
    }

    pub fn reload(&mut self) {
        let filepath = Log::gen_log_filepath(&self.metadata_dir);
        if std::path::Path::new(&filepath).exists() {
            let mut log_file = std::fs::File::open(filepath).unwrap();
            let mut log_json = String::new();
            log_file
                .read_to_string(&mut log_json)
                .expect("Failed to read raft log.");
            let log: Log = serde_json::from_str(log_json.as_str()).unwrap();
            self.entries = log.entries;
            self.start_index = log.start_index;
        }
    }

    // Save log to disk
    pub fn dump(&self) {
        let log_filepath = Log::gen_log_filepath(&self.metadata_dir);
        let mut log_file = std::fs::File::create(log_filepath).unwrap();
        let log_json = serde_json::to_string(self).unwrap();
        if let Err(e) = log_file.write(log_json.as_bytes()) {
            panic!("Failed to write raft log file, error: {}.", e)
        }
    }
}
