use crate::config::Configuration;
use crate::proto;
use crate::proto::LogEntry;
use log::error;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref VIRTUAL_LOG_ENTRY: LogEntry = LogEntry {
        index: 0,
        term: 0,
        r#entry_type: proto::EntryType::Noop.into(),
        data: "".as_bytes().to_vec(),
    };
}

pub type LogEntryData = (proto::EntryType, Vec<u8>);

#[derive(Debug)]
pub struct Log {
    entries: Vec<LogEntry>,
    start_index: u64,
    append_mutex: Mutex<String>,
}

impl Log {
    pub fn new(start_index: u64) -> Self {
        Self {
            entries: Vec::new(),
            start_index,
            append_mutex: Mutex::new("".to_string()),
        }
    }

    pub fn append_data(&mut self, term: u64, entry_data: Vec<LogEntryData>) {
        // Prevent the insertion of duplicate log entries with the same index
        if let Ok(_) = self.append_mutex.lock() {
            for entry_data in entry_data {
                let entry = LogEntry {
                    index: self.last_index() + 1,
                    term,
                    r#entry_type: entry_data.0.into(),
                    data: entry_data.1,
                };
                self.entries.push(entry);
            }
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

    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.start_index - 1)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|entry| entry.term).unwrap_or(0)
    }

    pub fn truncate_suffix(&mut self, last_index_kept: u64) {
        if last_index_kept < self.start_index {
            return;
        }
        self.entries
            .truncate((last_index_kept - self.start_index + 1) as usize);
    }

    pub fn last_configuration(&self) -> Configuration {
        for entry in self.entries().iter().rev() {
            if entry.entry_type() == proto::EntryType::Configuration {
                return Configuration::from_data(&entry.data.as_ref());
            }
        }

        return Configuration {
            old_servers: Vec::new(),
            new_servers: Vec::new(),
        };
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_log() {
        let mut log = super::Log::new(1);
        log.append_data(
            1,
            vec![(super::proto::EntryType::Data, "test1".as_bytes().to_vec())],
        );

        log.append_data(
            1,
            vec![(super::proto::EntryType::Data, "test2".as_bytes().to_vec())],
        );

        println!("{:?}", log);
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.start_index(), 1);
        assert_eq!(log.entry(1).unwrap().data, "test1".as_bytes());
        assert_eq!(log.entry(2).unwrap().data, "test2".as_bytes());
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.pack_entries(1).len(), 2);
        assert_eq!(log.pack_entries(2).len(), 1);
        assert_eq!(log.pack_entries(3).len(), 0);
    }

    #[test]
    fn test_log_truncate() {
        let mut log = super::Log::new(1);
        log.append_data(
            1,
            vec![(super::proto::EntryType::Data, "test1".as_bytes().to_vec())],
        );

        log.append_data(
            1,
            vec![(super::proto::EntryType::Data, "test2".as_bytes().to_vec())],
        );

        log.truncate_suffix(1);
        assert_eq!(log.entries().len(), 1);
    }
}
