use crate::proto;

#[derive(Debug)]
pub struct Log {
    entries: Vec<proto::LogEntry>,
    start_index: u64,
}

impl Log {
    pub fn new(start_index: u64) -> Self {
        Self {
            entries: Vec::new(),
            start_index,
        }
    }

    pub fn append_entries(&mut self, entries: Vec<proto::LogEntry>) {
        self.entries.extend(entries);
    }

    pub fn entries(&self) -> &Vec<proto::LogEntry> {
        &self.entries
    }

    pub fn get_entry(&self, index: u64) -> Option<&proto::LogEntry> {
        self.entries.get(index as usize)
    }

    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    pub fn last_index(&self) -> u64 {
        self.start_index + self.entries.len() as u64 - 1
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().unwrap().term
    }
}

#[cfg(test)]
mod tests {
    use crate::log::proto::LogEntry;

    #[test]
    fn test_log() {
        let mut log = super::Log::new(1);
        let entry1 = LogEntry {
            index: 1,
            term: 1,
            entry_type: 1,
            data: "test".as_bytes().to_vec(),
        };
        log.append_entries(vec![entry1]);

        let entry2 = LogEntry {
            index: 2,
            term: 1,
            entry_type: 1,
            data: "test".as_bytes().to_vec(),
        };
        log.append_entries(vec![entry2]);

        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.start_index(), 1);
        assert_eq!(log.get_entry(1).unwrap().index, 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
    }
}
