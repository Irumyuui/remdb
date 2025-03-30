use std::collections::BTreeMap;

use crate::format::key::Seq;

/// `Watermark` just contains a map, which txn ts is lived.
#[derive(Default, Debug)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_reader(&mut self, ts: Seq) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: Seq) {
        if let Some(count) = self.readers.get_mut(&ts) {
            *count -= 1;
            if *count == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    pub fn watermark(&self) -> Option<Seq> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }
}
