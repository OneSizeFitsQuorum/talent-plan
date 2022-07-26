use futures::StreamExt;
use labrpc::Error;
use labrpc::Error::Timeout;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{thread, time};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    timestamp_generator: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        Ok(TimestampResponse {
            timestamp: self.timestamp_generator.fetch_add(1, Ordering::Relaxed),
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

#[derive(Debug)]
pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let ts_start_inclusive = ts_start_inclusive.unwrap_or(0);
        let ts_end_inclusive = ts_end_inclusive.unwrap_or(u64::MAX);
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        map.range((
            Included((key.clone(), ts_start_inclusive)),
            Included((key, ts_end_inclusive)),
        ))
        .last()
    }

    #[inline]
    fn contains_in_write_column(&self, primary: Vec<u8>, start_ts: u64) -> Option<u64> {
        // Your code here.
        for (key, value) in self.write.range((
            Included((primary.clone(), 0)),
            Included((primary, u64::MAX)),
        )) {
            match value {
                Value::Timestamp(time) if *time == start_ts => return Some(*time),
                _ => continue,
            }
        }
        None
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let mut map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, start_ts: u64) {
        // Your code here.
        let mut map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, start_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        loop {
            let kv = self.data.lock().unwrap();
            if kv
                .read(req.key.clone(), Column::Lock, None, Some(req.start_ts))
                .is_some()
            {
                drop(kv);
                self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone());
                continue;
            }
            let latest_write = kv.read(req.key.clone(), Column::Write, None, Some(req.start_ts));
            if latest_write.is_none() {
                return Ok(GetResponse { value: Vec::new() });
            }
            if let Value::Timestamp(data_ts) = latest_write.unwrap().1 {
                if let Value::Vector(result) = kv
                    .read(req.key, Column::Data, Some(*data_ts), Some(*data_ts))
                    .unwrap()
                    .1
                {
                    return Ok(GetResponse {
                        value: result.to_vec(),
                    });
                } else {
                    panic!("Could not find the corresponding value")
                }
            } else {
                panic!(
                    "Unexpected value {:?} in write column",
                    latest_write.unwrap().1
                )
            }
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // // Your code here.
        let mut kv = self.data.lock().unwrap();
        if kv
            .read(req.key.clone(), Column::Write, Some(req.start_ts), None)
            .is_some()
        {
            return Ok(PrewriteResponse { success: false });
        }
        if kv.read(req.key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse { success: false });
        }
        info!(
            "write key=({:?}, {}), value={:?} to Column {:?}",
            req.key,
            req.start_ts,
            Value::Vector(req.value.clone()),
            Column::Data
        );
        kv.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );
        info!(
            "write key=({:?}, {}), value={:?} to Column {:?}",
            req.key,
            req.start_ts,
            Value::Vector(req.primary.clone()),
            Column::Lock
        );
        kv.write(
            req.key,
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary),
        );
        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut kv = self.data.lock().unwrap();
        if kv
            .read(
                req.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            )
            .is_none()
        {
            return Ok(CommitResponse { success: false });
        }
        info!(
            "write key=({:?}, {}), value={:?} to Column {:?}",
            req.key,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
            Column::Write
        );
        kv.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        info!(
            "erase key=({:?}, {}) in Column {:?}",
            req.key,
            req.start_ts,
            Column::Lock
        );
        kv.erase(req.key.clone(), Column::Lock, req.start_ts);
        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        thread::sleep(Duration::from_nanos(TTL));
        let mut kv = self.data.lock().unwrap();
        if let Some(entry) = kv.read(key.clone(), Column::Lock, None, Some(start_ts)) {
            let ts = entry.0 .1;
            if let Value::Vector(primary) = entry.1 {
                match kv.contains_in_write_column(primary.clone(), ts) {
                    None => {
                        info!(
                            "Recovery rollback tx: erase key=({:?}, {}) in Column {:?}",
                            key,
                            ts,
                            Column::Lock
                        );
                        kv.erase(key, Column::Lock, ts);
                    }
                    Some(commit_ts) => {
                        info!("erase key=({:?}, {}) in Column {:?}", key, ts, Column::Lock);
                        kv.erase(key.clone(), Column::Lock, ts);
                        info!(
                            "Recovery commit tx: write key=({:?}, {}), value={:?} to Column {:?}",
                            key,
                            commit_ts,
                            Value::Timestamp(ts),
                            Column::Write
                        );
                        kv.write(key, Column::Write, commit_ts, Value::Timestamp(ts));
                    }
                }
            } else {
                panic!("Unexpected error")
            }
        }
    }
}
