//! # ternary-reassembly
//!
//! Message/packet reassembly for GPU cluster communication where fragment status
//! is ternary: `+1` (complete), `0` (pending), `-1` (missing).
//!
//! Features: fragment buffering with timeout tracking, ternary completion status,
//! gap detection, partial reassembly with forward progress, TTL-based expiry,
//! and reassembly statistics.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Ternary fragment status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentStatus {
    /// Fragment data has been received and verified.
    Complete = 1,
    /// Fragment has not been seen yet; still waiting.
    Pending = 0,
    /// Fragment is confirmed missing (e.g. negative ACK or timeout).
    Missing = -1,
}

/// A single fragment within a message buffer.
#[derive(Debug, Clone)]
pub struct Fragment {
    pub index: usize,
    pub status: FragmentStatus,
    pub data: Option<Vec<u8>>,
    pub last_updated: Instant,
}

/// Tracks forward-progress information for partial reassembly.
#[derive(Debug, Clone, Default)]
pub struct Progress {
    /// Highest contiguous index that is complete (front-fill progress).
    pub contiguous_front: usize,
    /// Total complete fragments.
    pub complete_count: usize,
    /// Total pending fragments.
    pub pending_count: usize,
    /// Total missing fragments.
    pub missing_count: usize,
}

/// Per-message reassembly statistics (recorded when a message completes or expires).
#[derive(Debug, Clone)]
pub struct ReassemblyRecord {
    pub message_id: u64,
    pub total_fragments: usize,
    pub completed: bool,
    pub reassembly_time: Duration,
    pub fragments_completed: usize,
}

/// Statistics across all messages.
#[derive(Debug, Clone, Default)]
pub struct ReassemblyStats {
    pub total_messages: usize,
    pub completed_messages: usize,
    pub expired_messages: usize,
    pub completion_rate: f64,
    pub average_reassembly_time: Duration,
}

/// A buffered message undergoing reassembly.
#[derive(Debug)]
pub struct MessageBuffer {
    pub message_id: u64,
    pub total_fragments: usize,
    pub fragments: Vec<Fragment>,
    pub created_at: Instant,
    pub completed_at: Option<Instant>,
    pub ttl: Duration,
}

impl MessageBuffer {
    pub fn new(message_id: u64, total_fragments: usize, ttl: Duration) -> Self {
        let now = Instant::now();
        let fragments = (0..total_fragments)
            .map(|index| Fragment {
                index,
                status: FragmentStatus::Pending,
                data: None,
                last_updated: now,
            })
            .collect();
        Self {
            message_id,
            total_fragments,
            fragments,
            created_at: now,
            completed_at: None,
            ttl,
        }
    }

    /// Mark a fragment complete with its data.
    pub fn mark_complete(&mut self, index: usize, data: Vec<u8>) -> Result<(), &'static str> {
        if index >= self.total_fragments {
            return Err("fragment index out of range");
        }
        let now = Instant::now();
        self.fragments[index].status = FragmentStatus::Complete;
        self.fragments[index].data = Some(data);
        self.fragments[index].last_updated = now;

        // Check full completion.
        if self.fragments.iter().all(|f| f.status == FragmentStatus::Complete) {
            self.completed_at = Some(now);
        }
        Ok(())
    }

    /// Mark a fragment as missing.
    pub fn mark_missing(&mut self, index: usize) -> Result<(), &'static str> {
        if index >= self.total_fragments {
            return Err("fragment index out of range");
        }
        self.fragments[index].status = FragmentStatus::Missing;
        self.fragments[index].last_updated = Instant::now();
        Ok(())
    }

    /// Overall ternary completion status for this message.
    pub fn ternary_status(&self) -> FragmentStatus {
        let all_complete = self.fragments.iter().all(|f| f.status == FragmentStatus::Complete);
        if all_complete {
            FragmentStatus::Complete
        } else if self.fragments.iter().any(|f| f.status == FragmentStatus::Missing) {
            FragmentStatus::Missing
        } else {
            FragmentStatus::Pending
        }
    }

    /// Return indices of missing (-1) fragments.
    pub fn gaps(&self) -> Vec<usize> {
        self.fragments
            .iter()
            .filter(|f| f.status == FragmentStatus::Missing)
            .map(|f| f.index)
            .collect()
    }

    /// Return indices of pending (0) fragments.
    pub fn pending_indices(&self) -> Vec<usize> {
        self.fragments
            .iter()
            .filter(|f| f.status == FragmentStatus::Pending)
            .map(|f| f.index)
            .collect()
    }

    /// Compute forward progress: contiguous front-fill and counts.
    pub fn progress(&self) -> Progress {
        let mut contiguous_front = 0;
        for f in &self.fragments {
            if f.status == FragmentStatus::Complete {
                contiguous_front = f.index + 1;
            } else {
                break;
            }
        }

        let complete_count = self
            .fragments
            .iter()
            .filter(|f| f.status == FragmentStatus::Complete)
            .count();
        let pending_count = self
            .fragments
            .iter()
            .filter(|f| f.status == FragmentStatus::Pending)
            .count();
        let missing_count = self
            .fragments
            .iter()
            .filter(|f| f.status == FragmentStatus::Missing)
            .count();

        Progress {
            contiguous_front,
            complete_count,
            pending_count,
            missing_count,
        }
    }

    /// Whether this message has exceeded its TTL.
    pub fn is_expired(&self) -> bool {
        self.completed_at.is_none() && self.created_at.elapsed() > self.ttl
    }

    /// Whether all fragments are complete.
    pub fn is_complete(&self) -> bool {
        self.fragments.iter().all(|f| f.status == FragmentStatus::Complete)
    }

    /// Reassemble data in order (only if all fragments complete).
    pub fn reassemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut out = Vec::new();
        for f in &self.fragments {
            out.extend(f.data.as_ref().unwrap());
        }
        Some(out)
    }
}

/// The main fragment buffer managing multiple messages.
#[derive(Debug)]
pub struct FragmentBuffer {
    messages: HashMap<u64, MessageBuffer>,
    default_ttl: Duration,
    records: Vec<ReassemblyRecord>,
}

impl FragmentBuffer {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            messages: HashMap::new(),
            default_ttl,
            records: Vec::new(),
        }
    }

    /// Create a new message buffer for reassembly.
    pub fn create_message(&mut self, message_id: u64, total_fragments: usize) -> &MessageBuffer {
        self.create_message_with_ttl(message_id, total_fragments, self.default_ttl)
    }

    /// Create a new message buffer with a custom TTL.
    pub fn create_message_with_ttl(
        &mut self,
        message_id: u64,
        total_fragments: usize,
        ttl: Duration,
    ) -> &MessageBuffer {
        self.messages
            .entry(message_id)
            .or_insert_with(|| MessageBuffer::new(message_id, total_fragments, ttl));
        self.messages.get(&message_id).unwrap()
    }

    /// Mark a fragment complete for a given message.
    pub fn mark_fragment_complete(
        &mut self,
        message_id: u64,
        index: usize,
        data: Vec<u8>,
    ) -> Result<(), &'static str> {
        let msg = self
            .messages
            .get_mut(&message_id)
            .ok_or("message not found")?;
        msg.mark_complete(index, data)
    }

    /// Mark a fragment missing for a given message.
    pub fn mark_fragment_missing(
        &mut self,
        message_id: u64,
        index: usize,
    ) -> Result<(), &'static str> {
        let msg = self
            .messages
            .get_mut(&message_id)
            .ok_or("message not found")?;
        msg.mark_missing(index)
    }

    /// Get ternary status for a message.
    pub fn message_status(&self, message_id: u64) -> Option<FragmentStatus> {
        self.messages.get(&message_id).map(|m| m.ternary_status())
    }

    /// Get gap list for a message.
    pub fn gaps(&self, message_id: u64) -> Option<Vec<usize>> {
        self.messages.get(&message_id).map(|m| m.gaps())
    }

    /// Get progress for a message.
    pub fn progress(&self, message_id: u64) -> Option<Progress> {
        self.messages.get(&message_id).map(|m| m.progress())
    }

    /// Expire all messages past their TTL, recording stats.
    pub fn expire(&mut self) -> Vec<u64> {
        let expired_ids: Vec<u64> = self
            .messages
            .iter()
            .filter(|(_, m)| m.is_expired())
            .map(|(&id, _)| id)
            .collect();

        for id in &expired_ids {
            if let Some(msg) = self.messages.remove(id) {
                let p = msg.progress();
                self.records.push(ReassemblyRecord {
                    message_id: msg.message_id,
                    total_fragments: msg.total_fragments,
                    completed: false,
                    reassembly_time: msg.created_at.elapsed(),
                    fragments_completed: p.complete_count,
                });
            }
        }
        expired_ids
    }

    /// Finalize a completed message (remove from buffer, record stats).
    pub fn finalize(&mut self, message_id: u64) -> Option<Vec<u8>> {
        let msg = self.messages.get(&message_id)?;
        if !msg.is_complete() {
            return None;
        }
        let data = msg.reassemble()?;
        let reassembly_time = msg
            .completed_at
            .map(|t| t.duration_since(msg.created_at))
            .unwrap_or_else(|| msg.created_at.elapsed());
        let total_fragments = msg.total_fragments;
        self.records.push(ReassemblyRecord {
            message_id,
            total_fragments,
            completed: true,
            reassembly_time,
            fragments_completed: total_fragments,
        });
        self.messages.remove(&message_id);
        Some(data)
    }

    /// Compute aggregate statistics across all recorded messages.
    pub fn stats(&self) -> ReassemblyStats {
        let total = self.records.len();
        let completed = self.records.iter().filter(|r| r.completed).count();
        let expired = self.records.iter().filter(|r| !r.completed).count();
        let completion_rate = if total > 0 {
            completed as f64 / total as f64
        } else {
            0.0
        };
        let avg_time = if completed > 0 {
            let sum: Duration = self
                .records
                .iter()
                .filter(|r| r.completed)
                .map(|r| r.reassembly_time)
                .sum();
            sum / completed as u32
        } else {
            Duration::ZERO
        };
        ReassemblyStats {
            total_messages: total,
            completed_messages: completed,
            expired_messages: expired,
            completion_rate,
            average_reassembly_time: avg_time,
        }
    }

    /// Number of active messages in the buffer.
    pub fn active_count(&self) -> usize {
        self.messages.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_create_message_and_initial_status() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(1, 4);
        assert_eq!(buf.message_status(1), Some(FragmentStatus::Pending));
        let prog = buf.progress(1).unwrap();
        assert_eq!(prog.complete_count, 0);
        assert_eq!(prog.pending_count, 4);
        assert_eq!(prog.missing_count, 0);
    }

    #[test]
    fn test_complete_all_fragments() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(10, 3);
        buf.mark_fragment_complete(10, 0, vec![1]).unwrap();
        buf.mark_fragment_complete(10, 1, vec![2]).unwrap();
        buf.mark_fragment_complete(10, 2, vec![3]).unwrap();
        assert_eq!(buf.message_status(10), Some(FragmentStatus::Complete));
        let data = buf.finalize(10).unwrap();
        assert_eq!(data, vec![1, 2, 3]);
    }

    #[test]
    fn test_mark_missing_and_gaps() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(20, 5);
        buf.mark_fragment_complete(20, 0, vec![0xA]).unwrap();
        buf.mark_fragment_missing(20, 2).unwrap();
        buf.mark_fragment_missing(20, 4).unwrap();
        let gaps = buf.gaps(20).unwrap();
        assert_eq!(gaps, vec![2, 4]);
        assert_eq!(buf.message_status(20), Some(FragmentStatus::Missing));
    }

    #[test]
    fn test_forward_progress_contiguous() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(30, 6);
        buf.mark_fragment_complete(30, 0, vec![1]).unwrap();
        buf.mark_fragment_complete(30, 1, vec![2]).unwrap();
        buf.mark_fragment_complete(30, 2, vec![3]).unwrap();
        // index 3 is still pending, so contiguous breaks
        let prog = buf.progress(30).unwrap();
        assert_eq!(prog.contiguous_front, 3);
        assert_eq!(prog.complete_count, 3);
        assert_eq!(prog.pending_count, 3);
    }

    #[test]
    fn test_forward_progress_non_contiguous() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(31, 4);
        buf.mark_fragment_complete(31, 0, vec![1]).unwrap();
        buf.mark_fragment_complete(31, 3, vec![4]).unwrap(); // gap at 1,2
        let prog = buf.progress(31).unwrap();
        assert_eq!(prog.contiguous_front, 1); // only index 0 is contiguous
        assert_eq!(prog.complete_count, 2);
    }

    #[test]
    fn test_ttl_expiry() {
        let ttl = Duration::from_millis(50);
        let mut buf = FragmentBuffer::new(ttl);
        buf.create_message_with_ttl(40, 3, ttl);
        buf.mark_fragment_complete(40, 0, vec![1]).unwrap();
        thread::sleep(Duration::from_millis(80));
        let expired = buf.expire();
        assert_eq!(expired, vec![40]);
        assert!(buf.message_status(40).is_none());
        assert_eq!(buf.active_count(), 0);
    }

    #[test]
    fn test_statistics() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        // Complete one message.
        buf.create_message(50, 2);
        buf.mark_fragment_complete(50, 0, vec![1]).unwrap();
        buf.mark_fragment_complete(50, 1, vec![2]).unwrap();
        buf.finalize(50);
        // Expire one message.
        buf.create_message_with_ttl(51, 2, Duration::from_millis(10));
        buf.mark_fragment_complete(51, 0, vec![1]).unwrap();
        thread::sleep(Duration::from_millis(30));
        buf.expire();

        let stats = buf.stats();
        assert_eq!(stats.total_messages, 2);
        assert_eq!(stats.completed_messages, 1);
        assert_eq!(stats.expired_messages, 1);
        assert!((stats.completion_rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_out_of_range_fragment() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(60, 3);
        assert!(buf.mark_fragment_complete(60, 5, vec![1]).is_err());
        assert!(buf.mark_fragment_missing(60, 99).is_err());
    }

    #[test]
    fn test_partial_reassemble_returns_none() {
        let mut buf = FragmentBuffer::new(Duration::from_secs(60));
        buf.create_message(70, 3);
        buf.mark_fragment_complete(70, 0, vec![1]).unwrap();
        buf.mark_fragment_complete(70, 2, vec![3]).unwrap();
        // Not all complete; reassemble returns None
        let msg = buf.messages.get(&70).unwrap();
        assert!(msg.reassemble().is_none());
    }
}
