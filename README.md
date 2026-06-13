# Ternary Reassembly

Message fragment reassembly for GPU cluster communication, where each fragment's status is tracked on a ternary scale: `+1 (complete)`, `0 (pending)`, or `-1 (missing)`. Provides gap detection, forward-progress tracking, TTL-based expiry, and aggregate reassembly statistics.

## Why It Matters

In distributed GPU clusters, large messages (model weights, activation tensors, gradient updates) are split into fragments and sent across multiple network paths. Fragments arrive out of order, some are dropped, and the receiver must reassemble the original message — or declare it failed after a timeout.

The ternary status model maps naturally to this problem:

- **`+1` Complete**: fragment received, verified, buffered
- **`0` Pending**: fragment not yet seen, still within TTL
- **`-1` Missing**: fragment confirmed lost (negative ACK or timeout)

This is more expressive than binary (received/not-received) because it distinguishes "still waiting" from "definitely lost" — enabling proactive retransmission requests instead of passive timeout.

## How It Works

### Ternary Fragment Status

Each fragment $f_i$ in a message of $N$ fragments has status:

$$f_i.\text{status} = \begin{cases} +1 & \text{data received and verified} \\ 0 & \text{no signal yet (within TTL)} \\ -1 & \text{negative ACK or sub-fragment timeout} \end{cases}$$

The **message-level status** aggregates these:

$$\text{message\_status} = \begin{cases} +1 & \text{if } \forall i: f_i = +1 \\ -1 & \text{if } \exists i: f_i = -1 \\ 0 & \text{otherwise} \end{cases}$$

### Forward-Progress Tracking

The `progress()` method computes the **contiguous front-fill** — the largest prefix $[0, k)$ where all fragments are complete:

$$k = \min\{i : f_i.\text{status} \neq +1\}$$

This enables streaming reassembly: you can begin processing prefix-complete data before the entire message arrives, as long as processing is sequential.

### Gap Detection

Missing fragments are returned as a sorted index list:

$$\text{gaps} = \{i : f_i.\text{status} = -1\}$$

The gap list drives selective retransmission requests (NACK-based protocols), which are far more efficient than full retransmission.

### TTL Expiry

Each `MessageBuffer` has a time-to-live. When `created_at.elapsed() > ttl` and the message is incomplete, it expires:

- Fragment data is discarded
- A `ReassemblyRecord` is logged with partial completion stats
- Aggregate statistics are updated

### Reassembly Statistics

Across all messages, the buffer tracks:

$$\text{completion\_rate} = \frac{\text{completed}}{\text{completed} + \text{expired}}$$

$$\bar{T}_{\text{reassembly}} = \frac{1}{|\text{completed}|}\sum_{m \in \text{completed}} t_m$$

### Complexity

| Operation | Time | Space |
|---|---|---|
| `create_message` | $O(1)$ amortized (HashMap insert) | $O(N)$ for $N$ fragments |
| `mark_fragment_complete` | $O(N)$ (full completion check) | $O(1)$ |
| `mark_fragment_missing` | $O(1)$ | $O(1)$ |
| `gaps` | $O(N)$ scan | $O(k)$ for $k$ gaps |
| `progress` | $O(N)$ scan | $O(1)$ |
| `expire` | $O(M)$ for $M$ active messages | — |
| `finalize` | $O(N)$ copy | $O(N)$ output |
| `stats` | $O(R)$ for $R$ records | $O(1)$ |

## Quick Start

```rust
use std::time::Duration;
use ternary_reassembly::{FragmentBuffer, FragmentStatus};

let mut buf = FragmentBuffer::new(Duration::from_secs(60));

// Start reassembling a 4-fragment message
buf.create_message(42, 4);
assert_eq!(buf.message_status(42), Some(FragmentStatus::Pending));

// Fragments arrive out of order
buf.mark_fragment_complete(42, 0, vec![0xDE]).unwrap();
buf.mark_fragment_complete(42, 2, vec![0xAD]).unwrap();
buf.mark_fragment_missing(42, 1).unwrap();

// Check progress
let prog = buf.progress(42).unwrap();
assert_eq!(prog.contiguous_front, 1);  // Only fragment 0 is contiguous
assert_eq!(prog.complete_count, 2);
assert_eq!(prog.missing_count, 1);

// Retransmit the missing fragment
buf.mark_fragment_complete(42, 1, vec![0xBE]).unwrap();
buf.mark_fragment_complete(42, 3, vec![0xEF]).unwrap();

// Finalize and get the full data
let data = buf.finalize(42).unwrap();
assert_eq!(data, vec![0xDE, 0xBE, 0xAD, 0xEF]);
```

## API

### `FragmentBuffer`
The main manager. Methods: `new(ttl)`, `create_message(id, n)`, `create_message_with_ttl(id, n, ttl)`, `mark_fragment_complete(id, idx, data)`, `mark_fragment_missing(id, idx)`, `message_status(id)`, `gaps(id)`, `progress(id)`, `expire()`, `finalize(id)`, `stats()`, `active_count()`.

### `MessageBuffer`
Per-message reassembly state. Fields: `message_id`, `total_fragments`, `fragments: Vec<Fragment>`, `created_at`, `completed_at`, `ttl`.

### `Fragment`
Fields: `index`, `status: FragmentStatus`, `data: Option<Vec<u8>>`, `last_updated: Instant`.

### `Progress`
Fields: `contiguous_front`, `complete_count`, `pending_count`, `missing_count`.

### `ReassemblyStats`
Fields: `total_messages`, `completed_messages`, `expired_messages`, `completion_rate`, `average_reassembly_time`.

## Architecture Notes

Within the **γ + η = C** framework:

- **γ (gamma)** — the ternary fragment status: the *agent signal* from the network (received/pending/lost)
- **η (eta)** — the TTL and forward-progress tracking: the *environment response* that decides when to give up vs. keep waiting
- **C** — **communication reliability**: when γ and η are tuned together (appropriate TTL for network conditions), the system achieves high completion rates with low latency

The crate uses only `std::collections::HashMap` and `std::time` — zero external dependencies, suitable for `no_std`-adjacent environments.

## References

1. Postel, J. (1980). *RFC 768: User Datagram Protocol*. — Fragmentation and reassembly fundamentals.
2. Kent, C. A., & Mogul, J. C. (1987). "Fragmentation Considered Harmful." *SIGCOMM*. — Why fragment-level tracking matters.
3. Mathis, M., et al. (1997). "The Macroscopic Behavior of the TCP Congestion Avoidance Algorithm." *SIGCOMM*. — Loss detection and timeout analysis.
4. NVIDIA Corporation. (2024). *NCCL: NVIDIA Collective Communications Library Documentation*. — GPU cluster messaging and fragment handling.

## License

MIT
