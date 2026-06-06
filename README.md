# ternary-reassembly

Message reassembly for GPU cluster communication with ternary fragment status. Gap detection, partial reassembly, TTL expiry.

## Why This Matters

# ternary-reassembly
Message/packet reassembly for GPU cluster communication where fragment status
is ternary: `+1` (complete), `0` (pending), `-1` (missing).
Features: fragment buffering with timeout tracking, ternary completion status,
gap detection, partial reassembly with forward progress, TTL-based expiry,

## The Five-Layer Stack

This crate is part of the **Oxide Stack** — a distributed GPU runtime built on five layers:

```
┌─────────────────┐
│  cudaclaw        │  Persistent GPU kernels, warp consensus, SmartCRDT
├─────────────────┤
│  cuda-oxide      │  Flux → MIR → Pliron → NVVM → PTX compiler
├─────────────────┤
│  flux-core       │  Bytecode VM + A2A agent protocol
├─────────────────┤
│  pincher         │  "Vector DB as runtime, LLM as compiler"
├─────────────────┤
│  open-parallel   │  Async runtime (tokio fork)
└─────────────────┘
```

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Design

Every value in this crate follows **ternary algebra** (Z₃):

| Value | Meaning | GPU Analog |
|-------|---------|------------|
| +1 | Positive / Active / Healthy | Warp vote yes |
| 0 | Neutral / Pending / Balanced | Warp vote abstain |
| -1 | Negative / Failed / Overloaded | Warp vote no |

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary LLMs at 60% less power
2. **GPU warp voting** — hardware ballot returns ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity

## Key Types

```rust
pub enum FragmentStatus
pub struct Fragment
pub struct Progress
pub struct ReassemblyRecord
pub struct ReassemblyStats
pub struct MessageBuffer
pub fn new
pub fn mark_complete
pub fn mark_missing
pub fn ternary_status
pub fn gaps
pub fn pending_indices
```

## Usage

```toml
[dependencies]
ternary-reassembly = "0.1.0"
```

```rust
use ternary_reassembly::*;
// See src/lib.rs tests for complete working examples
```

## Testing

```bash
git clone https://github.com/SuperInstance/ternary-reassembly.git
cd ternary-reassembly
cargo test    # 9 tests
```

## Stats

| Metric | Value |
|--------|-------|
| Tests | 9 |
| Lines of Rust | 491 |
| Public API | 29 items |

## License

Apache-2.0
