# Buffer Scheduling

Buffers decide when bytes move. Zig’s IO interfaces surface that decision point instead of hiding it under decorators. This page maps Kelley’s scheduling rhetoric onto concrete stdlib mechanics.

## Memory Prisms
Stack vs heap vs buffer is a matter of who carved the slice and when. `std.Io.Writer.buffer` is caller-provided for `.fixed`, allocator-owned for `.allocating`, OS-backed for `fs.File.Writer` (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/Io/Writer.zig:122-145`, `fs/File.zig:1551-1660`). Regardless of origin, the interface treats it as a resource pool with a single invariant: don’t call `drain` unless you must.

## Scheduling Moves
- **Preserve & Rebase**: `Writer.rebase` insists on keeping the newest `preserve` bytes while asking the sink to flush older ones (`Writer.zig:80-118`). That’s equivalent to prioritising latency-sensitive data while giving throughput work the background timeslice.
- **Consume**: After a partial write, `Writer.consume` shifts remaining bytes to the front, resetting `end` (`Writer.zig:2294-2316`). Think of it as a scheduler updating a run queue when a task yields.
- **Limit Windows**: Both readers and writers operate with `std.Io.Limit`, which bounds how much work can be dispatched (`Writer.zig:7`, `Reader.zig:12`). Limits are like quantum sizes in time-sharing.

## Feedback Loops
`std.http.BodyWriter` selects different `drain` functions depending on transfer encoding, effectively swapping scheduling policies without changing the caller surface (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/http.zig:883-995`). Chunked mode batches headers, body, and trailers; content-length mode streams more steadily.

## Experiments
1. Wrap a `.fixed` writer around a tiny buffer and instrument calls to `drain`. Watch how often you “context switch”.
2. Swap to `.allocating` and observe how the queue never flushes because the buffer keeps growing—illustrating the danger of unbounded scheduling.
3. Add a logging decorator that records `w.buffered().len` over time to visualise back-pressure.

## Links
- [[Exploring Zig IO Interfaces#7. Scheduling Bytes]]
- [[Andrew Kelley Talk Commentary#2. Scheduling Is Inevitable (2:06 – 5:11)]]
- [[Stateful Interface Ontology#Scheduling Is Data]]
