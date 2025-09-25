# Buffer Ontology

What is a buffer? What is a stack? The question sounds elementary until you realise both words name agreements rather than physical entities. This note approaches them as ontological constructs—ways of organising obligations between code and the environment—before looping back to Zig’s IO machinery.

## 1. Names For Guarantees
A *buffer* is a promise that data may be staged between producer and consumer. A *stack* is a promise that call/return order will resemble a vertical pile. In both cases the memory involved is just bytes; what changes is the protocol layered on top.

- Buffer protocol: “You may write here until I say stop; you must not assume anything about when the data leaves.”
- Stack protocol: “You may allocate here by moving the frontier; you must release in reverse order.”

Both protocols exist to make scheduling tolerable. Buffers schedule *bytes*; stacks schedule *stack frames* (or more broadly, lifetimes).

## 2. Physical Substrate vs Social Contract
On a modern OS the call stack is allocated from the heap: the kernel reserves a region, lazily faulted in as you descend. Likewise, a Zig writer’s `buffer` might live on the stack, heap, static data, or shared memory. The substrate is fungible; the “stackness” or “bufferness” comes from the agreed-upon manipulations.

| Property                  | Buffer                                      | Stack                                      |
|---------------------------|----------------------------------------------|--------------------------------------------|
| Primary axis              | Producer/consumer time                       | Call/return nesting                        |
| Critical invariants       | Capacity, order of flush                     | LIFO discipline, frame layout              |
| Failure mode              | Overflow/back-pressure                       | Overflow/underflow                         |
| Scheduling metaphor       | Queue with service guarantees                | Runway slices for activation records       |
| Visibility in Zig         | `writer.buffer`, `writer.end`                | `@frame()` internals, coroutine resumption |

## 3. Ownership Stories
- **Stack**: Owned by the current execution context, borrowed by callees with the expectation of immediate return. Arena allocators mimic this by letting you "rewind" to a marker. Kelley’s remark that “stack memory is arena-allocated heap memory” captures this: it’s just a specialised allocator with deterministic reclamation.
- **Buffer**: Owned by the interface that stages data (writer/reader), lent to higher-level formatters. Zig’s `.fixed` writer borrows caller-provided space; `.allocating` owns it outright.

In both cases, the owner delegates temporarily but retains responsibility for lifetime. Stack frames pop; buffers drain.

## 4. Time And Reversibility
Stacks impose a *reversible* ordering: actions unwind exactly. Buffers can be irreversible; once flushed, bytes are gone. Yet in both cases, time is central:

- Stack growth corresponds to temporal progression of calls; unwinding rewinds time logically.  
- Buffer filling corresponds to time spent staging; draining advances the external clock (sending data to the sink).

Zig acknowledges the temporal nature by exposing `Writer.end` (current “now” of the buffer) and by letting coroutines snapshot their call stacks.

## 5. Schedulers Wearing Different Hats
Kelley’s talk reframes buffers and stacks as scheduling artifacts:

- Thread schedulers decide *which* stack gets CPU time.  
- Buffer flush logic decides *when* bytes get I/O bandwidth.  
- `Writer.rebase` is analogous to stack unwinding: you keep a preserved tail (like a partial frame) while making room for new arrivals.

Viewed this way, the question “what is a buffer?” becomes “what scheduling problem is this code solving?” For stacks: deterministic storage reclamation. For buffers: decoupling producer/consumer rates.

## 6. Agency And Responsibility
Buffers and stacks both transfer agency from environment to program:

- Without a stack, the OS would have to manage temporary storage for every call. The program couldn’t reason locally about lifetimes.  
- Without buffers, every subsystem would issue syscalls for every byte, and the OS/network would shoulder all pacing decisions.

Zig’s design emphasises shared agency: you hold the buffer; you decide when to flush. Likewise, coroutines expose frame management to the language/runtime rather than the OS.

## 7. Implications For API Design
- APIs should document the contract, not the substrate. Saying “accepts any writer” really means “accepts any capability that honours the buffer protocol.”  
- Providing `writableSlice` is akin to giving direct stack access: powerful yet risky without discipline.  
- Error handling mirrors stack unwinding: partial flushes behave like exceptions unwinding frames. `Writer.consume` shifts data similar to how unwinding discards frames while preserving live values.

## 8. Open Questions
- Can we treat asynchronous stacks (coroutine frames) and network buffers as instances of a generic “suspension store”?  
- What observability tools help humans see these protocols in action (stack traces; buffer occupancy timelines)?  
- How do we encode buffer/stack contracts in type systems without smothering ergonomics?

## Links
- [[Andrew Kelley Talk Commentary#1. Memory As Managed Commons (0:36 – 2:06)]]
- [[Buffer Scheduling#Memory Prisms]]
- [[Stateful Interface Ontology#Capability View]]
- [[Exploring Zig IO Interfaces#2. Buffering As Ontological Problem]]
