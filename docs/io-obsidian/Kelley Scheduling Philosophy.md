# Kelley’s Scheduling Philosophy

Andrew Kelley’s “Don’t Forget to Flush” talk is not merely a performance about buffers; it is a philosophy lecture disguised as systems engineering. The recurring motifs—stacks as heap slices, schedulers as necessary evils, buffered I/O as civic duty—invite us to reread common primitives as moral choices about who bears delay, risk, and responsibility. This essay teases out that worldview, tracing its lineage, its practical incarnations in Zig, and its consequences for how we reason about software.

## 1. Scheduling As Ontology
In Kelley’s framing, scheduling is not an implementation detail but the ontological substrate of computation. Whether we are queuing threads, parceling out bytes, or sequencing AST nodes, we are always deciding *what happens next*. Pretending otherwise is an abdication.

He mocks the idea that single-threaded blocking code “escapes” scheduling. In reality, the compiler precomputes an immutable schedule—you chose the order at compile time. In thread-per-request systems you outsource the queue to the kernel, and when `make -j` melts your laptop you learn the cost of outsourcing without oversight. In async runtimes, you buy flexibility at the price of explicit yields.

The thesis is that every abstraction hides a queue; every queue demands a steward. When we forget that, we call it “automatic” and celebrate ignorance. Kelley urges the opposite: embrace the queue, name it, own it.

## 2. Stacks, Buffers, Arenas: The Same Covenant
To support the ontological claim, Kelley collapses categories that engineers often keep distinct:

- **Stacks**: Pre-allocated arenas with LIFO etiquette. They exist so the kernel does not need to mediate every local variable request.  
- **Buffers**: Pre-allocated arenas with producer/consumer etiquette. They exist so the kernel does not receive syscalls for every byte.  
- **Arenas**: Bulk allocations with domain-specific reclamation rules, chosen so the general-purpose allocator does not bear niche workloads.

Each device is a covenant: “I will take care of the details,” says the program, “if you, environment, let me work without constant permission slips.” Stacks and buffers both represent a shift of scheduling power from the environment to the application. Once recognized, the merger demystifies Zig’s exposed writer buffer and the thread stack knob: both are forms of assuming custody over time and space.

## 3. Why Buffering Is Ethics
Buffering mediates between mismatched paces. Producers sprint; consumers lag. Kelley insists the mediator—the buffer owner—must decide the policy: flush early for low latency, hoard for throughput, drop or back-pressure when flooded. These are ethical decisions because they distribute pain. Do we burden the sender with back-pressure, or do we let queues swell and risk collapse elsewhere? Ignoring the question just passes it to the kernel, which is too general to make context-sensitive choices.

From this perspective, `std.Io.Writer.drain`’s preconditions are moral imperatives. Do not call `drain` until you must, because doing so reneges on the promise to keep cheap operations local. Do not rely on hidden flushes; call `flush` when you have upheld your end of the bargain. The code reads like a civil contract because Kelley views buffering through the lens of civic responsibility.

## 4. Scheduling Across Scales
Kelley’s examples telescope: stack frames, thread pools, async tasks, pipeline stages. The moral remains constant: at each scale, someone must choose the order and timing. The Zoomed-in details differ:

- **Stack Frames**: Scheduling of memory reclamation. LIFO discipline yields simplicity; defying it (setjmp/longjmp, coroutines) requires new bookkeeping.  
- **Thread Pools**: Scheduling of CPU slices. The application knows domain priorities; the kernel does not.  
- **Event Loops**: Scheduling of readiness-driven tasks. Properly structuring tasks and yields avoids starvation.  
- **Buffered Writers**: Scheduling of I/O syscalls. Choosing buffer sizes and flush points balances latency and throughput.

Rather than hierarchies, Kelley sees fractals: the same concerns repeating at different magnitudes. That explains why the talk glides from stacks to I/O to coroutines without apology—they are manifestations of a deeper pattern.

## 5. Zig As Philosophy Engine
Zig’s standard library encodes Kelley’s worldview:

- **Explicit Buffers**: `std.Io.Writer` and `.Reader` expose their slices. Users call `writableSlice` and `advance`, accepting stewardship of staging space.  
- **Configurable Stacks**: `std.Thread.spawn` takes `stack_size` and (on some targets) an allocator, forcing the user to confront the resource cost of concurrency.  
- **First-Class Back-Pressure**: `std.io.Limit`, `Writer.rebase`, `Reader.discard`, and `BodyWriter` policies all formalize the contract between producers and consumers.

Nothing stops you from writing high-level helpers, but the foundations stay transparent. Zig codifies Kelley’s plea: “you are responsible; the system will not hide the queue from you.”

## 6. Contrasts With Other Traditions
Many ecosystems treat scheduling as incidental, and their abstractions reflect that:

- Java’s buffered streams hide the buffer, implying the runtime will flush “at the right time.”  
- Rust’s `BufWriter` reduces scheduling choices to `flush` vs auto-flush, with no access to indices or in-flight data.  
- Go’s goroutines spawn with fixed stack sizes that grow dynamically; developers seldom confront stack budgets until panic or profiling.  
- JavaScript’s async/await and event loop present a single-threaded mental model where concurrency is “handled.”

These designs prioritize ease, sometimes at the cost of awareness. Kelley’s philosophy would call them convenient illusions. Zig’s contrary posture is to surface the machinery and demand literacy.

## 7. The emotional argument
There is anger in the talk—a frustration with systems that pretend scheduling away while setting traps for anyone who digs deeper. Kelley’s humor (“raise your hand if make -j never crashed your system”) highlights collective denial. By aligning stacks and buffers, he expands the scope of culpability: “if you allocate a megabyte on the stack, you are affecting scheduler choices.”

The emotional core is dignity: programmers deserve to understand what their code makes others do. Buffering and scheduling become acts of care rather than chores.

## 8. Practical Guidance
Taking Kelley seriously suggests habits:

1. **Name Your Queues**: Whenever you create a buffer (channel, stack, ring), document who owns it and when it drains.  
2. **Expose Switches**: Provide knobs for buffer sizes and flush cadence; do not hardcode trade-offs unless you own the domain.  
3. **Observe Pressure**: Instrument buffer occupancy and stack usage; treat thresholds as signals to renegotiate policy.  
4. **Respect Contracts**: If an API expects you to delay drains, honor that bargain. If you must violate it, document why and what backup policy you supply.  
5. **Teach The Pattern**: When mentoring, present stacks and buffers as related tools in the “scratch space” family so new developers internalize the scheduling lens early.

## 9. Where The Philosophy Leads
Kelley’s worldview invites further exploration:

- Could type systems express scheduling contracts (e.g., static analysis ensuring drains only occur when buffers full)?  
- How might observability platforms display scheduler hierarchies, from stack usage to network buffers?  
- Can we design APIs where real-time workloads articulate their latency/throughput trade-offs explicitly, handing the runtime richer context?

Ultimately, Kelley advocates for agency. Buffering, stack management, thread orchestration—all manifest a simple creed: you are the steward of your program’s schedule. Forgetting to flush is not just a bug; it is a failure to uphold the covenant you willingly entered when you took control of the scratch space in the first place.

Links: [[Andrew Kelley Talk Commentary]], [[Stack As Scratch]], [[Buffer Ontology]], [[Buffer Scheduling]], [[Thread Spawn and Stacks]].
