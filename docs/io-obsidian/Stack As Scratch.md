# Stack As Scratch

Why did Andrew Kelley yoke stack memory to buffered I/O? Because both are forms of scratch space whose ownership tells you who assumes responsibility for pacing. This note reframes “stack” away from pure LIFO semantics and toward the idea of per-thread workbenches that double as buffers.

## 1. Stack Beyond LIFO
LIFO is a consequence of how we *manage* the stack, not what the memory *is*. A stack is a region pre-allocated so a thread can stage temporaries without negotiating with the global allocator. Once you view it as a sandboxed scratch space, the parallels to I/O buffers become clearer:

- Both are finite arenas attached to an execution context (thread, writer).  
- Both exist to decouple local computation speed from external services (allocator, kernel, network).  
- Both are typically zero-cost abstractions in Zig because the compiler can reason about lifetimes.

## 2. Zig’s Stack-Friendly Style
Zig encourages stack allocation even for large buffers (`var buf: [1024 * 1024]u8 = undefined;`), trusting developers to pick sizes that fit the thread’s stack. That buffer often feeds directly into `std.Io.Writer.fixed`, turning stack space into an I/O staging area.

Key observations:
- The same `[]u8` slice can serve as coroutine scratch for formatting *and* as the writer’s buffer.  
- Zig’s `thread.spawn` lets you choose stack size; if you heap-allocate the stack, the distinction between stack and heap collapses—the “stack” is simply a dedicated buffer whose discipline (grow/shrink) is enforced by calling convention.

## 3. Kelley’s Argument Revisited
When Kelley says “stack memory is arena-allocated heap memory,” he’s reminding us that stacks are buffers allocated in bulk with an agreed protocol. The segue to buffered I/O is intentional: once you accept a stack as a buffer for activation records, it’s easier to accept an exposed writer buffer as a legitimate API surface. Both share DNA:

| Scratch Space | Typical Usage                       | Ownership            | Release Mechanism            |
|---------------|--------------------------------------|----------------------|------------------------------|
| Thread stack  | locals, temporaries, coroutine state | Thread runtime/user  | LIFO unwinding or discard    |
| Writer buffer | staged output bytes                  | Writer implementer   | `flush`/`drain`/`consume`    |
| Arena         | batch allocations                    | Arena owner          | Manual reset or bulk free    |

Kelley’s message: don’t mystify stacks; they’re buffers with rituals. Likewise, Zig demystifies I/O buffers by pushing them into user reach.

## 4. Stack-As-Buffer Patterns In Zig
- **Formatting Pipelines**: call `writer.writableSlice` to grab space inside the writer’s buffer, which might itself sit on the stack. You edit the slice, then `advance` commits it (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/Io/Writer.zig:195-224`).  
- **Thread-Local Scratch**: spawn a worker thread with a heap-allocated stack big enough for heavy parsing; inside, allocate `var read_buf: [4 * 1024 * 1024]u8`. The stack is acting as a private arena.  
- **Coroutine Frames**: Zig can allocate coroutine frames on the stack when lifetime allows, turning the call stack into a temporary buffer for async state.

## 5. Making The Click
Buffered I/O and stack management are both strategies for confronting mismatch:
- CPU vs memory latency (stack hides allocator latency).  
- Application vs kernel/network throughput (buffer hides syscall latency).  
Seen through that lens, Kelley’s talk is about the ethics of taking responsibility. Using the stack responsibly means understanding how much scratch space you’re consuming on behalf of a thread. Using an exposed writer buffer responsibly means understanding when to flush, how much to preserve, and when to rebase.

Once you admit stacks are just buffers with LIFO etiquette, it feels less jarring that Zig hands you writer buffers. Both peel back a layer of abstraction so the programmer can make pacing decisions appropriate to their domain.

## 6. Practical Implications
- **Sizing**: Picking stack size and picking buffer size follow the same logic—estimate peak usage, add headroom, monitor in production.  
- **Back-pressure**: Stack overflow is back-pressure from excessive locals; writer overflow is back-pressure from slow sinks. In both cases, the fix might be to externalise the workload (heap allocate, flush more often).  
- **Observability**: Stack traces reveal how frames use scratch space; buffer instrumentation shows flush cadence. Both tools surface scheduling bugs.

## 7. Further Reading
- [[Buffer Ontology#Ownership Stories]]
- [[Buffer Scheduling#Memory Prisms]]
- [[Andrew Kelley Talk Commentary#1. Memory As Managed Commons (0:36 – 2:06)]]
- Zig language reference on stack/doc comments around `@asyncCall` and manual stack sizing.
