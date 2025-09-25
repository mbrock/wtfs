# Thread Spawn and Stacks

Zig’s `std.Thread.spawn` exposes stack sizing the same way `std.Io.Writer` exposes buffering. Digging into the Linux implementation makes the parallel unmistakable: spawning a thread is literally building a custom buffer arena for that thread’s scratch space.

## 1. SpawnConfig And Stack Size
`std.Thread.spawn(.{ .stack_size = … }, f, args)` lets the caller choose how much scratch space the new thread receives. If you omit a size, Zig picks a default tuned per platform; but the API nudges you to think about it. Just as you pick a buffer length for a writer, you pick a stack length for a thread.

On WASI, an allocator is required to obtain the stack; on POSIX the runtime maps pages manually. This difference underscores that “stack allocation” is policy-dependent—sometimes it’s backed by POSIX `mmap`, sometimes by user-provided heap space.

## 2. Anatomy Of The Linux Implementation
Key steps from `std.Thread.spawn` (Linux flavour) in `/usr/local/zig-x86_64-linux-0.15.1/lib/std/Thread.zig`:

1. **Compute Layout**: Align guard page, stack pages, TLS area, and an `Instance` struct (`Instance` holds function args and completion state).
2. **Map Memory**: Use `mmap` with `PROT.NONE` to reserve the region; then `mprotect` to make everything except the guard writable. This is pre-allocation of a buffer—exactly what we do when handing `Writer.fixed` a slice.
3. **Prepare TLS**: Carve out a chunk for thread-local storage (`linux.tls.prepareArea`). More buffer choreography.
4. **Place Instance**: Copy args into the reserved `Instance` struct at the top of the mapping. When the thread starts, it reads from this mini control block.
5. **Call clone**: `linux.clone` receives the stack pointer (`&mapped[stack_offset]`). When the new thread runs, that pointer becomes its scratch space.

## 3. Stack As Configurable Buffer
The call to `linux.clone` passes `stack_ptr` computed from the reserved mapping. If you picked `stack_size = 4 * 1024 * 1024`, you literally set the capacity of the thread’s staging buffer. The guard page is a sentinel just like buffer capacity checks: overflow trips a fault, akin to `Writer` returning `error.WriteFailed` when you exceed available space.

Because the mapping is private to the thread, you can safely allocate large temporary slices on the stack—precisely the pattern Zig encourages (`var buf: [N]u8 = undefined;`). Combine that with `.fixed` writers and the stack becomes your I/O buffer by default.

## 4. Detaching And Cleanup
The `Instance` struct stores a `ThreadCompletion` that knows how to free the mapped region if the thread is detached. Again, ownership mirrors buffer semantics: whoever owns the stack buffer must release it. In the writer world, `Writer.Allocating.deinit` frees its arena; here `ThreadCompletion.freeAndExit` unmaps the stack.

## 5. Why Kelley Talks About Both
Stack sizing and buffer sizing are the same responsibility at different layers:

- Choose too small a stack → overflow, crash, or forced migration to heap.  
- Choose too small a buffer → constant drains, syscall overhead, throughput collapse.  
- Choose too large a stack → waste address space or commit more pages than necessary.  
- Choose too large a buffer → hoard memory, increase latency before flushing.

Kelley’s thesis is that programmers can’t hide from these trade-offs. Zig reflects that by making both dimensions explicit in APIs (`spawn` and `Writer` constructors). The Linux spam of guard pages, TLS alignment, and clones is the concrete manifestation of “stack as buffer” rhetoric.

## 6. Takeaways
- A thread’s stack is literally a mapped buffer with guard, alignment, and control blocks.  
- Zig’s spawn API gives you direct control over that buffer the same way `Writer.fixed` gives you control over I/O staging.  
- Understanding the implementation makes it feel natural to treat buffers and stacks as siblings in the broader “scratch space” family.

Links: [[Stack As Scratch]], [[Buffer Ontology#Ownership Stories]], [[Buffer Scheduling#Memory Prisms]].
