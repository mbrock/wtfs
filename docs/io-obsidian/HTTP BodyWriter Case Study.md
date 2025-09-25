# HTTP BodyWriter Case Study

The HTTP server/client modules in Zig wrap `std.Io.Writer` to orchestrate different transfer encodings. This note looks at how the vtable swaps express policy.

## State Layout
`std.http.BodyWriter` embeds a `writer: std.Io.Writer` inside its state machine (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/http.zig:739-959`). The enclosing struct tracks headers, transfer-encoding flags, and whether the body can be elided.

## Vtable Variants
Depending on headers, the code selects among multiple drain/sendFile pairs:
- **Chunked**: `chunkedDrain` wraps payloads in size-prefixed chunks and writes trailers on `end` (`http.zig:883-959`).
- **Content-Length**: `contentLengthDrain` streams exact byte counts, asserting the user doesn’t exceed `content_length`.
- **Eliding**: `elidingDrain` discards data until late binding determines whether a body is necessary.

Each variant still respects the Writer contract: drain only runs when the buffer is full or during flush/end. The difference lies in how `buffered()` is serialized.

## Flush Rituals
- `flush` pushes buffered bytes through the current drain.  
- `end` optionally appends trailers and final CRLF in chunked mode.  
- `endUnflushed` skips the extra flush for callers who guarantee the buffer is empty.
These functions demonstrate how higher-level protocols ritualise flush semantics without introducing new interface types.

## Lessons For Buffer Design
1. Policy swaps can stay ergonomic when implemented as vtable replacements.  
2. Exposed buffers let protocols stage headers alongside body bytes before a single drain.  
3. Eliding is just “don’t schedule the bytes yet”—a direct tie-in to [[Buffer Scheduling]].

## Related Notes
- [[Exploring Zig IO Interfaces#4. Case Study Interludes]]
- [[Buffer Scheduling#Feedback Loops]]
