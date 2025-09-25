# Comparative Interface Patterns

A cheat sheet contrasting Zig’s reader/writer surfaces with analogous abstractions in other ecosystems.

## Rust
- `std::io::Write` trait defines `write`, `flush`, `write_vectored`; state resides in adapters like `BufWriter`.  
- Rust’s `Write` trait object is a fat pointer (data + vtable). Unlike Zig, there’s no public buffer slot to manipulate.  
- Short-write semantics resemble Zig’s `writeVec`, but buffering strategies require wrappers.

## Go
- `io.Writer` is a single method interface; `bufio.Writer` adds buffering privately.  
- Flush semantics rely on `Flush()`; there is no equivalent of `Writer.rebase` because the buffer is not caller-visible.  
- Composition occurs via embedding, similar to `std.fs.File.Writer`, but still hides internal slices.

## C++
- `std::streambuf` mixes virtual methods with pointer members (`eback`, `gptr`, `pptr`), making it the closest analogue.  
- `std::ostream` wraps `streambuf`, adding formatting sugar. Zig essentially hands you the `streambuf` without the wrapper, encouraging direct manipulation.

## Java & JVM
- `BufferedOutputStream` stores bytes then flushes on demand; the base `OutputStream` exposes only methods.  
- Decorator pattern mirrors Zig’s `Writer` vtable swapping, but the buffer stays private.  
- Flushing rules (`flush`, `close`) align with Zig’s contract yet rely on subclass overrides.

## JavaScript Streams API
- Writable streams mediated by controllers: `desiredSize`, `write`, `close`, `abort`.  
- Back-pressure surfaces via promises and queue sizes, conceptually similar to Zig’s explicit `Limit`.  
- Bring-your-own-buffer (BYOB) readers echo Zig’s `.fixed` readers where the consumer donates storage.

## Takeaways
- Zig collapses interface and adapter into one struct, maximising introspection.  
- Most other ecosystems separate behaviour (trait/interface) from buffering (wrapper/decorator).  
- C++ proves the idea isn’t alien; Zig simply institutionalises the transparent version.  
- Back-pressure semantics show up everywhere, whether through queue sizes, flush methods, or explicit limits—supporting the thesis that buffering is scheduling.

Links: [[Exploring Zig IO Interfaces#5. Comparative Field Notes]], [[Stateful Interface Ontology#Interface As Object]].
