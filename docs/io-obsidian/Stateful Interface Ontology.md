# Stateful Interface Ontology

Zig calls `std.Io.Writer` and `std.Io.Reader` “interfaces,” yet they store buffers, indices, and even default methods. This page unpacks why that naming works—and why it unsettles readers used to behavior-only interfaces.

## Interface As Object
`Writer` is a struct with three visible fields and a vtable pointer (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/Io/Writer.zig:12-16`). The struct *is* the interface value; implementations embed it (`std.fs.File.Writer.interface`, `fs/File.zig:1551-1660`). Users interact with the struct directly, no trait object wrapper required.

## Capability View
Treat a writer as a capability token: possession grants permission to flush bytes into a sink plus mutate the staging buffer. Capabilities often bundle authority with limited state. Zig formalises this by letting implementers replace `drain`, `sendFile`, `flush`, `rebase` while sharing the same buffer surface. See [[Exploring Zig IO Interfaces#6. Interface As Capability]].

## Scheduling Is Data
Because buffer indices live on the interface, scheduling decisions—what stays, what flushes—become part of the data model. `Writer.rebase` and `Reader.rebase` mutate the struct to enact those decisions (`Writer.zig:80-118`, `Reader.zig:1235-1251`). Reference [[Buffer Scheduling#Scheduling Moves]] for the operational consequences.

## Language Comparisons
- Rust’s `Write` trait keeps state in separate structs like `BufWriter`; the interface value is a fat pointer to methods only.  
- Go’s `io.Writer` is behaviour-only; `bufio.Writer` wraps it with hidden state.  
- C++ `std::streambuf` quietly mirrors Zig: a vtable plus mutable buffer pointers. Zig simply refuses to hide the streambuf behind `ostream`.

For deeper contrasts, jump to [[Comparative Interface Patterns]].

## Naming Alternatives
If “interface” feels overloaded, experiment with “portal” or “stateful interface struct.” The important property is that users write generic code against the struct’s functions while optionally manipulating its exposed buffer. That hybrid is exactly what Kelley advocates when railing against function colouring: one value, many runtimes. See [[Andrew Kelley Talk Commentary#3. Concurrency Models and Function Colouring (5:11 – 9:08)]].
