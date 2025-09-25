# Tar Writer Case Study

`std.tar.Writer` shows how format-specific logic composes with a generic writer while leaning on buffer primitives like `writeAll`, `sendFileAll`, and `splatByteAll`.

## Setup
The struct holds a pointer to an underlying `std.Io.Writer`, optional prefix, and cached `mtime` (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/tar/Writer.zig:17-33`).

## Header Emission
`writeHeader` builds a 512-byte header struct then writes it via `underlying_writer.writeAll` (`tar/Writer.zig:88-108`). Names that exceed the POSIX field limits trigger GNU long-name extension using `writeExtendedHeader`, which streams slices directly into the writer.

## Streaming File Content
- `writeFile` hands a `std.fs.File.Reader` to `sendFileAll` for efficient kernel-assisted copies (`tar/Writer.zig:41-58`).  
- `writeFileStream` reads exactly `size` bytes from an arbitrary reader, piping them through `reader.streamExact64` into the underlying writer.

## Padding As Scheduling
Tar blocks must align to 512 bytes. `writePadding` and `writePadding64` use `splatByteAll(0, n)` to queue zero bytes until the next block boundary (`tar/Writer.zig:139-150`). Padding is treated like any other chunk—buffered first, drained later.

## Finishing Up
`finishPedantically` writes two all-zero blocks if the caller wants strict compliance, relying again on `splatByteAll`. Because the buffer is exposed, callers could inspect `underlying_writer.buffered()` before finalizing.

## Links
- [[Exploring Zig IO Interfaces#4. Case Study Interludes]]
- [[Buffer Scheduling#Scheduling Moves]]
