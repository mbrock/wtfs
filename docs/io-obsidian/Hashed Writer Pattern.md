# Hashed Writer Pattern

`std.Io.Writer.hashed` wraps an existing writer and increments a hasher while bytes flow through. It exemplifies “side-channel transformations” made possible by exposing the buffer.

## Construction
`Writer.hashed` takes an existing writer pointer, a hasher, and a staging buffer (`/usr/local/zig-x86_64-linux-0.15.1/lib/std/Io/Writer.zig:134-135`). The returned struct embeds the original writer interface plus bookkeeping for the hash state.

## Flow
- Calls to `write` first add bytes to the local buffer; when drained, the wrapper updates the hasher then forwards slices to the underlying writer.  
- `flush` ensures both the hash and downstream sink see identical data.

## Use Cases
- Checksumming large archive streams without storing data twice (pair with [[Tar Writer Case Study]]).  
- Content-addressable storage: compute digest while writing to disk or network.  
- Debugging: instrument the hash to detect accidental mutations between logical and physical layers.

## Relation To Capability Model
Because Zig’s writer is a capability, the hashed wrapper composes by delegation rather than inheritance. It keeps the buffer semantics consistent while layering new side effects. See [[Stateful Interface Ontology#Capability View]].

## Further Reading
- [[Exploring Zig IO Interfaces#4. Case Study Interludes]]
- `std.crypto` hash implementations for pluggable hashers.
