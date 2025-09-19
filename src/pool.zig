const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMapUnmanaged = std.HashMapUnmanaged;
const StringIndexContext = std.hash_map.StringIndexContext;
const StringIndexAdapter = std.hash_map.StringIndexAdapter;

/// Andrew Kelley's incredible void map trick.
///
/// https://zig.news/andrewrk/how-to-use-hash-map-contexts-to-save-memory-when-doing-a-string-table-3l33
///
/// Instead of a HashMap([]u8, u32), we use HashMap(u32, void),
/// and use an external context to store the actual string data.
/// The u32 is simply the byte offset into that string data.
///
/// A slice is 16 bytes on 64-bit systems.
/// So a K+V entry is 20 bytes for a slice-to-index map.
/// Instead, we use a 4-byte index-to-nothing map!
///
/// The u32 key is simply the byte offset into that array where
/// the string starts.  Zero bytes are used as sentinels to mark
/// the end of each string.
///
/// To intern a string, we first append it to the byte array
/// Then we try to insert the slice into the map, with an
/// adapter & context that do hashing and equality comparisons
/// via the byte array.  If the string was already present,
/// we roll back the append and return the existing index.
/// If it was not present, we store the new index in the slot.
pub const IndexSet = HashMapUnmanaged(
    u32,
    void,
    StringIndexContext,
    std.hash_map.default_max_load_percentage,
);

pub fn intern(
    idxset: *IndexSet,
    allocator: Allocator,
    buffer: *std.ArrayList(u8),
    key: []const u8,
) !u32 {
    const here: u32 = @intCast(buffer.items.len);
    try buffer.appendSlice(allocator, key);

    const place = try idxset.getOrPutContextAdapted(
        allocator,
        key,
        StringIndexAdapter{ .bytes = buffer },
        StringIndexContext{ .bytes = buffer },
    );

    if (place.found_existing) {
        buffer.shrinkRetainingCapacity(here);
        return place.key_ptr.*;
    } else {
        place.key_ptr.* = here;
        try buffer.append(allocator, 0);
        return here;
    }
}

test "interning deduplicates" {
    const gpa = std.testing.allocator;

    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(gpa);

    var idxset: IndexSet = .empty;
    defer idxset.deinit(gpa);

    const a = try intern(&idxset, gpa, &bytes, "hello");
    const b = try intern(&idxset, gpa, &bytes, "world");
    const c = try intern(&idxset, gpa, &bytes, "hello");

    try std.testing.expect(a != b);
    try std.testing.expect(a == c);

    try std.testing.expect(std.mem.eql(u8, std.mem.sliceTo(bytes.items[a..], 0), "hello"));
    try std.testing.expect(std.mem.eql(u8, std.mem.sliceTo(bytes.items[b..], 0), "world"));
}
