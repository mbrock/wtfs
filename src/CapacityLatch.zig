/// CapacityLatch coordinates **lock-free appends** with **serialized growth**.
///
/// It maintains two independent counters:
///  - `len`: number of rows **reserved** (linearization point for appenders)
///  - `cap`: number of rows **published/initialized** (safe for readers to deref)
///
/// Fast paths:
///  - Readers: call `snapshot()` once (acquire) and use its `.cap` bound.
///  - Appenders: call `reservation(n)`, ensure capacity ≥ `reservation.len` in your
///    container, then `reservation.take()` to CAS `len` and claim indices.
///
/// Growth path (rare):
///  - Call `acquireOrWaitFor(target_cap)`:
///     * returns **Growth** (mutex held) if work is needed — you initialize backing
///       storage up to `target_cap`, then `publishTo(target_cap)` (release) and unlock
///     * returns **null** if another thread already brought `cap` to `target_cap`
///       (this function waited on a condvar if necessary)

// Memory ordering (why it’s correct):
//  - Writers grow: **initialize memory first**, then `publishTo()` does a **release**
//    store to `cap` and wakes waiters. This guarantees visibility of all initialized
//    bytes to threads that subsequently **acquire**-load `cap` (via `snapshot()`).
//  - Appenders: `Reservation.take()` CASes `len` with **acq_rel**, which linearizes
//    append; the **acquire** in readers’ `snapshot()` must precede any deref into
//    shelves (your container enforces `index < cap`), so uninitialized memory is never
//    observed.
const CapacityLatch = @This();

const std = @import("std");
const test_threads = @import("test_threading.zig");

/// Logical rows reserved (linearization point for append).
len: std.atomic.Value(usize) = .init(0),

/// Logical rows published/initialized (upper bound safe for readers).
cap: std.atomic.Value(usize) = .init(0),

/// Serialized growth; waiters sleep here until `_cap` advances.
mu: std.Thread.Mutex = .{},
cv: std.Thread.Condition = .{},

/// Snapshot of `{ len, cap }` with **acquire** semantics on both.
///
/// Readers should take one snapshot and ensure any index access satisfies
/// `index < snap.cap`. Appenders may race and increase `len`, but readers
/// will never dereference beyond `cap` (which is only advanced after
/// initialization with a **release** store).
pub const Snapshot = struct {
    len: usize,
    cap: usize,
};

pub fn snapshot(self: *const CapacityLatch) Snapshot {
    // Acquire loads ensure that if `cap` was published (release),
    // readers will observe all initialization that happened-before.
    return .{
        .len = self.len.load(.acquire),
        .cap = self.cap.load(.acquire),
    };
}

/// An optimistic claim for `count` rows.
///
/// Usage pattern:
///
/// ```
///   var r = try latch.reservation(count);
///   try ensureCapacityTo(r.len); // in your container
///   if (r.take()) |base| { /* write rows [base ..< base+count) */ } else retry;
/// ```
pub const Reservation = struct {
    latch: *CapacityLatch,
    base: usize, // starting index being claimed
    len: usize, // base + count (the post-CAS length)

    /// Try to finalize the reservation: CAS `_len` from `base` to `len`.
    ///
    /// Returns:
    ///   - `base` on success (you own rows `[base ..< len)`)
    ///   - `null` if another thread raced; caller should retry with a new reservation
    ///
    /// Correctness: CAS with **acq_rel** linearizes the append. The acquire part
    /// pairs with any prior release on `_len` and prevents reordering of your writes
    /// to the claimed rows with respect to subsequent observers that use `snapshot()`.
    pub fn take(self: Reservation) ?usize {
        const ok = self.latch.len.cmpxchgStrong(
            self.base,
            self.len,
            .acq_rel, // publish our claim; synchronize-with readers observing len if they care
            .acquire, // on failure, still an acquire to avoid reordering on re-read
        ) == null;
        return if (ok) self.base else null;
    }
};

/// Make a reservation for `count` rows (0 disallowed).
///
/// Note: this does **not** ensure capacity; your container must call
/// `acquireOrWaitFor(reservation.len)` (via `ensureCapacityTo`) and initialize
/// backing storage before dereferencing within the new range.
pub fn reservation(self: *CapacityLatch, count: usize) error{Overflow}!Reservation {
    std.debug.assert(count > 0);
    const base = self.len.load(.acquire);
    const len = try std.math.add(usize, base, count);
    return .{ .latch = self, .base = base, .len = len };
}

/// Growth guard: owning the mutex while preparing capacity.
///
/// Invariants while held:
///  - Caller must initialize all storage for rows in `[start_cap .. target_cap)`
///    **before** calling `publishTo(target_cap)`.
pub const Growth = struct {
    l: *CapacityLatch,
    start_cap: usize,
    held: bool = true,

    /// Publish all rows up to `target_cap` (exclusive upper bound), with **release**,
    /// then wake all waiters. This is the *only* place `_cap` advances.
    ///
    /// Precondition: the caller has fully initialized every byte required
    /// for rows `[start_cap .. target_cap)`.
    ///
    /// Correctness: release-store here guarantees that any thread performing
    /// an acquire-load of `_cap` (via `snapshot()`) and seeing `cap >= target_cap`
    /// will also observe the initialization that happened-before this call.
    pub fn publishTo(self: *Growth, target_cap: usize) void {
        self.l.cap.store(target_cap, .release);
        // Wake all waiters; Condition requires holding the mutex.
        self.l.cv.broadcast();
    }

    /// Release the growth mutex. Prefer `defer g.unlock()`.
    pub fn unlock(self: *Growth) void {
        if (!self.held) return;
        self.l.mu.unlock();
        self.held = false;
    }
};

/// Ensure `cap >= target_cap`: either become the grower (return a Growth guard
/// with the mutex held) or, if another thread can/does grow, wait until the
/// condition is satisfied and return `null`.
///
/// Typical usage in your container:
///
/// ```
///   if (latch.acquireOrWaitFor(target)) |g| {
///       defer g.unlock();
///       // initialize rows [g.start_cap .. target)
///       g.publishTo(target);
///   } // else: target already satisfied (this waited if needed)
/// ```
pub fn acquireOrWaitFor(self: *CapacityLatch, target_cap: usize) ?Growth {
    self.mu.lock();

    // Fast path: nothing to do (also covers spurious wakeups in callers).
    const now = self.cap.load(.acquire);
    if (now >= target_cap) {
        self.mu.unlock();
        return null;
    }

    // Policy: caller grows immediately (holds the mutex).
    // If you prefer to *always* wait instead, replace this with a loop on _cv.wait.
    return .{ .l = self, .start_cap = now };
}

test "CapacityLatch concurrent reservations produce unique indices" {
    var latch = CapacityLatch{};

    const thread_count = 8;
    const per_thread = 128;
    const total: usize = thread_count * per_thread;

    const allocator = std.testing.allocator;
    const results = try allocator.alloc(usize, total);
    defer allocator.free(results);

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(tid: usize, latch_ptr: *CapacityLatch, per: usize, out: []usize) !void {
            const start = tid * per;
            var i: usize = 0;
            while (i < per) : (i += 1) {
                while (true) {
                    var r = try latch_ptr.reservation(1);
                    if (r.take()) |base| {
                        out[start + i] = base;
                        break;
                    }
                }
            }
        }
    };

    try group.spawnMany(thread_count, Worker.run, .{ &latch, per_thread, results });
    group.wait();

    const seen = try allocator.alloc(bool, total);
    defer allocator.free(seen);
    for (seen) |*slot| {
        slot.* = false;
    }

    for (results[0..total]) |base| {
        try std.testing.expect(base < total);
        try std.testing.expect(!seen[base]);
        seen[base] = true;
    }

    try std.testing.expectEqual(total, latch.len.load(.acquire));
}

test "CapacityLatch acquireOrWaitFor selects a single grower" {
    var latch = CapacityLatch{};

    const initial_cap: usize = 16;
    const target_cap: usize = initial_cap * 3;
    latch.len.store(initial_cap, .release);
    latch.cap.store(initial_cap, .release);

    const thread_count = 5;

    var grower_count = std.atomic.Value(usize).init(0);
    var waiter_count = std.atomic.Value(usize).init(0);

    var group = try test_threads.ThreadTestGroup.init(thread_count);

    const Worker = struct {
        fn run(
            tid: usize,
            latch_ptr: *CapacityLatch,
            target: usize,
            grower_counter: *std.atomic.Value(usize),
            waiter_counter: *std.atomic.Value(usize),
            expected_start: usize,
        ) !void {
            _ = tid;
            if (latch_ptr.acquireOrWaitFor(target)) |growth_value| {
                var g = growth_value;
                defer g.unlock();

                const prev = grower_counter.fetchAdd(1, .acq_rel);
                try std.testing.expectEqual(@as(usize, 0), prev);
                try std.testing.expectEqual(expected_start, g.start_cap);

                g.publishTo(target);
            } else {
                _ = waiter_counter.fetchAdd(1, .acq_rel);
                const snap = latch_ptr.snapshot();
                try std.testing.expectEqual(target, snap.cap);
                try std.testing.expectEqual(expected_start, snap.len);
            }
        }
    };

    try group.spawnMany(
        thread_count,
        Worker.run,
        .{ &latch, target_cap, &grower_count, &waiter_count, initial_cap },
    );
    group.wait();

    try std.testing.expectEqual(@as(usize, 1), grower_count.load(.acquire));
    try std.testing.expectEqual(@as(usize, thread_count - 1), waiter_count.load(.acquire));

    const final = latch.snapshot();
    try std.testing.expectEqual(target_cap, final.cap);
    try std.testing.expectEqual(initial_cap, final.len);
}
