const std = @import("std");
const TaskQueue = @This();

io: std.Io,
progress: *std.Progress.Node,
high_watermark: std.atomic.Value(usize) = .init(0),
mutex: std.Io.Mutex = .init,
cond: std.Io.Condition = .init,
items: std.ArrayList(usize) = .empty,
done: bool = false,

pub fn deinit(self: *TaskQueue, allocator: std.mem.Allocator) void {
    self.items.deinit(allocator);
}

pub fn push(self: *TaskQueue, allocator: std.mem.Allocator, value: usize) !void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const current_len = self.items.items.len;
    self.progress.setCompletedItems(current_len + 1);
    _ = self.high_watermark.fetchMax(current_len + 1, .acq_rel);

    try self.items.append(allocator, value);
    self.cond.signal(self.io);
}

pub fn pop(self: *TaskQueue) ?usize {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    while (self.items.items.len == 0 and !self.done) {
        self.cond.waitUncancelable(self.io, &self.mutex);
    }

    if (self.items.items.len == 0) {
        return null;
    }

    self.progress.setCompletedItems(self.items.items.len - 1);
    return self.items.pop();
}

pub fn close(self: *TaskQueue) void {
    self.mutex.lockUncancelable(self.io);
    self.done = true;
    self.mutex.unlock(self.io);
    self.cond.broadcast(self.io);
}
