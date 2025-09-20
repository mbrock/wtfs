const std = @import("std");
const TaskQueue = @This();

progress: *std.Progress.Node,
high_watermark: std.atomic.Value(usize) = .init(0),
mutex: std.Thread.Mutex = .{},
cond: std.Thread.Condition = .{},
items: std.ArrayList(usize) = .{},
done: bool = false,

pub fn deinit(self: *TaskQueue, allocator: std.mem.Allocator) void {
    self.items.deinit(allocator);
}

pub fn push(self: *TaskQueue, allocator: std.mem.Allocator, value: usize) !void {
    self.mutex.lock();
    defer self.mutex.unlock();
    const current_len = self.items.items.len;
    self.progress.setCompletedItems(current_len + 1);
    _ = self.high_watermark.fetchMax(current_len + 1, .acq_rel);

    try self.items.append(allocator, value);
    self.cond.signal();
}

pub fn pop(self: *TaskQueue) ?usize {
    self.mutex.lock();
    defer self.mutex.unlock();

    while (self.items.items.len == 0 and !self.done) {
        self.cond.wait(&self.mutex);
    }

    if (self.items.items.len == 0) {
        return null;
    }

    self.progress.setCompletedItems(self.items.items.len - 1);
    return self.items.pop();
}

pub fn close(self: *TaskQueue) void {
    self.mutex.lock();
    self.done = true;
    self.mutex.unlock();
    self.cond.broadcast();
}
