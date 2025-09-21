const std = @import("std");

pub const Alignment = enum { left, right };

pub const Column = struct {
    width: usize,
    alignment: Alignment = .left,
};

pub const Error = error{InvalidColumnCount};

pub const TabWriter = struct {
    writer: *std.Io.Writer,
    columns: []const Column,
    gap: []const u8 = "  ",

    pub fn init(writer: *std.Io.Writer, columns: []const Column) TabWriter {
        return .{ .writer = writer, .columns = columns };
    }

    pub fn withGap(writer: *std.Io.Writer, columns: []const Column, gap: []const u8) TabWriter {
        return .{ .writer = writer, .columns = columns, .gap = gap };
    }

    pub fn writeHeader(self: *TabWriter, headers: []const []const u8) !void {
        try self.writeRow(headers);
    }

    pub fn writeSeparator(self: *TabWriter, ch: u8) !void {
        for (self.columns, 0..) |column, idx| {
            if (idx != 0) try self.writer.writeAll(self.gap);
            const width = column.width;
            const segment_width = if (width == 0) 3 else width;
            try writeRepeated(self.writer, segment_width, ch);
        }
        try self.writer.writeByte('\n');
    }

    pub fn writeRow(self: *TabWriter, values: []const []const u8) !void {
        if (values.len != self.columns.len) {
            return Error.InvalidColumnCount;
        }

        for (values, self.columns, 0..) |value, column, idx| {
            if (idx != 0) try self.writer.writeAll(self.gap);
            try writeColumn(self.writer, column, value);
        }
        try self.writer.writeByte('\n');
    }

    fn writeColumn(writer: *std.Io.Writer, column: Column, value: []const u8) !void {
        const width = column.width;
        if (width == 0 or value.len >= width) {
            try writer.writeAll(value);
            return;
        }

        const padding = width - value.len;
        switch (column.alignment) {
            .left => {
                try writer.writeAll(value);
                try writeRepeated(writer, padding, ' ');
            },
            .right => {
                try writeRepeated(writer, padding, ' ');
                try writer.writeAll(value);
            },
        }
    }
};

fn writeRepeated(writer: *std.Io.Writer, count: usize, ch: u8) !void {
    if (count == 0) return;

    var buffer: [16]u8 = undefined;
    @memset(&buffer, ch);

    var remaining = count;
    while (remaining >= buffer.len) : (remaining -= buffer.len) {
        try writer.writeAll(buffer[0..]);
    }

    if (remaining > 0) {
        try writer.writeAll(buffer[0..remaining]);
    }
}
