const std = @import("std");

pub const Alignment = enum { left, right };

pub const Column = struct {
    width: usize,
    alignment: Alignment = .left,
};

pub const BorderStyle = enum { none, box };

pub const Options = struct {
    gap: []const u8 = "  ",
    style: BorderStyle = .none,
};

pub const Error = error{InvalidColumnCount};

pub const TabWriter = struct {
    writer: *std.Io.Writer,
    columns: []const Column,
    gap: []const u8,
    style: BorderStyle,

    pub fn init(writer: *std.Io.Writer, columns: []const Column) TabWriter {
        return initWithOptions(writer, columns, .{});
    }

    pub fn initWithOptions(
        writer: *std.Io.Writer,
        columns: []const Column,
        options: Options,
    ) TabWriter {
        return .{
            .writer = writer,
            .columns = columns,
            .gap = options.gap,
            .style = options.style,
        };
    }

    pub fn withGap(writer: *std.Io.Writer, columns: []const Column, gap: []const u8) TabWriter {
        return initWithOptions(writer, columns, .{ .gap = gap });
    }

    pub fn writeHeader(self: *TabWriter, headers: []const []const u8) !void {
        if (headers.len != self.columns.len) {
            return Error.InvalidColumnCount;
        }

        switch (self.style) {
            .none => {
                try self.writeRowInternal(headers, false);
            },
            .box => {
                try self.writeBorder(.top);
                try self.writeRowInternal(headers, true);
            },
        }
    }

    pub fn writeSeparator(self: *TabWriter, fill: []const u8) !void {
        switch (self.style) {
            .none => try self.writePlainSeparator(fill),
            .box => try self.writeBorder(.header),
        }
    }

    pub fn writeRow(self: *TabWriter, values: []const []const u8) !void {
        if (values.len != self.columns.len) {
            return Error.InvalidColumnCount;
        }
        try self.writeRowInternal(values, self.style == .box);
    }

    pub fn finish(self: *TabWriter) !void {
        if (self.style == .box) {
            try self.writeBorder(.bottom);
        }
    }

    fn writeRowInternal(self: *TabWriter, values: []const []const u8, boxed: bool) !void {
        if (boxed) {
            try self.writer.writeAll("│");
            for (values, self.columns) |value, column| {
                const cell_width = column.width;
                try writeRepeated(self.writer, " ", 1);
                try writeColumn(self.writer, column, value, cell_width);
                try writeRepeated(self.writer, " ", 1);
                try self.writer.writeAll("│");
            }
            try self.writer.writeByte('\n');
            return;
        }

        for (values, self.columns, 0..) |value, column, idx| {
            if (idx != 0) try self.writer.writeAll(self.gap);
            try writeColumn(self.writer, column, value, column.width);
        }
        try self.writer.writeByte('\n');
    }

    fn writePlainSeparator(self: *TabWriter, fill: []const u8) !void {
        if (fill.len == 0) return;
        for (self.columns, 0..) |column, idx| {
            if (idx != 0) try self.writer.writeAll(self.gap);
            const segment_width = if (column.width == 0) 3 else column.width;
            try writeRepeated(self.writer, fill, segment_width);
        }
        try self.writer.writeByte('\n');
    }

    const BorderKind = enum { top, header, bottom };

    fn writeBorder(self: *TabWriter, kind: BorderKind) !void {
        const symbols = switch (kind) {
            .top => BorderSymbols{
                .left = "┌",
                .mid = "┬",
                .right = "┐",
            },
            .header => BorderSymbols{
                .left = "├",
                .mid = "┼",
                .right = "┤",
            },
            .bottom => BorderSymbols{
                .left = "└",
                .mid = "┴",
                .right = "┘",
            },
        };

        try self.writer.writeAll(symbols.left);
        for (self.columns, 0..) |column, idx| {
            const run = column.width + 2;
            try writeRepeated(self.writer, "─", run);
            if (idx + 1 == self.columns.len) {
                try self.writer.writeAll(symbols.right);
            } else {
                try self.writer.writeAll(symbols.mid);
            }
        }
        try self.writer.writeByte('\n');
    }

    const BorderSymbols = struct {
        left: []const u8,
        mid: []const u8,
        right: []const u8,
    };
};

fn writeColumn(
    writer: *std.Io.Writer,
    column: Column,
    value: []const u8,
    width: usize,
) !void {
    if (width == 0) {
        try writer.writeAll(value);
        return;
    }

    const trimmed = try trimForWidth(value, width, column.alignment);
    const len = try approximateMonospaceStringWidth(trimmed);
    const padding = if (len >= width) 0 else width - len;

    switch (column.alignment) {
        .left => {
            try writer.writeAll(trimmed);
            if (padding != 0) try writeRepeated(writer, " ", padding);
        },
        .right => {
            if (padding != 0) try writeRepeated(writer, " ", padding);
            try writer.writeAll(trimmed);
        },
    }
}

fn trimForWidth(value: []const u8, width: usize, alignment: Alignment) ![]const u8 {
    const len = try approximateMonospaceStringWidth(value);
    if (len <= width) return value;
    return switch (alignment) {
        .left => try glyphSlice(value, 0, width),
        .right => try glyphSlice(value, len - width, null),
    };
}

fn glyphSlice(bytes: []const u8, left: usize, right: ?usize) ![]const u8 {
    var string = std.unicode.Utf8Iterator{ .bytes = bytes, .i = 0 };
    const b1 = string.peek(left).len;
    string.i = b1;
    return if (right) |r|
        bytes[b1..string.peek(r).len]
    else
        bytes[b1..];
}

/// not correct, need unicode glyph table data
fn approximateMonospaceStringWidth(value: []const u8) !usize {
    return std.unicode.utf8CountCodepoints(value);
}

fn writeRepeated(writer: *std.Io.Writer, sequence: []const u8, count: usize) !void {
    try writer.splatBytesAll(sequence, count);
}

test "bordered table renders correctly to fixed writer" {
    const expected =
        \\┌─────────────┬─────────────┬─────────┬──────────────┬─────────────┐
        \\│ Directory   │        Size │   Share │        Files │        Dirs │
        \\├─────────────┼─────────────┼─────────┼──────────────┼─────────────┤
        \\│ ytcut       │     1.5 GiB │   27.5% │           29 │           8 │
        \\│ froth       │   685.8 MiB │   11.9% │        4.29K │         337 │
        \\│ Marvel’     │   636.7 MiB │   11.1% │        40.2K │       2.98K │
        \\└─────────────┴─────────────┴─────────┴──────────────┴─────────────┘
        \\
    ;

    var columns = [_]Column{
        Column{ .width = "ytcut      ".len },
        Column{ .width = 11, .alignment = .right },
        Column{ .width = 7, .alignment = .right },
        Column{ .width = 12, .alignment = .right },
        Column{ .width = 11, .alignment = .right },
    };

    var buffer: [expected.len]u8 = undefined;
    var stdout = std.Io.Writer.fixed(&buffer);
    var table = TabWriter.initWithOptions(&stdout, columns[0..], .{ .style = BorderStyle.box });

    try table.writeHeader(&[_][]const u8{ "Directory", "Size", "Share", "Files", "Dirs" });
    try table.writeSeparator("─");

    inline for (.{
        &[_][]const u8{ "ytcut", "1.5 GiB", "27.5%", "29", "8" },
        &[_][]const u8{ "froth", "685.8 MiB", "11.9%", "4.29K", "337" },
        &[_][]const u8{ "Marvel’", "636.7 MiB", "11.1%", "40.2K", "2.98K" },
    }) |x| {
        try table.writeRow(x);
    }

    try table.finish();

    try std.testing.expectEqualStrings(expected, stdout.buffered());
}
