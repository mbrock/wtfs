#pragma once

#include <iostream>
#include <string>
#include <vector>

namespace wtfs {

enum class Alignment {
    Left,
    Right,
    Center
};

struct Column {
    size_t width;
    Alignment alignment = Alignment::Left;
    std::string header;

    Column(const std::string& h, size_t w, Alignment a = Alignment::Left)
        : width(w), alignment(a), header(h) {}
};

enum class TableStyle {
    Plain,
    Box
};

/// A simple table writer for formatted output
class TabWriter {
public:
    TabWriter(std::ostream& out, const std::vector<Column>& columns, TableStyle style = TableStyle::Box);

    void write_header();
    void write_separator(char fill = '-');
    void write_row(const std::vector<std::string>& cells);
    void finish();

private:
    std::string pad_cell(const std::string& content, size_t width, Alignment align) const;

    std::ostream& out_;
    std::vector<Column> columns_;
    TableStyle style_;
    bool header_written_ = false;
    bool finished_ = false;
};

} // namespace wtfs
