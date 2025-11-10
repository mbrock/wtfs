#include "tab_writer.hpp"
#include <iomanip>
#include <sstream>

namespace wtfs {

TabWriter::TabWriter(std::ostream& out, const std::vector<Column>& columns, TableStyle style)
    : out_(out), columns_(columns), style_(style) {
}

void TabWriter::write_header() {
    if (header_written_) return;

    if (style_ == TableStyle::Box) {
        out_ << "┌";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) out_ << "─┬";
            out_ << std::string(columns_[i].width + 2, '─');
        }
        out_ << "─┐\n";
    }

    // Write header row
    if (style_ == TableStyle::Box) {
        out_ << "│";
    }

    for (size_t i = 0; i < columns_.size(); ++i) {
        if (i > 0 && style_ == TableStyle::Box) {
            out_ << " │";
        }
        out_ << " " << pad_cell(columns_[i].header, columns_[i].width, Alignment::Left);
    }

    if (style_ == TableStyle::Box) {
        out_ << " │";
    }
    out_ << "\n";

    // Write separator
    if (style_ == TableStyle::Box) {
        out_ << "├";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) out_ << "─┼";
            out_ << std::string(columns_[i].width + 2, '─');
        }
        out_ << "─┤\n";
    } else {
        write_separator('-');
    }

    header_written_ = true;
}

void TabWriter::write_separator(char fill) {
    if (style_ == TableStyle::Box) {
        out_ << "├";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) out_ << fill << "┼";
            out_ << std::string(columns_[i].width + 2, fill);
        }
        out_ << fill << "┤\n";
    } else {
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) out_ << " ";
            out_ << std::string(columns_[i].width + 2, fill);
        }
        out_ << "\n";
    }
}

void TabWriter::write_row(const std::vector<std::string>& cells) {
    if (!header_written_) {
        write_header();
    }

    if (style_ == TableStyle::Box) {
        out_ << "│";
    }

    for (size_t i = 0; i < columns_.size() && i < cells.size(); ++i) {
        if (i > 0 && style_ == TableStyle::Box) {
            out_ << " │";
        }
        out_ << " " << pad_cell(cells[i], columns_[i].width, columns_[i].alignment);
    }

    if (style_ == TableStyle::Box) {
        out_ << " │";
    }
    out_ << "\n";
}

void TabWriter::finish() {
    if (finished_) return;

    if (style_ == TableStyle::Box) {
        out_ << "└";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) out_ << "─┴";
            out_ << std::string(columns_[i].width + 2, '─');
        }
        out_ << "─┘\n";
    }

    finished_ = true;
}

std::string TabWriter::pad_cell(const std::string& content, size_t width, Alignment align) const {
    if (content.length() >= width) {
        return content.substr(0, width);
    }

    size_t padding = width - content.length();

    switch (align) {
        case Alignment::Left:
            return content + std::string(padding, ' ');

        case Alignment::Right:
            return std::string(padding, ' ') + content;

        case Alignment::Center: {
            size_t left_pad = padding / 2;
            size_t right_pad = padding - left_pad;
            return std::string(left_pad, ' ') + content + std::string(right_pad, ' ');
        }
    }

    return content;
}

} // namespace wtfs
