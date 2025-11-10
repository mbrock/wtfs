#include "disk_scan.hpp"
#include "dir_scanner.hpp"
#include "tab_writer.hpp"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cctype>

namespace wtfs {

DiskScan::DiskScan(const std::string& root_path, bool skip_hidden)
    : root_path_(root_path)
    , skip_hidden_(skip_hidden)
    , large_file_threshold_(DEFAULT_LARGE_FILE_THRESHOLD) {
}

ScanResults DiskScan::run() {
    auto start = std::chrono::steady_clock::now();

    // Create root directory entry
    directories_.clear();
    large_files_.clear();

    DirectoryNode root;
    root.name = root_path_;
    root.parent_index = 0;
    root.depth = 0;
    directories_.push_back(root);

    // Start scanning
    try {
        scan_directory(fs::path(root_path_), 0, 0);
    } catch (const std::exception& e) {
        std::cerr << "Error scanning directory: " << e.what() << "\n";
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    // Calculate totals
    uint64_t total_dirs = 0;
    uint64_t total_files = 0;
    uint64_t total_bytes = 0;

    for (const auto& dir : directories_) {
        total_dirs += dir.dir_count;
        total_files += dir.file_count;
        total_bytes += dir.total_size;
    }

    return ScanResults{
        .elapsed_ns = static_cast<uint64_t>(elapsed.count()),
        .total_directories = total_dirs,
        .total_files = total_files,
        .total_bytes = total_bytes
    };
}

void DiskScan::scan_directory(const fs::path& path, size_t parent_index, size_t depth) {
    if (should_skip(path)) {
        return;
    }

    try {
        DirScanner scanner(path);

        while (auto entry_opt = scanner.next()) {
            auto& entry = *entry_opt;

            if (should_skip(entry.name)) {
                continue;
            }

            auto entry_path = path / entry.name;

            switch (entry.kind) {
                case EntryKind::File: {
                    directories_[parent_index].file_count++;
                    directories_[parent_index].total_size += entry.size;

                    // Track large files
                    if (entry.size >= large_file_threshold_) {
                        large_files_.push_back(FileEntry{
                            .size = entry.size,
                            .path = entry_path.string()
                        });
                    }
                    break;
                }

                case EntryKind::Directory: {
                    // Create new directory entry
                    DirectoryNode child;
                    child.name = entry.name;
                    child.parent_index = parent_index;
                    child.depth = depth + 1;

                    size_t child_index = directories_.size();
                    directories_.push_back(child);
                    directories_[parent_index].dir_count++;

                    // Recursively scan
                    scan_directory(entry_path, child_index, depth + 1);

                    // Propagate size up to parent
                    directories_[parent_index].total_size += directories_[child_index].total_size;
                    break;
                }

                case EntryKind::Symlink:
                case EntryKind::Other:
                    // Skip symlinks and other special files
                    break;
            }
        }
    } catch (const std::exception& e) {
        // Skip directories we can't access
        std::cerr << "Warning: Cannot access " << path << ": " << e.what() << "\n";
    }
}

bool DiskScan::should_skip(const fs::path& path) const {
    if (!skip_hidden_) {
        return false;
    }

    std::string name = path.filename().string();
    return !name.empty() && name[0] == '.';
}

std::string DiskScan::build_path(size_t dir_index) const {
    if (dir_index >= directories_.size()) {
        return "";
    }

    std::vector<std::string> components;
    size_t current = dir_index;

    while (current != 0 || components.empty()) {
        components.push_back(directories_[current].name);
        if (current == 0) break;
        current = directories_[current].parent_index;
    }

    std::reverse(components.begin(), components.end());

    std::string result;
    for (size_t i = 0; i < components.size(); ++i) {
        if (i > 0) result += "/";
        result += components[i];
    }

    return result;
}

std::vector<SummaryEntry> DiskScan::get_top_level_entries() const {
    std::vector<SummaryEntry> entries;

    // Find direct children of root (depth == 1)
    for (size_t i = 1; i < directories_.size(); ++i) {
        if (directories_[i].depth == 1) {
            entries.push_back(SummaryEntry{
                .index = i,
                .path = build_path(i)
            });
        }
    }

    // Sort by total size descending
    std::sort(entries.begin(), entries.end(), [this](const auto& a, const auto& b) {
        return directories_[a.index].total_size > directories_[b.index].total_size;
    });

    return entries;
}

std::vector<SummaryEntry> DiskScan::get_heaviest_directories(size_t limit) const {
    std::vector<SummaryEntry> entries;

    for (size_t i = 0; i < directories_.size(); ++i) {
        entries.push_back(SummaryEntry{
            .index = i,
            .path = build_path(i)
        });
    }

    // Sort by total size descending
    std::sort(entries.begin(), entries.end(), [this](const auto& a, const auto& b) {
        return directories_[a.index].total_size > directories_[b.index].total_size;
    });

    // Take top N
    if (entries.size() > limit) {
        entries.resize(limit);
    }

    return entries;
}

void DiskScan::report_results(const ScanResults& results) {
    print_summary(results);
    std::cout << "\n";
    print_top_level_table();
    std::cout << "\n";
    print_heaviest_table();

    if (!large_files_.empty()) {
        std::cout << "\n";
        print_large_files();
    }
}

void DiskScan::print_summary(const ScanResults& results) {
    if (directories_.empty()) return;

    const auto& root = directories_[0];
    double elapsed_sec = results.elapsed_ns / 1e9;

    std::cout << root.name << ": "
              << results.total_directories << " dirs, "
              << results.total_files << " files, "
              << format_bytes(results.total_bytes) << " total"
              << " (" << std::fixed << std::setprecision(2) << elapsed_sec << "s)\n";
}

void DiskScan::print_top_level_table() {
    auto entries = get_top_level_entries();

    if (entries.empty()) {
        std::cout << "No top-level directories found.\n";
        return;
    }

    std::cout << "Top-level directories by total size:\n\n";

    uint64_t total_size = directories_.empty() ? 1 : directories_[0].total_size;

    std::vector<Column> columns = {
        Column("Directory", 30, Alignment::Left),
        Column("Size", 12, Alignment::Right),
        Column("Share", 8, Alignment::Right),
        Column("Files", 10, Alignment::Right),
        Column("Dirs", 10, Alignment::Right)
    };

    TabWriter table(std::cout, columns);
    table.write_header();

    for (const auto& entry : entries) {
        const auto& dir = directories_[entry.index];

        double share = total_size > 0 ? (100.0 * dir.total_size) / total_size : 0.0;

        std::ostringstream share_str;
        share_str << std::fixed << std::setprecision(1) << share << "%";

        table.write_row({
            entry.path,
            format_bytes(dir.total_size),
            share_str.str(),
            std::to_string(dir.file_count),
            std::to_string(dir.dir_count)
        });
    }

    table.finish();
}

void DiskScan::print_heaviest_table() {
    auto entries = get_heaviest_directories(10);

    if (entries.empty()) {
        return;
    }

    std::cout << "Heaviest directories in tree:\n\n";

    uint64_t total_size = directories_.empty() ? 1 : directories_[0].total_size;

    std::vector<Column> columns = {
        Column("Directory", 50, Alignment::Left),
        Column("Size", 12, Alignment::Right),
        Column("Share", 8, Alignment::Right)
    };

    TabWriter table(std::cout, columns);
    table.write_header();

    for (const auto& entry : entries) {
        const auto& dir = directories_[entry.index];

        double share = total_size > 0 ? (100.0 * dir.total_size) / total_size : 0.0;

        std::ostringstream share_str;
        share_str << std::fixed << std::setprecision(1) << share << "%";

        // Add indentation based on depth
        std::string indented_path = std::string(entry.index == 0 ? 0 : dir.depth * 2, ' ') + entry.path;

        table.write_row({
            indented_path,
            format_bytes(dir.total_size),
            share_str.str()
        });
    }

    table.finish();
}

void DiskScan::print_large_files() {
    if (large_files_.empty()) {
        return;
    }

    std::cout << "Large files (>" << format_bytes(large_file_threshold_) << "):\n\n";

    // Sort by size descending
    auto sorted_files = large_files_;
    std::sort(sorted_files.begin(), sorted_files.end(), [](const auto& a, const auto& b) {
        return a.size > b.size;
    });

    std::vector<Column> columns = {
        Column("Size", 12, Alignment::Right),
        Column("Path", 60, Alignment::Left)
    };

    TabWriter table(std::cout, columns);
    table.write_header();

    for (const auto& file : sorted_files) {
        table.write_row({
            format_bytes(file.size),
            file.path
        });
    }

    table.finish();
}

// Utility functions

uint64_t parse_size(const std::string& value) {
    if (value.empty()) {
        throw std::invalid_argument("Empty size string");
    }

    std::string num_part = value;
    uint64_t multiplier = 1;

    // Check for suffix
    char last = std::toupper(num_part.back());

    // Handle optional 'B' suffix
    if (last == 'B' && num_part.length() > 1) {
        num_part.pop_back();
        last = std::toupper(num_part.back());
    }

    // Handle K/M/G/T suffixes
    if (last == 'K' || last == 'M' || last == 'G' || last == 'T') {
        switch (last) {
            case 'K': multiplier = 1024ULL; break;
            case 'M': multiplier = 1024ULL * 1024; break;
            case 'G': multiplier = 1024ULL * 1024 * 1024; break;
            case 'T': multiplier = 1024ULL * 1024 * 1024 * 1024; break;
        }
        num_part.pop_back();
    }

    if (num_part.empty()) {
        throw std::invalid_argument("No numeric part in size string");
    }

    try {
        uint64_t number = std::stoull(num_part);
        return number * multiplier;
    } catch (...) {
        throw std::invalid_argument("Invalid number in size string: " + value);
    }
}

std::string format_bytes(uint64_t bytes) {
    const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    int unit_index = 0;
    double size = static_cast<double>(bytes);

    while (size >= 1024.0 && unit_index < 4) {
        size /= 1024.0;
        unit_index++;
    }

    std::ostringstream oss;
    if (unit_index == 0) {
        oss << static_cast<uint64_t>(size) << " " << units[unit_index];
    } else {
        oss << std::fixed << std::setprecision(1) << size << " " << units[unit_index];
    }

    return oss.str();
}

} // namespace wtfs
