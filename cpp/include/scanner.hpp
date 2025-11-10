#pragma once

#include "binary_writer.hpp"
#include <filesystem>
#include <string>
#include <vector>

namespace wtfs {

namespace fs = std::filesystem;

/// Fast directory scanner focused on binary dump creation
class Scanner {
public:
    Scanner(const std::string& root_path, bool skip_hidden = true);

    void set_large_file_threshold(uint64_t threshold) { large_file_threshold_ = threshold; }

    /// Scan directory tree and write binary dump
    void scan_and_write(const std::string& output_file);

private:
    void scan_directory(const fs::path& path, size_t parent_index);
    bool should_skip(const std::string& name) const;

    std::string root_path_;
    bool skip_hidden_;
    uint64_t large_file_threshold_;

    std::vector<BinaryDirectory> directories_;
    std::vector<BinaryLargeFile> large_files_;

    // Totals
    uint64_t total_dirs_ = 0;
    uint64_t total_files_ = 0;
    uint64_t total_bytes_ = 0;

    static constexpr uint64_t DEFAULT_THRESHOLD = 100 * 1024 * 1024; // 100 MB
};

} // namespace wtfs
