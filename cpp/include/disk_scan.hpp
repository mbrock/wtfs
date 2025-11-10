#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace wtfs {

namespace fs = std::filesystem;

// Forward declarations
class TabWriter;

/// Represents a file discovered during scanning
struct FileEntry {
    uint64_t size;
    std::string path;
};

/// Represents a directory node in the scan tree
struct DirectoryNode {
    uint64_t total_size = 0;
    uint64_t file_count = 0;
    uint64_t dir_count = 0;
    std::string name;
    size_t parent_index = 0;
    size_t depth = 0;
};

/// Statistics for the entire scan
struct ScanResults {
    uint64_t elapsed_ns;
    uint64_t total_directories;
    uint64_t total_files;
    uint64_t total_bytes;
};

/// Summary entry for reporting
struct SummaryEntry {
    size_t index;
    std::string path;
};

/// Main disk scanning class
class DiskScan {
public:
    DiskScan(const std::string& root_path, bool skip_hidden = true);
    ~DiskScan() = default;

    // Configuration
    void set_large_file_threshold(uint64_t threshold) { large_file_threshold_ = threshold; }
    void set_skip_hidden(bool skip) { skip_hidden_ = skip; }

    // Main operations
    ScanResults run();
    void report_results(const ScanResults& results);

    // Binary I/O (for future implementation)
    // void save_binary(const std::string& path);
    // ScanResults load_binary(const std::string& path);

private:
    // Scanning
    void scan_directory(const fs::path& path, size_t parent_index, size_t depth);
    bool should_skip(const fs::path& path) const;

    // Summary generation
    std::vector<SummaryEntry> get_top_level_entries() const;
    std::vector<SummaryEntry> get_heaviest_directories(size_t limit = 10) const;
    std::string build_path(size_t dir_index) const;

    // Reporting
    void print_summary(const ScanResults& results);
    void print_top_level_table();
    void print_heaviest_table();
    void print_large_files();

    // Data members
    std::string root_path_;
    bool skip_hidden_;
    uint64_t large_file_threshold_;

    std::vector<DirectoryNode> directories_;
    std::vector<FileEntry> large_files_;

public:
    static constexpr uint64_t DEFAULT_LARGE_FILE_THRESHOLD = 100 * 1024 * 1024; // 100 MB
};

/// Parse size string with K/M/G/T suffix (e.g., "100M", "1G")
uint64_t parse_size(const std::string& value);

/// Format bytes to human-readable string (e.g., "1.5 GiB")
std::string format_bytes(uint64_t bytes);

} // namespace wtfs
