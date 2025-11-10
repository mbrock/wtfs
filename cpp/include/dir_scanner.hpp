#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>

namespace wtfs {

namespace fs = std::filesystem;

/// Entry type for directory scanning
enum class EntryKind {
    File,
    Directory,
    Symlink,
    Other
};

/// Result of scanning a directory entry
struct ScanEntry {
    std::string name;
    EntryKind kind;
    uint64_t size;        // File size in bytes
    uint64_t alloc_size;  // Allocated size on disk (may differ due to sparse files)

    ScanEntry(const std::string& n, EntryKind k, uint64_t s = 0, uint64_t a = 0)
        : name(n), kind(k), size(s), alloc_size(a) {}
};

/// Simple directory scanner using std::filesystem
class DirScanner {
public:
    explicit DirScanner(const fs::path& path);

    std::optional<ScanEntry> next();
    bool has_more() const;

private:
    fs::directory_iterator iter_;
    fs::directory_iterator end_;
};

} // namespace wtfs
