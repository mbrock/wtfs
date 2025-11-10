#include "dir_scanner.hpp"
#include <system_error>

namespace wtfs {

DirScanner::DirScanner(const fs::path& path)
    : iter_(path), end_() {
}

std::optional<ScanEntry> DirScanner::next() {
    if (iter_ == end_) {
        return std::nullopt;
    }

    try {
        const auto& entry = *iter_;
        ++iter_;

        std::string name = entry.path().filename().string();
        EntryKind kind = EntryKind::Other;
        uint64_t size = 0;
        uint64_t alloc_size = 0;

        if (entry.is_symlink()) {
            kind = EntryKind::Symlink;
        } else if (entry.is_directory()) {
            kind = EntryKind::Directory;
        } else if (entry.is_regular_file()) {
            kind = EntryKind::File;
            size = entry.file_size();
            // For simplicity, assume alloc_size == size
            // On Unix, we could use stat() to get st_blocks * 512
            alloc_size = size;
        }

        return ScanEntry(name, kind, size, alloc_size);

    } catch (const fs::filesystem_error& e) {
        // Skip entries we can't access
        ++iter_;
        return next(); // Try the next entry
    }
}

bool DirScanner::has_more() const {
    return iter_ != end_;
}

} // namespace wtfs
