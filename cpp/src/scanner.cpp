#include "scanner.hpp"
#include <iostream>
#include <chrono>

namespace wtfs {

Scanner::Scanner(const std::string& root_path, bool skip_hidden)
    : root_path_(root_path)
    , skip_hidden_(skip_hidden)
    , large_file_threshold_(DEFAULT_THRESHOLD) {
}

void Scanner::scan_and_write(const std::string& output_file) {
    auto start = std::chrono::steady_clock::now();

    // Reset state
    directories_.clear();
    large_files_.clear();
    total_dirs_ = 0;
    total_files_ = 0;
    total_bytes_ = 0;

    // Create root directory
    BinaryDirectory root;
    root.parent_index = 0;
    root.name = root_path_;
    root.total_size = 0;
    root.total_files = 0;
    root.total_dirs = 0;
    directories_.push_back(root);

    // Scan recursively
    try {
        scan_directory(fs::path(root_path_), 0);
    } catch (const std::exception& e) {
        std::cerr << "Error during scan: " << e.what() << "\n";
    }

    // Calculate totals from root directory
    if (!directories_.empty()) {
        total_dirs_ = directories_[0].total_dirs;
        total_files_ = directories_[0].total_files;
        total_bytes_ = directories_[0].total_size;
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print summary to stderr
    std::cerr << "Scanned " << total_dirs_ << " directories, "
              << total_files_ << " files, "
              << total_bytes_ << " bytes in "
              << elapsed.count() << "ms\n";
    std::cerr << "Writing binary dump to " << output_file << "...\n";

    // Write binary dump
    DirectoryTotals totals{
        .directories = total_dirs_,
        .files = total_files_,
        .bytes = total_bytes_
    };

    BinaryWriter writer(output_file);
    writer.write_dump(totals, directories_, large_files_);

    std::cerr << "Done. Wrote " << directories_.size() << " directory entries, "
              << large_files_.size() << " large files\n";
}

void Scanner::scan_directory(const fs::path& path, size_t parent_index) {
    if (should_skip(path.filename().string())) {
        return;
    }

    try {
        for (const auto& entry : fs::directory_iterator(path)) {
            const std::string name = entry.path().filename().string();

            if (should_skip(name)) {
                continue;
            }

            try {
                if (entry.is_symlink()) {
                    // Skip symlinks
                    continue;
                }

                if (entry.is_regular_file()) {
                    const uint64_t size = entry.file_size();

                    directories_[parent_index].total_files++;
                    directories_[parent_index].total_size += size;

                    // Track large files
                    if (size >= large_file_threshold_) {
                        large_files_.push_back(BinaryLargeFile{
                            .directory_index = static_cast<uint64_t>(parent_index),
                            .name = name,
                            .size = size
                        });
                    }
                }
                else if (entry.is_directory()) {
                    // Create child directory entry
                    BinaryDirectory child;
                    child.parent_index = static_cast<uint32_t>(parent_index);
                    child.name = name;
                    child.total_size = 0;
                    child.total_files = 0;
                    child.total_dirs = 0;

                    size_t child_index = directories_.size();
                    directories_.push_back(child);
                    directories_[parent_index].total_dirs++;

                    // Recursively scan
                    scan_directory(entry.path(), child_index);

                    // Propagate totals to parent
                    directories_[parent_index].total_size += directories_[child_index].total_size;
                    directories_[parent_index].total_files += directories_[child_index].total_files;
                    directories_[parent_index].total_dirs += directories_[child_index].total_dirs;
                }
            }
            catch (const std::filesystem::filesystem_error& e) {
                // Skip entries we can't access
                std::cerr << "Warning: Cannot access " << entry.path() << ": " << e.what() << "\n";
            }
        }
    }
    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Warning: Cannot read directory " << path << ": " << e.what() << "\n";
    }
}

bool Scanner::should_skip(const std::string& name) const {
    if (!skip_hidden_) {
        return false;
    }
    return !name.empty() && name[0] == '.';
}

} // namespace wtfs
