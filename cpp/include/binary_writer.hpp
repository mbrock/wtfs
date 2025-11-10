#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

namespace wtfs {

/// Binary format constants
constexpr char MAGIC_HEADER[] = "wtfsdumpv1     \n";
constexpr size_t MAGIC_SIZE = 16;

/// Totals structure (must match Python/Zig format)
struct DirectoryTotals {
    uint64_t directories;
    uint64_t files;
    uint64_t bytes;
};

/// Directory node for binary output
struct BinaryDirectory {
    uint32_t parent_index;
    std::string name;
    uint64_t total_size;
    uint64_t total_files;
    uint64_t total_dirs;
};

/// Large file entry for binary output
struct BinaryLargeFile {
    uint64_t directory_index;
    std::string name;
    uint64_t size;
};

/// Writer for wtfsdumpv1 binary format
class BinaryWriter {
public:
    explicit BinaryWriter(const std::string& filename);
    ~BinaryWriter();

    void write_dump(
        const DirectoryTotals& totals,
        const std::vector<BinaryDirectory>& directories,
        const std::vector<BinaryLargeFile>& large_files
    );

private:
    void write_le_u32(uint32_t value);
    void write_le_u64(uint64_t value);

    std::ofstream file_;
};

} // namespace wtfs
