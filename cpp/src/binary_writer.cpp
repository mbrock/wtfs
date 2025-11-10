#include "binary_writer.hpp"
#include <stdexcept>
#include <cstring>

namespace wtfs {

BinaryWriter::BinaryWriter(const std::string& filename)
    : file_(filename, std::ios::binary) {
    if (!file_) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
}

BinaryWriter::~BinaryWriter() {
    if (file_.is_open()) {
        file_.close();
    }
}

void BinaryWriter::write_le_u32(uint32_t value) {
    unsigned char bytes[4];
    bytes[0] = value & 0xFF;
    bytes[1] = (value >> 8) & 0xFF;
    bytes[2] = (value >> 16) & 0xFF;
    bytes[3] = (value >> 24) & 0xFF;
    file_.write(reinterpret_cast<char*>(bytes), 4);
}

void BinaryWriter::write_le_u64(uint64_t value) {
    unsigned char bytes[8];
    bytes[0] = value & 0xFF;
    bytes[1] = (value >> 8) & 0xFF;
    bytes[2] = (value >> 16) & 0xFF;
    bytes[3] = (value >> 24) & 0xFF;
    bytes[4] = (value >> 32) & 0xFF;
    bytes[5] = (value >> 40) & 0xFF;
    bytes[6] = (value >> 48) & 0xFF;
    bytes[7] = (value >> 56) & 0xFF;
    file_.write(reinterpret_cast<char*>(bytes), 8);
}

void BinaryWriter::write_dump(
    const DirectoryTotals& totals,
    const std::vector<BinaryDirectory>& directories,
    const std::vector<BinaryLargeFile>& large_files
) {
    // Write magic header
    file_.write(MAGIC_HEADER, MAGIC_SIZE);

    // Write totals (3 x u64)
    write_le_u64(totals.directories);
    write_le_u64(totals.files);
    write_le_u64(totals.bytes);

    // Build name buffer (directory names + large file names, null-terminated)
    std::vector<char> name_buffer;
    for (const auto& dir : directories) {
        name_buffer.insert(name_buffer.end(), dir.name.begin(), dir.name.end());
        name_buffer.push_back('\0');
    }
    for (const auto& lf : large_files) {
        name_buffer.insert(name_buffer.end(), lf.name.begin(), lf.name.end());
        name_buffer.push_back('\0');
    }

    // Write name buffer length and data
    write_le_u64(name_buffer.size());
    if (!name_buffer.empty()) {
        file_.write(name_buffer.data(), name_buffer.size());
    }

    // Write directory count
    write_le_u64(directories.size());

    // Write directory data (structure-of-arrays format)
    // Parent indices
    for (const auto& dir : directories) {
        write_le_u32(dir.parent_index);
    }

    // Name slice indices (we use sequential indices: 0, 1, 2, ...)
    for (uint32_t i = 0; i < directories.size(); ++i) {
        write_le_u32(i);
    }

    // Total sizes
    for (const auto& dir : directories) {
        write_le_u64(dir.total_size);
    }

    // Total files
    for (const auto& dir : directories) {
        write_le_u64(dir.total_files);
    }

    // Total dirs
    for (const auto& dir : directories) {
        write_le_u64(dir.total_dirs);
    }

    // Write large file count
    write_le_u64(large_files.size());

    if (!large_files.empty()) {
        // Directory indices
        for (const auto& lf : large_files) {
            write_le_u64(lf.directory_index);
        }

        // Name slice indices (continue after directory names)
        for (size_t i = 0; i < large_files.size(); ++i) {
            write_le_u32(static_cast<uint32_t>(directories.size() + i));
        }

        // Sizes
        for (const auto& lf : large_files) {
            write_le_u64(lf.size);
        }
    }

    file_.flush();
}

} // namespace wtfs
