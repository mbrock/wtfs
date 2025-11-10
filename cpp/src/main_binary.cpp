#include "scanner.hpp"
#include <iostream>
#include <string>
#include <cstdlib>

using namespace wtfs;

void print_usage(const char* program_name) {
    std::cerr << "wtfs-cpp: Fast disk scanner with binary dump output\n\n";
    std::cerr << "Usage: " << program_name << " [OPTIONS] <directory>\n\n";
    std::cerr << "Arguments:\n";
    std::cerr << "  <directory>    Directory to scan\n\n";
    std::cerr << "Options:\n";
    std::cerr << "  --skip-hidden              Skip hidden files (default: false)\n";
    std::cerr << "  --large-file-threshold N   Track files larger than N (default: 100M)\n";
    std::cerr << "                             Accepts K/M/G/T suffix (e.g., 1G, 500M)\n";
    std::cerr << "  --binary-output PATH       Output file for binary dump (required)\n";
    std::cerr << "  --help                     Show this help\n\n";
    std::cerr << "Examples:\n";
    std::cerr << "  " << program_name << " --binary-output scan.bin /home/user\n";
    std::cerr << "  " << program_name << " --large-file-threshold 1G --binary-output out.bin .\n\n";
    std::cerr << "Output format:\n";
    std::cerr << "  The binary dump uses the wtfsdumpv1 format and can be analyzed using:\n";
    std::cerr << "    python3 -m wtfs.dump output.bin\n";
}

uint64_t parse_size(const std::string& value) {
    if (value.empty()) {
        throw std::invalid_argument("Empty size string");
    }

    std::string num_part = value;
    uint64_t multiplier = 1;

    char last = std::toupper(num_part.back());
    if (last == 'B' && num_part.length() > 1) {
        num_part.pop_back();
        last = std::toupper(num_part.back());
    }

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
        throw std::invalid_argument("No numeric part");
    }

    return std::stoull(num_part) * multiplier;
}

int main(int argc, char* argv[]) {
    bool skip_hidden = false;  // Default: scan hidden files (like Zig version)
    uint64_t large_file_threshold = 100 * 1024 * 1024; // 100 MB
    std::string directory;
    std::string output_file;

    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
        else if (arg == "--skip-hidden") {
            skip_hidden = true;
        }
        else if (arg == "--large-file-threshold") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --large-file-threshold requires a value\n";
                return 1;
            }
            try {
                large_file_threshold = parse_size(argv[++i]);
            } catch (const std::exception& e) {
                std::cerr << "Error: Invalid threshold: " << e.what() << "\n";
                return 1;
            }
        }
        else if (arg.rfind("--large-file-threshold=", 0) == 0) {
            try {
                large_file_threshold = parse_size(arg.substr(23));
            } catch (const std::exception& e) {
                std::cerr << "Error: Invalid threshold: " << e.what() << "\n";
                return 1;
            }
        }
        else if (arg == "--binary-output") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --binary-output requires a value\n";
                return 1;
            }
            output_file = argv[++i];
        }
        else if (arg.rfind("--binary-output=", 0) == 0) {
            output_file = arg.substr(16);
        }
        else if (arg == "--force") {
            // Accepted for compatibility but ignored (always overwrites)
        }
        else if (arg[0] == '-') {
            std::cerr << "Error: Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
        else {
            // Positional argument - the directory
            if (directory.empty()) {
                directory = arg;
            } else {
                std::cerr << "Error: Too many positional arguments\n";
                print_usage(argv[0]);
                return 1;
            }
        }
    }

    // Validate arguments
    if (directory.empty()) {
        std::cerr << "Error: Missing directory argument\n\n";
        print_usage(argv[0]);
        return 1;
    }

    if (output_file.empty()) {
        std::cerr << "Error: Missing --binary-output option\n\n";
        print_usage(argv[0]);
        return 1;
    }

    try {
        Scanner scanner(directory, skip_hidden);
        scanner.set_large_file_threshold(large_file_threshold);
        scanner.scan_and_write(output_file);
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
