#include "disk_scan.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <cstring>

using namespace wtfs;

void print_usage(const char* program_name) {
    std::cerr << "usage: " << program_name
              << " [--skip-hidden] [--large-file-threshold SIZE] [dir]\n";
    std::cerr << "       SIZE accepts optional K/M/G/T suffix (base 1024)\n";
    std::cerr << "\n";
    std::cerr << "Options:\n";
    std::cerr << "  --skip-hidden              Skip hidden files and directories (default: true)\n";
    std::cerr << "  --no-skip-hidden           Don't skip hidden files and directories\n";
    std::cerr << "  --large-file-threshold N   Set threshold for large files (default: 100M)\n";
    std::cerr << "  --help                     Show this help message\n";
    std::cerr << "\n";
    std::cerr << "Examples:\n";
    std::cerr << "  " << program_name << " .                           # Scan current directory\n";
    std::cerr << "  " << program_name << " --large-file-threshold 1G ~  # Scan home with 1GB threshold\n";
}

int main(int argc, char* argv[]) {
    std::string root_path = ".";
    bool skip_hidden = true;
    uint64_t large_file_threshold = DiskScan::DEFAULT_LARGE_FILE_THRESHOLD;

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
        else if (arg == "--no-skip-hidden") {
            skip_hidden = false;
        }
        else if (arg == "--large-file-threshold") {
            if (i + 1 >= argc) {
                std::cerr << "Error: --large-file-threshold requires a value\n";
                print_usage(argv[0]);
                return 1;
            }
            try {
                large_file_threshold = parse_size(argv[++i]);
            } catch (const std::exception& e) {
                std::cerr << "Error: Invalid size for --large-file-threshold: " << e.what() << "\n";
                return 1;
            }
        }
        else if (arg.rfind("--large-file-threshold=", 0) == 0) {
            std::string value = arg.substr(23);
            try {
                large_file_threshold = parse_size(value);
            } catch (const std::exception& e) {
                std::cerr << "Error: Invalid size for --large-file-threshold: " << e.what() << "\n";
                return 1;
            }
        }
        else if (arg[0] == '-') {
            std::cerr << "Error: Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
        else {
            // Assume it's the directory path
            if (!root_path.empty() && root_path != ".") {
                std::cerr << "Error: Multiple directory paths specified\n";
                print_usage(argv[0]);
                return 1;
            }
            root_path = arg;
        }
    }

    try {
        // Create scanner and configure it
        DiskScan scanner(root_path, skip_hidden);
        scanner.set_large_file_threshold(large_file_threshold);

        // Run the scan
        auto results = scanner.run();

        // Report results
        scanner.report_results(results);

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
