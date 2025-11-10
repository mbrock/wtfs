# wtfs-cpp

Modern C++20 port of the wtfs disk usage analyzer.

## About

This is a clean, idiomatic C++ port of the original Zig implementation. It provides the same functionality with a focus on readability, simplicity, and modern C++ best practices.

## Features

- **Fast directory scanning** using `std::filesystem`
- **Beautiful table output** with Unicode box drawing
- **Large file tracking** with configurable thresholds
- **Clean, modern C++20** with STL containers and smart pointers
- **Simple meson build system**

## Building

### Requirements

- C++20 compatible compiler (GCC 10+, Clang 10+, or MSVC 2019+)
- Meson build system
- Ninja (recommended)

### Build Instructions

```bash
# Install meson and ninja if not already installed
pip3 install --user meson ninja

# Set up build directory
meson setup build

# Compile
meson compile -C build

# Optional: Install
meson install -C build
```

## Usage

```bash
# Scan current directory
./build/wtfs-cpp .

# Scan with custom large file threshold
./build/wtfs-cpp --large-file-threshold 1G ~/Documents

# Include hidden files
./build/wtfs-cpp --no-skip-hidden .

# Show help
./build/wtfs-cpp --help
```

## Command Line Options

- `--skip-hidden` - Skip hidden files and directories (default: true)
- `--no-skip-hidden` - Don't skip hidden files
- `--large-file-threshold SIZE` - Set threshold for large files (default: 100M)
  - SIZE accepts K/M/G/T suffix (base 1024)
- `--help` - Show help message

## Example Output

```
.: 18 dirs, 52 files, 575.3 KiB total (0.01s)

Top-level directories by total size:

┌────────────────────────────────┬─────────────┬─────────┬────────────┬────────────┐
│ Directory                      │ Size        │ Share   │ Files      │ Dirs       │
├────────────────────────────────┼─────────────┼─────────┼────────────┼────────────┤
│ src                            │   159.8 KiB │   59.9% │         13 │          0 │
│ .git                           │    40.8 KiB │   15.3% │          5 │          6 │
│ cpp                            │    19.8 KiB │    7.4% │          1 │          2 │
└────────────────────────────────┴─────────────┴─────────┴────────────┴────────────┘
```

## Architecture

The codebase is organized into clean, modular components:

- **disk_scan.hpp/cpp** - Main scanning logic and reporting
- **dir_scanner.hpp/cpp** - Directory iteration abstraction
- **tab_writer.hpp/cpp** - Table formatting with Unicode box drawing
- **main.cpp** - Command-line interface and argument parsing

## Differences from Zig Version

This C++ port focuses on simplicity and readability:

- Uses STL containers (`std::vector`, `std::string`) instead of custom allocators
- Uses `std::filesystem` for cross-platform directory scanning
- Simpler memory management with RAII
- No binary snapshot format yet (planned for future)
- No parallel scanning yet (planned for future)

## License

MIT License - See parent directory LICENSE file for details.

## Contributing

Contributions welcome! This is an experimental port to explore idiomatic modern C++ design.
