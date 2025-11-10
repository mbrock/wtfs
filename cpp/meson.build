project('wtfs-cpp', 'cpp',
  version: '0.6.0',
  default_options: [
    'cpp_std=c++20',
    'warning_level=3',
    'werror=false'
  ]
)

# Include directories
inc_dirs = include_directories('include')

# Source files for binary-only scanner (primary)
sources_binary = [
  'src/main_binary.cpp',
  'src/scanner.cpp',
  'src/binary_writer.cpp',
]

# Source files for text-output version (kept for reference)
sources_text = [
  'src/main.cpp',
  'src/disk_scan.cpp',
  'src/dir_scanner.cpp',
  'src/tab_writer.cpp',
]

# Compiler flags
cpp_args = []
if get_option('buildtype') == 'release'
  cpp_args += ['-O3']
endif

# Dependencies
thread_dep = dependency('threads')

# Primary executable - binary-only scanner
executable('wtfs-scan',
  sources_binary,
  include_directories: inc_dirs,
  cpp_args: cpp_args,
  dependencies: [thread_dep],
  install: true
)

# Secondary executable - text output (kept for compatibility)
executable('wtfs-text',
  sources_text,
  include_directories: inc_dirs,
  cpp_args: cpp_args,
  dependencies: [thread_dep],
  install: false
)

# Tests (to be added later)
# test_sources = []
# test_exe = executable('tests', test_sources,
#   include_directories: inc_dirs,
#   dependencies: [thread_dep]
# )
# test('unit tests', test_exe)
