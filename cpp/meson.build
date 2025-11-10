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

# Source files
sources = [
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

# Executable
executable('wtfs-cpp',
  sources,
  include_directories: inc_dirs,
  cpp_args: cpp_args,
  dependencies: [thread_dep],
  install: true
)

# Tests (to be added later)
# test_sources = []
# test_exe = executable('tests', test_sources,
#   include_directories: inc_dirs,
#   dependencies: [thread_dep]
# )
# test('unit tests', test_exe)
