project('pg_smgrstat', 'c',
  version: '0.1.0',
  meson_version: '>=1.1',
  default_options: [
    'c_std=gnu23',
    'warning_level=2',
  ],
)

# Locate pg_config — override with -Dpg_config=/path/to/pg_config
pg_config = find_program(get_option('pg_config'), required: true)

pg_includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
pg_pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
pg_sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()

pg_includes = include_directories(pg_includedir_server)

libm = meson.get_compiler('c').find_library('m', required: false)

shared_module('pg_smgrstat',
  'src/pg_smgrstat.c',
  'src/smgr_stats_guc.c',
  'src/smgr_stats_store.c',
  'src/smgr_stats_link.c',
  'src/smgr_stats_seq.c',
  'src/smgr_stats_worker.c',
  'src/smgr_stats_hist.c',
  'src/smgr_stats_functions.c',
  include_directories: [pg_includes, include_directories('src')],
  dependencies: [libm],
  name_prefix: '',  # produce pg_smgrstat.so, not libpg_smgrstat.so
  install: true,
  install_dir: pg_pkglibdir,
)

configure_file(
  input: 'pg_smgrstat.control.in',
  output: 'pg_smgrstat.control',
  configuration: {'VERSION': meson.project_version()},
  install: true,
  install_dir: pg_sharedir / 'extension',
)

install_data(
  'sql/pg_smgrstat--0.1.0.sql',
  install_dir: pg_sharedir / 'extension',
)

clang_format = find_program('clang-format', required: false)
if clang_format.found()
  run_target('format',
    command: ['sh', '-c', 'find "$1"/src -name "*.c" -o -name "*.h" | xargs "$2" -i', '--',
             meson.project_source_root(), clang_format.full_path()],
  )
endif

clang_tidy = find_program('clang-tidy', required: false)
if clang_tidy.found()
  run_target('tidy',
    command: ['sh', '-c', 'find "$1"/src -name "*.c" | xargs "$2" -p "$3"', '--',
             meson.project_source_root(), clang_tidy.full_path(), meson.project_build_root()],
  )
endif

# Integration tests via RSpec (requires `meson install` first)
rspec = find_program('rspec', required: false)
if rspec.found()
  test('integration',
    rspec,
    workdir: meson.project_source_root(),
    env: {
      'PG_CONFIG': pg_config.full_path(),
    },
    timeout: 60,
  )
endif
