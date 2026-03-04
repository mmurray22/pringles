project('pringles', 'cpp',
  version : '1.0.0',
  default_options : ['warning_level=3', 'cpp_std=c++17', 'werror=true'])
# flags + address sanitizers
add_global_arguments('-DSOME_TOKEN=value', language : 'cpp')
add_project_arguments(
  '-mno-omit-leaf-frame-pointer',
  language: 'cpp'
)
add_project_arguments(
  '-fno-omit-frame-pointer',
  language: 'cpp'
)
yaml_cpp = dependency('yaml-cpp', static: true)
protoc = find_program('protoc')
protobuf = dependency('protobuf', static: true)
# Meson will now download and compile these as static libraries automatically
#spdlog = dependency('spdlog', fallback: ['spdlog', 'spdlog_dep'], static: true)
spdlog = declare_dependency(
    link_args: ['/proj/ove-PG0/murray/spdlog/build/libspdlog.a'],
    include_directories: include_directories('/proj/ove-PG0/murray/spdlog/include')
)
fmt_dep = dependency('fmt', fallback: ['fmt', 'fmt_dep'], static: true)
#spdlog = dependency('spdlog', fallback: ['spdlog', 'spdlog_dep'], static: true)
#tbb_dep = dependency('tbb', required: true, static: true) 
tbb_dep = declare_dependency(
    link_args: ['/proj/ove-PG0/murray/oneTBB/build/gnu_11.4_cxx11_64_release/libtbb.a'],
    include_directories: include_directories('/proj/ove-PG0/murray/oneTBB/include')
)
add_project_link_arguments(['-lprotobuf'], language: ['cpp'])
add_project_link_arguments(['-static'], language: ['cpp'])

# Generate protobuf library
gen = generator(protoc,
  output : ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments : ['--proto_path=../code/proto', '--cpp_out=@BUILD_DIR@', '@INPUT@', '--experimental_allow_proto3_optional']) # Generalize path?
generated = gen.process(['code/proto/ringclient.proto'])

inc = include_directories('code/include/')
subdir('code/include')
subdir('code/src')
subdir('experiments/unit_tests')

pkg_mod = import('pkgconfig')
pkg_mod.generate(libraries : pringleslib,
                 version : '1.0',
                 name : 'libpringles',
                 filebase : 'pringles',
                 description : 'Testing Meson for Pringles')
