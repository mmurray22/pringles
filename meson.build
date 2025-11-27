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
yaml_cpp = dependency('yaml-cpp')
protoc = find_program('protoc')
protobuf = dependency('protobuf')
spdlog = dependency('spdlog')
tbb_dep = dependency('tbb', required: true) 
add_project_link_arguments(['-lprotobuf'], language: ['cpp'])

# Generate protobuf library
gen = generator(protoc,
  output : ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments : ['--proto_path=../code/proto', '--cpp_out=@BUILD_DIR@', '@INPUT@', '--experimental_allow_proto3_optional']) # Generalize path?
generated = gen.process([
  'code/proto/ringclient.proto',
  'code/proto/corfuclient.proto',
  'code/proto/corfustorage.proto',
  'code/proto/corfusequencer.proto',
  'code/proto/simpleclient.proto',
  'code/proto/ringstorage.proto'
])

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
