#net_serv = executable('net_serv', 'net_serv.cpp',
#                     include_directories : inc,
#                     dependencies: [yaml_cpp, spdlog],
#                     link_with : pringleslib)
#net_cli = executable('net_cli', 'net_cli.cpp',
#                     include_directories : inc,
#                     dependencies: [yaml_cpp, spdlog],
#                     link_with : pringleslib)
#pringles_cli = executable('cli', 'pringles_cli.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_switch = executable('switch', 'pringles_switch.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_stor = executable('stor', 'pringles_stor.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_cli = executable('net_cli', 'net_cli.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_stor = executable('net_stor', 'net_stor.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)
corfu_seq = executable('test_sequencer', 'test_sequencer_new.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
corfu_simple = executable('test_corfu_simple', 'test_corfu_simple.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
corfu_impl = executable('test_corfu_impl', 'test_corfu_impl.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
corfu_cli = executable('corfu_cli', 'corfu_cli.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
corfu_stor = executable('corfu_stor', 'corfu_stor.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
corfu_seq = executable('corfu_seq', 'corfu_seq.cpp', generated,
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_seq = executable('net_seq', 'net_seq.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib,
#                          link_args: ['-static-libstdc++', '-static-libgcc'])
#pringles_stor = executable('raw_cli', 'benchmark_raw_cli.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)
#pringles_seq = executable('raw_serv', 'benchmark_raw_serv.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)





#test('pringleslib test', net_serv, args: ['/home/micahrocks/Programming/ringlog/experiments/yaml/unit_test_datagram.yaml'])
#test('corfu sequencer test', corfu_seq, args: ['/root/name/pringles/experiments/yaml/test_sequencer.yaml'])
#test('corfu simple test', corfu_simple, args: ['/root/name/pringles/experiments/yaml/corfu_client_simple.yaml', '/root/name/pringles/experiments/yaml/corfu_seq_simple.yaml', '/root/name/pringles/experiments/yaml/corfu_storage_simple.yaml'])
#test('corfu implementation test', corfu_impl, args: ['/proj/ove-PG0/colang/pringles/experiments/yaml/corfu_client_simple.yaml', '/proj/ove-PG0/colang/pringles/experiments/yaml/corfu_seq_simple.yaml', '/proj/ove-PG0/colang/pringles/experiments/yaml/corfu_storage_simple.yaml'])