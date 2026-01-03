pringles_cli = executable('net_cli', 'pringles_cli.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
#pringles_stor = executable('stor', 'pringles_stor.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)
#pringles_cli = executable('net_cli', 'net_cli.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)
pringles_stor = executable('net_stor', 'net_stor.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
pringles_seq = executable('net_seq', 'net_seq.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
#pringles_stor = executable('raw_cli', 'benchmark_raw_cli.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)
#pringles_seq = executable('raw_serv', 'benchmark_raw_serv.cpp',
#                          include_directories : inc,
#                          dependencies: [yaml_cpp, spdlog, tbb_dep],
#                          link_with : pringleslib)





#test('pringleslib test', net_serv, args: ['/home/micahrocks/Programming/ringlog/experiments/yaml/unit_test_datagram.yaml'])
