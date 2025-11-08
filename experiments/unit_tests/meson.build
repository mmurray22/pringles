#net_serv = executable('net_serv', 'net_serv.cpp',
#                     include_directories : inc,
#                     dependencies: [yaml_cpp, spdlog],
#                     link_with : pringleslib)
#net_cli = executable('net_cli', 'net_cli.cpp',
#                     include_directories : inc,
#                     dependencies: [yaml_cpp, spdlog],
#                     link_with : pringleslib)
pringles_cli = executable('cli', 'pringles_cli.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
pringles_stor = executable('stor', 'pringles_stor.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
pringles_cli = executable('net_cli', 'net_cli.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)
pringles_stor = executable('net_stor', 'net_stor.cpp',
                          include_directories : inc,
                          dependencies: [yaml_cpp, spdlog, tbb_dep],
                          link_with : pringleslib)


#test('pringleslib test', net_serv, args: ['/home/micahrocks/Programming/ringlog/experiments/yaml/unit_test_datagram.yaml'])
