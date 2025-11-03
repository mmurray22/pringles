net_serv = executable('net_serv', 'net_serv.cpp',
                     include_directories : inc,
                     dependencies: [yaml_cpp, spdlog],
                     link_with : pringleslib)
net_cli = executable('net_cli', 'net_cli.cpp',
                     include_directories : inc,
                     dependencies: [yaml_cpp, spdlog],
                     link_with : pringleslib)
test_sequencer = executable('test_sequencer', 'test_sequencer.cpp',
                     include_directories : inc,
                     dependencies: [yaml_cpp, spdlog],
                     link_with : pringleslib)

test('pringleslib test', test_sequencer, args: ['/root/name/pringles/experiments/yaml/test_sequencer.yaml'])
