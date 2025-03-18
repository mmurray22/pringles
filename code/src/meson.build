#pringles_sources = ['base_client.cpp', 'base_sequencer.cpp', 'pringles_client.cpp', 'RingStorage.cpp', 'simple_client.cpp', 'utils.cpp']
pringles_sources = ['network.cpp', 'utils.cpp', 'structs.cpp', 'trace.cpp', 'statistics.cpp']
pringleslib = shared_library('logging', 
  pringles_sources, 
  generated, 
  include_directories : inc, 
  dependencies : [yaml_cpp, spdlog], 
  install : true)
