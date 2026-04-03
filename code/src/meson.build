#pringles_sources = ['base_client.cpp', 'base_sequencer.cpp', 'pringles_client.cpp', 'RingStorage.cpp', 'simple_client.cpp', 'utils.cpp']
pringles_sources = ['network.cpp', 'utils.cpp', 'trace.cpp', 'measure.cpp', 'pringles_client.cpp', 'pringles_storage.cpp', 'pringles_software_switch.cpp']
pringleslib = static_library('logging', 
  pringles_sources, 
  generated, 
  include_directories : inc, 
  dependencies : [yaml_cpp, spdlog, tbb_dep], 
  install : true)
