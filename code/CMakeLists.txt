# Creates SOURCES variable which contains all important source files
set(SOURCES client/base_client.cpp client/api.cpp)

# Creates code library
add_library(code ${SOURCES})
