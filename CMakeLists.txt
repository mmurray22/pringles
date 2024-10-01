cmake_minimum_required(VERSION 3.30.3)

# set the project name
project(pringles)

# specify the c++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# compile protobuf
execute_process(COMMAND bash -c "protoc --proto_path=${PROJECT_SOURCE_DIR}/proto --cpp_out=${PROJECT_SOURCE_DIR}/proto ${PROJECT_SOURCE_DIR}/proto/ringclient.proto")


# add subdirectories
add_subdirectory(code)

# add the executable
add_executable(simple experiments/unit_tests/simple_client.cpp)

# Link the code library 
target_link_libraries(simple PUBLIC code)

target_include_directories(simple PUBLIC
	"${PROJECT_BINARY_DIR}"
	"${PROJECT_SOURCE_DIR}/code"
	)
