# Creates SOURCES variable which contains all important source files
set(SOURCES client/*.cpp)

# Creates code library
add_library(code ${SOURCES})

# Fetch external library - yaml
include(FetchContent)

FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG yaml-cpp-0.8.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
)
FetchContent_MakeAvailable(yaml-cpp)

# Link the code library 
target_link_libraries(simple PUBLIC yaml-cpp::yaml-cpp) # The library or executable that require yaml-cpp library
target_link_libraries(simple PUBLIC code)

target_include_directories(simple PUBLIC
	"${PROJECT_BINARY_DIR}"
	"${PROJECT_SOURCE_DIR}/code"
	)
