cmake_minimum_required(VERSION 3.16)
project(sqlite_gcs_vfs CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable testing
enable_testing()

# Find dependencies
find_package(absl CONFIG REQUIRED COMPONENTS status statusor str_format)



# Fetch google-cloud-cpp version 3.5.0 since host vcpkg only has version 2.37.0
include(FetchContent)
set(GOOGLE_CLOUD_CPP_ENABLE_ALL OFF CACHE BOOL "" FORCE)
set(GOOGLE_CLOUD_CPP_ENABLE_STORAGE ON CACHE BOOL "" FORCE)
set(GOOGLE_CLOUD_CPP_ENABLE_STORAGE_GRPC ON CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(GOOGLE_CLOUD_CPP_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)

message(STATUS "Fetching Google Cloud C++ SDK version 3.5.0 from GitHub...")
FetchContent_Declare(
    google_cloud_cpp
    URL https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v3.5.0.tar.gz
)
FetchContent_MakeAvailable(google_cloud_cpp)

# Find SQLite3 (with vcpkg unofficial-sqlite3 support and standard fallback)
find_package(unofficial-sqlite3 CONFIG QUIET)
if(TARGET unofficial::sqlite3::sqlite3)
    add_library(SQLite3::SQLite3 ALIAS unofficial::sqlite3::sqlite3)
else()
    find_package(SQLite3 REQUIRED)
    if(NOT TARGET SQLite3::SQLite3)
        add_library(SQLite3::SQLite3 UNKNOWN IMPORTED)
        set_target_properties(SQLite3::SQLite3 PROPERTIES
            IMPORTED_LOCATION "${SQLite3_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${SQLite3_INCLUDE_DIRS}"
        )
    endif()
endif()

# Core library
add_library(sqlite3_appendonly_vfs STATIC
    block_mapper.cc
    local_storage.cc
    gcs_storage.cc
    vfs_backend.cc
)

target_include_directories(sqlite3_appendonly_vfs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(sqlite3_appendonly_vfs PUBLIC
    SQLite3::SQLite3
    absl::status
    absl::statusor
    absl::str_format
)

target_link_libraries(sqlite3_appendonly_vfs PUBLIC
    google-cloud-cpp::storage
    google-cloud-cpp::storage_grpc
)

# CLI binary
add_executable(sqlite3_cli sqlite3_cli.cc)
target_link_libraries(sqlite3_cli PRIVATE sqlite3_appendonly_vfs)

# GoogleTest setup
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Common test linker targets
function(add_vfs_test test_name)
    add_executable(${test_name} ${test_name}.cc)
    target_link_libraries(${test_name} PRIVATE
        sqlite3_appendonly_vfs
        gtest_main
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
endfunction()

add_vfs_test(block_mapper_test)
add_vfs_test(storage_test)
add_vfs_test(vfs_test)

# gcs_storage_test requires mock objects (optional)
if(TARGET google-cloud-cpp::storage_mocks)
    add_executable(gcs_storage_test gcs_storage_test.cc)
    target_link_libraries(gcs_storage_test PRIVATE
        sqlite3_appendonly_vfs
        google-cloud-cpp::storage_mocks
        gtest_main
    )
    add_test(NAME gcs_storage_test COMMAND gcs_storage_test)
else()
    message(STATUS "google-cloud-cpp storage mocks target not found; skipping gcs_storage_test.")
endif()
