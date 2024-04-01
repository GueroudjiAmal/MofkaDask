cmake_minimum_required (VERSION 3.8)

project (mofka CXX)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions (-Wextra -Wall -Wpedantic)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")

find_package (mofka REQUIRED)
find_package (PkgConfig REQUIRED)
pkg_check_modules (ssg REQUIRED IMPORTED_TARGET ssg)


add_library (my_validator  MODULE MyValidator.cpp)
target_link_libraries (my_validator  PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_serializer  MODULE MySerializer.cpp)
target_link_libraries (my_serializer PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_partition_selector MODULE MyPartitionSelector.cpp)
target_link_libraries (my_partition_selector PUBLIC mofka-client PRIVATE coverage_config warnings_config)
