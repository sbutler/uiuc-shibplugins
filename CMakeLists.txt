cmake_minimum_required(VERSION 3.1.0)
project(shib-dynamodb-store)
set(CMAKE_CXX_STANDARD 11)

function(build_aws_sdk_cpp)
    set(ENABLE_UNITY_BUILD ON CACHE BOOL "AWS SDK: enable smaller builds" FORCE)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "AWS SDK: don't build shared libraries" FORCE)
    set(SIMPLE_INSTALL ON CACHE BOOL "AWS SDK: simple install path" FORCE)
    set(BUILD_ONLY "dynamodb" CACHE STRING "AWS SDK: only build specific modules" FORCE)

    add_subdirectory("aws-sdk-cpp")
endfunction(build_aws_sdk_cpp)

build_aws_sdk_cpp()

file(GLOB SHIB_DYNAMODB_STORE_SOURCE "source/*.cpp")

find_package(Boost COMPONENTS program_options REQUIRED)
find_package(XercesC REQUIRED)

include(FindPkgConfig)
pkg_check_modules(XMLTOOLING REQUIRED xmltooling>=3.0.0)
set(XMLTOOLING_LIBARIES_ABS)
foreach(lib ${XMLTOOLING_LIBRARIES})
    set(var_name XMLTOOLING_LIBRARIES_ABS_${lib})
    find_library(${var_name} ${lib} ${XMLTOOLING_LIBRARY_DIR})
    list(APPEND XMLTOOLING_LIBRARIES_ABS ${${var_name}})
endforeach()

pkg_check_modules(LOG4SHIB REQUIRED log4shib>=2.0.0)
set(LOG4SHIB_LIBARIES_ABS)
foreach(lib ${LOG4SHIB_LIBRARIES})
    set(var_name LOG4SHIB_LIBRARIES_ABS_${lib})
    find_library(${var_name} ${lib} ${LOG4SHIB_LIBRARY_DIR})
    list(APPEND LOG4SHIB_LIBRARIES_ABS ${${var_name}})
endforeach()

add_library(${PROJECT_NAME} SHARED ${SHIB_DYNAMODB_STORE_SOURCE})
target_include_directories(${PROJECT_NAME} PUBLIC
    aws-cpp-sdk-core
    aws-cpp-sdk-dynamodb
    ${Boost_INCLUDE_DIR}
    ${XercesC_INCLUDE_DIR}
    ${XMLTOOLING_INCLUDE_DIRS}
    ${LOG4SHIB_INCLUDE_DIRS}
)
target_link_libraries(${PROJECT_NAME} PUBLIC
    aws-cpp-sdk-core
    aws-cpp-sdk-dynamodb
    ${Boost_LIBRARIES}
    ${XercesC_LIBRARY}
    ${XMLTOOLING_LIBRARIES_ABS}
    ${LOG4SHIB_LIBRARIES_ABS}
)

add_subdirectory("dynamodb-store-tool")
