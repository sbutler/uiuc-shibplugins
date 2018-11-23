cmake_minimum_required(VERSION 3.1.0)
project(shib-dynamodb-store)
set(CMAKE_CXX_STANDARD 11)

include(ExternalProject)
ExternalProject_Add(AWS_SDK_CPP
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG 1.7.2
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/external-install
    CMAKE_CACHE_ARGS
        -DENABLE_UNITY_BUILD:BOOL=ON
        -DBUILD_SHARED_LIBS:BOOL=OFF
        -DSIMPLE_INSTALL:BOOL=ON
        -DBUILD_ONLY:STRING=dynamodb
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/external-build/aws-sdk-cpp
    BUILD_IN_SOURCE 0
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/external-install
)
set(AWS_SDK_CPP_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/external-install/include)
set(AWS_SDK_CPP_STATICLIBRARY_core ${CMAKE_CURRENT_BINARY_DIR}/external-install/lib/libaws-cpp-sdk-core.a)
set(AWS_SDK_CPP_STATICLIBRARY_dynamodb ${CMAKE_CURRENT_BINARY_DIR}/external-install/lib/libaws-cpp-sdk-dynamodb.a)

file(GLOB SHIB_DYNAMODB_STORE_SOURCE
    "source/aws_sdk/core/utils/logging/*.cpp"
    "source/xmltooling/*.cpp"
)
file(GLOB SHIB_DYNAMODB_STORE_HEADERS
    "include/uiuc/aws_sdk/core/utils/logging/*.h"
    "include/uiuc/xmltooling/*.h"
)

find_package(Boost COMPONENTS program_options REQUIRED)
find_package(CURL REQUIRED)
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
add_dependencies(${PROJECT_NAME} AWS_SDK_CPP)
target_include_directories(${PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${AWS_SDK_CPP_INCLUDE_DIR}
    ${Boost_INCLUDE_DIR}
    ${XercesC_INCLUDE_DIR}
    ${XMLTOOLING_INCLUDE_DIRS}
    ${LOG4SHIB_INCLUDE_DIRS}
)
target_link_libraries(${PROJECT_NAME} PUBLIC
    ${AWS_SDK_CPP_STATICLIBRARY_core}
    ${AWS_SDK_CPP_STATICLIBRARY_dynamodb}
    ${Boost_LIBRARIES}
    ${CURL_LIBRARIES}
    ${XercesC_LIBRARY}
    ${XMLTOOLING_LIBRARIES_ABS}
    ${LOG4SHIB_LIBRARIES_ABS}
)
install(TARGETS ${PROJECT_NAME}
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
)

add_subdirectory("dynamodb-store-tool")
