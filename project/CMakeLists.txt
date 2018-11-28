cmake_minimum_required(VERSION 3.1.0)
project(uiuc-shibplugins)
set(CMAKE_CXX_STANDARD 11)

set(AWS_SDK_CPP_PREFIX_PATH "" CACHE STRING "Path(s) to the AWS CPP SDK cmake config files.")
set(CMAKE_PREFIX_PATH ${AWS_SDK_CPP_PREFIX_PATH})

find_package(aws-cpp-sdk-core       REQUIRED)
find_package(aws-cpp-sdk-dynamodb   REQUIRED)

file(GLOB UIUC_SHIBPLUGINS_SOURCE
    "source/aws_sdk/core/utils/logging/*.cpp"
    "source/xmltooling/*.cpp"
)
file(GLOB UIUC_SHIBPLUGINS_HEADERS
    "include/uiuc/aws_sdk/core/utils/logging/*.h"
    "include/uiuc/xmltooling/*.h"
)

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

add_library(${PROJECT_NAME} SHARED ${UIUC_SHIBPLUGINS_SOURCE})
target_include_directories(${PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${Boost_INCLUDE_DIR}
    ${XercesC_INCLUDE_DIR}
    ${XMLTOOLING_INCLUDE_DIRS}
    ${LOG4SHIB_INCLUDE_DIRS}

    aws-cpp-sdk-core
    aws-cpp-sdk-dynamodb
)
target_link_libraries(${PROJECT_NAME} PUBLIC
    ${Boost_LIBRARIES}
    ${XercesC_LIBRARY}
    ${XMLTOOLING_LIBRARIES_ABS}
    ${LOG4SHIB_LIBRARIES_ABS}

    aws-cpp-sdk-core
    aws-cpp-sdk-dynamodb
)
install(TARGETS ${PROJECT_NAME}
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
)

add_subdirectory("store-tool")
