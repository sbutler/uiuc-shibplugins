file(GLOB UIUC_SHIBPLUGINS_STORE_TOOL_SOURCE "source/*.cpp")

add_executable(${PROJECT_NAME}-store ${UIUC_SHIBPLUGINS_STORE_TOOL_SOURCE})
target_compile_definitions(${PROJECT_NAME}-store PUBLIC
    -DDYNAMODB_LIB_NAME="$<TARGET_FILE_NAME:${PROJECT_NAME}>"
)
target_include_directories(${PROJECT_NAME}-store PUBLIC
    ${Boost_INCLUDE_DIR}
    ${XercesC_INCLUDE_DIR}
    ${XMLTOOLING_INCLUDE_DIRS}
    ${LOG4SHIB_INCLUDE_DIRS}

    aws-cpp-sdk-core
)
target_link_libraries(${PROJECT_NAME}-store PUBLIC
    ${Boost_LIBRARIES}
    ${XercesC_LIBRARY}
    ${XMLTOOLING_LIBRARIES_ABS}
    ${LOG4SHIB_LIBRARIES_ABS}

    aws-cpp-sdk-core
)
install(TARGETS ${PROJECT_NAME}-store
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
)
