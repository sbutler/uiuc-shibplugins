file(GLOB SHIB_DYNAMODB_STORE_TOOL_SOURCE "source/*.cpp")

add_executable(${PROJECT_NAME}-tool ${SHIB_DYNAMODB_STORE_TOOL_SOURCE})
target_compile_definitions(${PROJECT_NAME}-tool PUBLIC
    -DDYNAMODB_LIB_NAME="$<TARGET_FILE_NAME:${PROJECT_NAME}>"
)
target_include_directories(${PROJECT_NAME}-tool PUBLIC
    ${Boost_INCLUDE_DIR}
    ${XercesC_INCLUDE_DIR}
    ${XMLTOOLING_INCLUDE_DIRS}
    ${LOG4SHIB_INCLUDE_DIRS}
)
target_link_libraries(${PROJECT_NAME}-tool PUBLIC
    ${Boost_LIBRARIES}
    ${XercesC_LIBRARY}
    ${XMLTOOLING_LIBRARIES_ABS}
    ${LOG4SHIB_LIBRARIES_ABS}
)
install(TARGETS ${PROJECT_NAME}-tool)
