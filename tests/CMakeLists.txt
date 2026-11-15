protobuf_generate_cpp(TEST_PROTO_SRCS TEST_PROTO_HDRS test_msgs.proto)

add_executable(hybrid_comm_test comm_test.cpp ${TEST_PROTO_SRCS})
target_include_directories(hybrid_comm_test PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(hybrid_comm_test PRIVATE hybrid_comm::hybrid_comm)

# iceoryx2 从工作目录下的 config/iceoryx2.toml 读取配置
configure_file(iceoryx2.toml ${CMAKE_CURRENT_BINARY_DIR}/config/iceoryx2.toml COPYONLY)
add_test(NAME hybrid_comm_test COMMAND hybrid_comm_test WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
