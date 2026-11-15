cmake_minimum_required(VERSION 3.16)
project(hybrid_comm LANGUAGES CXX)

include(cmake/arch/${CMAKE_SYSTEM_PROCESSOR}.cmake)

option(BUILD_TESTS "编译测试程序" ON)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

set(IOX2_ROOT "${THIRDPARTY_DIR}/iceoryx2")
set(ZMQ_ROOT "${THIRDPARTY_DIR}/zeromq")

# ============================ 第三方库 ============================
# libiceoryx2_ffi_c.so 没有 SONAME，按 -l 方式链接，避免把构建机路径写入 NEEDED
foreach(_lib iceoryx2_ffi_c iceoryx2_cxx)
    add_library(hybrid_comm::${_lib} SHARED IMPORTED)
    set_target_properties(hybrid_comm::${_lib} PROPERTIES
        IMPORTED_LOCATION "${IOX2_ROOT}/lib/lib${_lib}.so"
        IMPORTED_NO_SONAME TRUE)
endforeach()

add_library(hybrid_comm_zmq SHARED IMPORTED)
set_target_properties(hybrid_comm_zmq PROPERTIES
    IMPORTED_LOCATION "${ZMQ_ROOT}/lib/libzmq.so.5"
    INTERFACE_INCLUDE_DIRECTORIES "${ZMQ_ROOT}/include")

# ============================ hybrid_comm 库 ============================
add_library(hybrid_comm SHARED
    src/common.cpp
    src/shm_common.cpp
    src/msg_channel.cpp
)
add_library(hybrid_comm::hybrid_comm ALIAS hybrid_comm)

target_include_directories(hybrid_comm PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${IOX2_ROOT}/include>
    $<INSTALL_INTERFACE:include>
)
# 模板实现用到 iceoryx2 与 Protobuf，因此对外公开；ZeroMQ 只在实现文件中使用
target_link_libraries(hybrid_comm
    PUBLIC
        hybrid_comm::iceoryx2_cxx
        hybrid_comm::iceoryx2_ffi_c
        protobuf::libprotobuf
        Threads::Threads
    PRIVATE
        hybrid_comm_zmq
)
# 使用 RPATH（不用 RUNPATH）：RUNPATH 对间接依赖无效，libiceoryx2_cxx.so 依赖的 libiceoryx2_ffi_c.so 会找不到。
# 引用方的可执行文件同样直接依赖 libiceoryx2_cxx.so，因此该选项对外公开。
target_link_options(hybrid_comm PUBLIC "LINKER:--disable-new-dtags")
set_target_properties(hybrid_comm PROPERTIES
    BUILD_RPATH "${IOX2_ROOT}/lib;${ZMQ_ROOT}/lib"
    INSTALL_RPATH "$ORIGIN"
)

# ============================ 安装 ============================
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS hybrid_comm EXPORT hybrid_commTargets
    LIBRARY DESTINATION lib
)
install(DIRECTORY include/ DESTINATION include)
install(DIRECTORY ${IOX2_ROOT}/include/ DESTINATION include)
# 运行时依赖的第三方库随包安装到 lib/
install(DIRECTORY ${IOX2_ROOT}/lib/ DESTINATION lib FILES_MATCHING PATTERN "*.so*")
install(DIRECTORY ${ZMQ_ROOT}/lib/ DESTINATION lib FILES_MATCHING PATTERN "*.so*")

install(EXPORT hybrid_commTargets
    NAMESPACE hybrid_comm::
    DESTINATION lib/cmake/hybrid_comm
)
file(READ "${PROJECT_SOURCE_DIR}/VERSION" APP_VERSION)
string(STRIP "${APP_VERSION}" APP_VERSION)
configure_package_config_file(
    cmake/config/hybrid_commConfig.cmake.in
    ${PROJECT_BINARY_DIR}/hybrid_commConfig.cmake
    INSTALL_DESTINATION lib/cmake/hybrid_comm
)
write_basic_package_version_file(
    ${PROJECT_BINARY_DIR}/hybrid_commConfigVersion.cmake
    VERSION ${APP_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${PROJECT_BINARY_DIR}/hybrid_commConfig.cmake
    ${PROJECT_BINARY_DIR}/hybrid_commConfigVersion.cmake
    DESTINATION lib/cmake/hybrid_comm
)

# ============================ 测试 ============================
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ============================= 打包 =============================
string(TIMESTAMP BUILD_TIMESTAMP "%Y%m%d%H%M%S" UTC)
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_FILE_NAME "${PROJECT_NAME}-${APP_VERSION}-${CMAKE_SYSTEM_PROCESSOR}-${BUILD_TIMESTAMP}")
# 产物为 .tar.gz，用 hybrid_pk install 安装（hybrid_pk 只接受 .tar.gz）
set(CPACK_GENERATOR "TGZ")
include(CPack)
