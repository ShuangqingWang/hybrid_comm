#!/bin/bash
# hybrid_comm 编译脚本
# 用法: bash scripts/build_tar.sh [--clean] [--package] [-D...]
#   --clean    删除 build 目录后重新配置
#   --package  编译后执行 make package 生成 .tar.gz 发布包
#   -D...      透传给 cmake，例如 -DCMAKE_BUILD_TYPE=Release -DTHIRDPARTY_DIR=/path

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT_DIR="$(dirname "$SCRIPT_DIR")"

BUILD_DIR="$PROJECT_ROOT_DIR/build"
CMAKE_ARGS=""
DO_PACKAGE=0

for arg in "$@"; do
    case $arg in
        --clean)
            echo "Cleaning build directory..."
            rm -rf "$BUILD_DIR"
            ;;
        --package)
            DO_PACKAGE=1
            ;;
        -D*)
            CMAKE_ARGS="$CMAKE_ARGS $arg"
            ;;
        *)
            echo "未知参数: $arg"
            exit 1
            ;;
    esac
done

set -e
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake .. $CMAKE_ARGS
TOTAL_CORES=$(nproc)
BUILD_JOBS=$(( TOTAL_CORES * 80 / 100 ))
[ "$BUILD_JOBS" -lt 1 ] && BUILD_JOBS=1
echo "Using $BUILD_JOBS / $TOTAL_CORES cores for build"
cmake --build . --parallel "$BUILD_JOBS"

if [ "$DO_PACKAGE" -eq 1 ]; then
    make package
fi
