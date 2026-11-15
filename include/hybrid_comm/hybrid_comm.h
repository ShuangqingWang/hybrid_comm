#ifndef SNOW_HYBRID_COMM_HYBRID_COMM_H
#define SNOW_HYBRID_COMM_HYBRID_COMM_H

/**
 * @file hybrid_comm.h
 * @brief hybrid_comm 唯一对外头文件：本机进程间的发布订阅与请求应答。
 * @details 消息类型决定底层通道：
 *          - 定义了 IOX2_TYPE_NAME 且可按字节拷贝的结构体：iceoryx2 共享内存；
 *          - Protobuf 消息：ZeroMQ。
 *          项目只包含本文件，禁止直接包含 detail 目录与 iceoryx2、ZeroMQ 的头文件。
 */

#include "hybrid_comm/common.h"
#include "hybrid_comm/reader.h"
#include "hybrid_comm/service.h"
#include "hybrid_comm/writer.h"

#endif // SNOW_HYBRID_COMM_HYBRID_COMM_H
