/*
 * @Author: chunyuLi 
 * @Date: 2022-09-13 14:54:37 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 15:12:41
 */

#pragma once

#include <memory>

#include "glog/logging.h"
#include "utils/config.hpp"
#include "utils/unit.hpp"

class RemoteBuffer {
 public:
    RemoteBuffer() {
        config_ = Config::GetInstance();
        remote_buffer_ = new char[config_->remote_buffer_sz];
        memset(remote_buffer_, 0, config_->remote_buffer_sz);

        config_->vtx_store = remote_buffer_ + config_->vertex_offset;
        config_->kvstore = remote_buffer_ + config_->kvstore_offset;
    }

    ~RemoteBuffer() {
        delete[] remote_buffer_;
    }

    inline char* GetBuf() {
        return remote_buffer_;
    }

    inline uint64_t GetRemoteBufSize() {
        return config_->remote_buffer_sz;
    }

 private:
    // layout: (kv-store) | send_buffer | recv_buffer | local_head_buffer | remote_head_buffer
    char* remote_buffer_; 
    Config* config_;
    // Node & node_;
};
