#pragma once
#include <k2/common/Log.h>

namespace k2::log {
    inline thread_local k2::logging::Logger hot("k2::hot");
}