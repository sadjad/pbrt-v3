#ifndef PBRT_CLOUD_SCHEDULER_H
#define PBRT_CLOUD_SCHEDULER_H

#include <chrono>
#include <cstdint>
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "lambda.h"

namespace pbrt {
    struct TreeletScheduler {
        public:
            // return an allocation
            std::map<TreeletId, uint32_t> rebalanceTreelets(WorkerStats& stats, std::chrono::time_point currentTime);
        private:
            // should we rebalance or not?
            bool rebalanceDecision(WorkerStats& stats, std::chrono::time_point, currentTime);
    };
} // namespace pbrt


#endif /* PBRT_CLOUD_SCHEDULER*/
