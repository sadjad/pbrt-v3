#ifndef PBRT_CLOUD_SCHEDULER_H
#define PBRT_CLOUD_SCHEDULER_H

#include <cstdint>
#include "util/optional.h"
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "lambda.h"

namespace pbrt {
    using Mapping = std::map<TreeletId, std::set<uint32_t>>;
    class Scheduler {
        public:
            Scheduler() {};
            virtual Optional<Mapping> distributeTreelets(const Mapping currentMapping, WorkerStats& stats, uint32_t numWorkers) = 0;
            virtual ~Scheduler() {}
    };
} // namespace pbrt

#endif /* PBRT_CLOUD_SCHEDULER*/
