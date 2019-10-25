#include "scheduler.h"

#include <math.h>

using namespace std;
using namespace chrono;

namespace pbrt {
    
    bool TreeletScheduler::rebalanceDecision(WorkerStats& stats, std::chrono::time_point currentTime) {
        return false;
    }

    std::map<TreeletId, uint32_t> TreeletScheduler::rebalanceTreelets(WorkerStats& stats, std::chrono::time_point currentTime) {
        std::map<TreeletId, uint32_t> allocation;

        /* return empty allocation if there is no need to schedule currently */
        if (!rebalanceDecision(stats, currentTime)) {
            return allocation
        }
    }

} // namespace pbrt


