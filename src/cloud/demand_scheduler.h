#ifndef DEMAND_SCHEDULER_H
#define DEMAND_SCHEDULER_H

#include <chrono>
#include "scheduler.h"
#include "util/optional.h"

namespace pbrt {

class DemandScheduler : public Scheduler {
  public:
    Optional<Mapping> distributeTreelets(const Mapping currentMapping,
                                         WorkerStats& stats,
                                         uint32_t numWorkers) override;
    ~DemandScheduler() {}

  private:
    /* each worker has at most 1 treelet */
    using Allocation = std::map<TreeletId, uint32_t>;

    /* mapping of treelet id to the number of workers assigned */
    Mapping newAssignments(const Mapping currentMapping,
                           const Allocation allocation);

    Allocation calculateAllocation(const Mapping mapping);
    timepoint_t lastRebalanced{now()};
};

}  // namespace pbrt

#endif /* DEMAND_SCHEDULER */
