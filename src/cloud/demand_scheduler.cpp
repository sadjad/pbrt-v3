#include "demand_scheduler.h"
#include <math.h>
#include "scheduler.h"

using namespace std;
using namespace std::chrono;

constexpr milliseconds REBALANCE_INTERVAL{5000};

namespace pbrt {

/* Returns an allocation based on the current allocation and worker statistics.
 */
Optional<Mapping> DemandScheduler::distributeTreelets(
    const Mapping currentMapping, WorkerStats& stats, uint32_t numWorkers) {
    /* for now: just rebalance every 5 seconds */
    if (lastRebalanced + REBALANCE_INTERVAL > now()) {
        lastRebalanced = now();
        return Optional<Mapping>();
    }

    uint32_t availableWorkers = numWorkers;

    /* calculate proportional demand of treelets that are needed */
    map<TreeletId, uint64_t> demandMap;
    uint64_t totalDemand = 0;

    for (auto& kv : stats.objectStats) {
        ObjectKey objectKey = kv.first;

        if (objectKey.type != ObjectType::Treelet) {
            continue;
        }

        RayStats objectStats = kv.second;
        uint64_t demand = objectStats.demandedRays - objectStats.finishedRays;

        if (demand == 0) {
            continue;
        }

        demandMap[objectKey.id] = demand;
        totalDemand += demand;
    }

    map<TreeletId, float> proportionalDemand;

    for (auto& kv : demandMap) {
        proportionalDemand[kv.first] =
            (float)(kv.second) / (float)(totalDemand);
    }

    Allocation allocation;

    for (auto& kv : demandMap) {
        /* Assign every treelet with unmet need 1 worker */
        allocation[kv.first] = 1;
        availableWorkers -= 1;
    }

    for (auto& kv : demandMap) {
        /* Assign the rest proportionally to the available workers */
        allocation[kv.first] +=
            (uint64_t)(proportionalDemand[kv.first] * float(availableWorkers));
    }

    Mapping newMapping = newAssignments(currentMapping, allocation);

    return {move(newMapping)};
}

/* Minimizes worker movement */
Mapping DemandScheduler::newAssignments(const Mapping currentMapping,
                                        const Allocation newAlloc) {
    Mapping newMapping;
    map<TreeletId, int32_t> allocationDif;
    Allocation currentAlloc = calculateAllocation(currentMapping);

    // calculate the change in workers for each treelet
    for (auto& kv : currentAlloc) {
        allocationDif[kv.first] = (int32_t)(kv.second) * (int32_t)(-1);
    }

    for (auto& kv : newAlloc) {
        allocationDif[kv.second] += (int32_t)(kv.second);
    }

    // iterate through the mapping (treelet -> set of workers) and take away
    // workers as needed

    return newMapping;
}

DemandScheduler::Allocation DemandScheduler::calculateAllocation(
    const Mapping mapping) {
    Allocation alloc;

    for (auto& kv : mapping) {
        TreeletId treelet = kv.first;

        // add the number of workers
        alloc[treelet] = kv.second.size();
    }

    return alloc;
}

}  // namespace pbrt
