#ifndef PBRT_CLOUD_SCHEDULERS_DYNAMIC_H
#define PBRT_CLOUD_SCHEDULERS_DYNAMIC_H

#include <string>

#include "cloud/allocator.h"
#include "cloud/scheduler.h"
#include "cloud/schedulers/static.h"

namespace pbrt {

class DynamicScheduler : public Scheduler {
  private:
    enum Stage { INITIAL, ROOT_ONLY, STATIC };

    StaticScheduler staticScheduler;
    Stage stage{INITIAL};

  public:
    DynamicScheduler(const std::string &path);

    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &stats) override;
};

}  // namespace pbrt
#endif /* PBRT_CLOUD_SCHEDULERS_DYNAMIC_H */