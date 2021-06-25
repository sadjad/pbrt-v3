#ifndef PBRT_INCLUDE_MAIN_H
#define PBRT_INCLUDE_MAIN_H

#include <cstdint>
#include <istream>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "common.h"
#include "geometry.h"
#include "pbrt.h"
#include "transform.h"

namespace pbrt {

class TriangleMesh;
class GlobalSampler;
class CloudBVH;
class Sample;
class RayState;
using RayStatePtr = std::unique_ptr<RayState>;

struct AccumulatedStats {
    std::map<std::string, int64_t> counters{};
    std::map<std::string, int64_t> memoryCounters{};
    std::map<std::string, int64_t> intDistributionSums{};
    std::map<std::string, int64_t> intDistributionCounts{};
    std::map<std::string, int64_t> intDistributionMins{};
    std::map<std::string, int64_t> intDistributionMaxs{};
    std::map<std::string, double> floatDistributionSums{};
    std::map<std::string, int64_t> floatDistributionCounts{};
    std::map<std::string, double> floatDistributionMins{};
    std::map<std::string, double> floatDistributionMaxs{};
    std::map<std::string, std::pair<int64_t, int64_t>> percentages{};
    std::map<std::string, std::pair<int64_t, int64_t>> ratios{};

    void Merge(const AccumulatedStats &other);
};

namespace scene {

class Base {
  private:
    std::vector<std::set<ObjectKey>> treeletDependencies{};
    Transform identityTransform;

  public:
    std::shared_ptr<Camera> camera{};
    std::shared_ptr<GlobalSampler> sampler{};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::unique_ptr<Scene> fakeScene{};
    std::vector<std::shared_ptr<Light>> lights{};
    std::vector<std::shared_ptr<Light>> infiniteLights{};

    std::vector<std::shared_ptr<TriangleMesh>> areaLightMeshes{};
    std::vector<std::shared_ptr<Shape>> areaLightShapes{};

    int samplesPerPixel;
    pbrt::Bounds2i sampleBounds{};
    pbrt::Vector2i sampleExtent{};
    size_t totalPaths{0};

    Base();

    Base(Base &&);
    Base &operator=(Base &&);

    Base(const std::string &path, const int samplesPerPixel);
    ~Base();

    std::set<ObjectKey> &GetTreeletDependencies(const TreeletId treeletId) {
        return treeletDependencies.at(treeletId);
    }

    size_t GetTreeletCount() const { return treeletDependencies.size(); }
};

std::string GetObjectName(const ObjectType type, const uint32_t id);

Base LoadBase(const std::string &path, const int samplesPerPixel);

std::shared_ptr<CloudBVH> LoadTreelet(const std::string &path,
                                      const TreeletId treeletId,
                                      const char *buffer = nullptr,
                                      const size_t length = 0);

void DumpSceneObjects(const std::string &description,
                      const std::string outputPath);

}  // namespace scene

namespace graphics {

RayStatePtr TraceRay(RayStatePtr &&rayState, const CloudBVH &treelet);

std::pair<RayStatePtr, RayStatePtr> ShadeRay(
    RayStatePtr &&rayState, const CloudBVH &treelet,
    const std::vector<std::shared_ptr<Light>> &lights,
    const Vector2<int> &sampleExtent, std::shared_ptr<GlobalSampler> &sampler,
    int maxPathDepth, MemoryArena &arena);

RayStatePtr GenerateCameraRay(const std::shared_ptr<Camera> &camera,
                              const Point2<int> &pixel,
                              const uint32_t sample_num, const uint8_t maxDepth,
                              const Vector2<int> &sampleExtent,
                              std::shared_ptr<GlobalSampler> &sampler);

void AccumulateImage(const std::shared_ptr<Camera> &camera,
                     const std::vector<Sample> &rays);

void WriteImage(const std::shared_ptr<Camera> &camera,
                const std::string &filename = {});

}  // namespace graphics

namespace stats {

AccumulatedStats GetThreadStats();

}

}  // namespace pbrt

#endif /* PBRT_CLOUD_GRAPHICS_H */
