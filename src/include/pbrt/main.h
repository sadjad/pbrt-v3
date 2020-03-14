#ifndef PBRT_INCLUDE_MAIN_H
#define PBRT_INCLUDE_MAIN_H

#include <cstdint>
#include <memory>
#include <set>
#include <vector>

#include "common.h"
#include "geometry.h"
#include "pbrt.h"

namespace pbrt {

class GlobalSampler;
class CloudBVH;
class Sample;
class RayState;
using RayStatePtr = std::unique_ptr<RayState>;

namespace scene {

class Base {
  private:
    std::vector<std::set<ObjectKey>> treeletDependencies{};

  public:
    std::shared_ptr<Camera> camera{};
    std::shared_ptr<GlobalSampler> sampler{};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::unique_ptr<Scene> fakeScene{};
    std::vector<std::shared_ptr<Light>> lights{};

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
                                      const TreeletId treeletId);

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

void WriteImage(const std::shared_ptr<Camera> &camera);

}  // namespace graphics

}  // namespace pbrt

#endif /* PBRT_CLOUD_GRAPHICS_H */
