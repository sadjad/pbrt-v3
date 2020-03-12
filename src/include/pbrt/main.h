#ifndef PBRT_INCLUDE_MAIN_H
#define PBRT_INCLUDE_MAIN_H

#include <cstdint>
#include <memory>
#include <vector>

#include "pbrt.h"

namespace pbrt {

class GlobalSampler;
class CloudBVH;
class Sample;
class RayState;
using RayStatePtr = std::unique_ptr<RayState>;

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

}  // namespace graphics

}  // namespace pbrt

#endif /* PBRT_CLOUD_GRAPHICS_H */
