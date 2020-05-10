#ifndef PBRT_ACCELERATORS_PROXY_H
#define PBRT_ACCELERATORS_PROXY_H

#include <exception>

#include "primitive.h"
#include "messages/serialization.h"

namespace pbrt {

class ProxyBVH : public Aggregate {
public:
    ProxyBVH(const Bounds3f &bounds, uint64_t size,
             const std::string &name, uint64_t nodeCount,
             std::vector<const ProxyBVH *> &&deps)
        : bounds_(bounds), size_(size),
          name_(name), nodeCount_(nodeCount),
          dependencies_(move(deps)),
          numIncludes_(1)
    {}

    Bounds3f WorldBound() const { return bounds_; }
    uint64_t Size() const { return size_; }
    std::string Name() const { return name_; }
    uint64_t nodeCount() const { return nodeCount_; }
    const std::vector<const ProxyBVH *> & Dependencies() const { return dependencies_; }
    std::vector<std::unique_ptr<protobuf::RecordReader>> GetReaders() const;
    uint64_t UsageCount() const { return numIncludes_; }
    void IncrUsage() { numIncludes_++; }

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const {
        throw std::runtime_error("Intersect called on proxy");
    }
    bool IntersectP(const Ray &ray) const {
        throw std::runtime_error("IntersectP called on proxy");
    }

private:
    Bounds3f bounds_;
    uint64_t size_;
    std::string name_;
    uint64_t nodeCount_;
    std::vector<const ProxyBVH *> dependencies_;
    uint64_t numIncludes_;
};

}

#endif
