#ifndef PBRT_ACCELERATORS_PROXY_H
#define PBRT_ACCELERATORS_PROXY_H

#include <exception>

#include "primitive.h"

namespace pbrt {

class ProxyBVH : public Aggregate {
public:
    ProxyBVH(const Bounds3f &bounds, uint64_t size)
        : bounds_(bounds), size_(size)
    {}

    Bounds3f WorldBound() const { return bounds_; }
    uint64_t Size() const { return size_; }

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const {
        throw std::runtime_error("Intersect called on proxy");
    }
    bool IntersectP(const Ray &ray) const {
        throw std::runtime_error("IntersectP called on proxy");
    }
private:
    Bounds3f bounds_;
    uint64_t size_;
};

}

#endif
