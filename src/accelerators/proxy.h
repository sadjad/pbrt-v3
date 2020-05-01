#ifndef PBRT_ACCELERATORS_PROXY_H
#define PBRT_ACCELERATORS_PROXY_H

#include <exception>

#include "primitive.h"

class ProxyBVH : public Aggregate {
public:
    ProxyBVH(const Bounds3f &bounds)
        : bounds_(bounds)
    {}

    Bounds3f WorldBound() const { return bounds_; }

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const {
        throw std::runtime_error("Intersect called on proxy");
    }
    bool IntersectP(const Ray &ray) const {
        throw std::runtime_error("IntersectP called on proxy");
    }
private:
    Bounds3f bounds_;
};

#endif
