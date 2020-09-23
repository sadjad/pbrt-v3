#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include <deque>
#include <istream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <vector>

#include "pbrt.h"
#include "pbrt/raystate.h"
#include "primitive.h"
#include "transform.h"

namespace pbrt {

struct TreeletNode;
class TriangleMesh;

class CloudBVH : public Aggregate {
  public:
    struct TreeletInfo {
        std::set<uint32_t> children{};
        std::map<uint32_t, uint64_t> instances {};
    };

    CloudBVH(const uint32_t bvh_root = 0, const bool preload_all = false);
    ~CloudBVH();

    CloudBVH(const CloudBVH &) = delete;
    CloudBVH &operator=(const CloudBVH &) = delete;

    Bounds3f WorldBound() const;
    Float RootSurfaceAreas(Transform txfm = Transform()) const;
    Float SurfaceAreaUnion() const;

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

    void Trace(RayState &rayState) const;
    bool Intersect(RayState &rayState, SurfaceInteraction *isect) const;

    void LoadTreelet(const uint32_t root_id, std::istream *stream) const {
        loadTreelet(root_id, stream);
    }

    const TreeletInfo &GetInfo(const uint32_t treelet_id) {
        throw std::runtime_error("Not implemented");
    }

    struct TreeletNode {
        Bounds3f bounds;
        uint8_t axis;

        union {
            struct {
                uint16_t child_treelet[2] = {0};
                uint32_t child_node[2] = {0};
            };
            struct {
                uint32_t leaf_tag;
                uint32_t primitive_offset;
                uint32_t primitive_count;
            };
        };

        TreeletNode(const Bounds3f &bounds, const uint8_t axis)
            : bounds(bounds), axis(axis) {}

        bool is_leaf() const { return leaf_tag == ~0; }
    };

  private:
    enum Child { LEFT = 0, RIGHT = 1 };

    struct Treelet {
        std::deque<TreeletNode> nodes{};
        std::deque<std::unique_ptr<Primitive>> primitives{};
        std::map<uint32_t, std::shared_ptr<TriangleMesh>> meshes;
        std::list<std::unique_ptr<Transform>> transforms;
        std::map<uint64_t, std::shared_ptr<Primitive>> instances;
    };

    class IncludedInstance : public Aggregate {
      public:
        IncludedInstance(const Treelet *treelet, int nodeIdx)
            : treelet_(treelet), nodeIdx_(nodeIdx) {}

        Bounds3f WorldBound() const;
        bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
        bool IntersectP(const Ray &ray) const;

      private:
        const Treelet *treelet_;
        int nodeIdx_;
    };

    const std::string bvh_path_;
    const uint32_t bvh_root_;
    const bool preload_;
    bool preloading_done_ {false};

    mutable std::map<uint32_t, std::unique_ptr<Treelet>> treelets_;
    mutable std::map<uint64_t, std::shared_ptr<Primitive>> bvh_instances_;
    mutable std::map<uint32_t, std::shared_ptr<Material>> materials_;

    mutable std::shared_ptr<Material> default_material;

    void loadTreelet(const uint32_t root_id,
                     std::istream *stream = nullptr) const;

    void clear() const;

    // returns array of Bounds3f with structure of Treelet's internal BVH nodes
    std::vector<Bounds3f> getTreeletNodeBounds(
        const uint32_t treelet_id, const int recursionLimit = 4) const;

    void recurseBVHNodes(const int depth, const int recursionLimit,
                         const int idx, const Treelet &currTreelet,
                         const TreeletNode &currNode,
                         std::vector<Bounds3f> &treeletBounds) const;

    Transform identity_transform_;
};

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps);

Vector3f ComputeRayDir(unsigned idx);
unsigned ComputeIdx(const Vector3f &dir);

}  // namespace pbrt

#endif /* PBRT_ACCELERATORS_CLOUD_BVH_H */
