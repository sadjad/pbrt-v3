#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include <deque>
#include <istream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <stdexcept>
#include <vector>

#include "material.h"
#include "paramset.h"
#include "pbrt.h"
#include "pbrt/raystate.h"
#include "primitive.h"
#include "transform.h"

namespace pbrt {

struct TreeletNode;
class TriangleMesh;

class PlaceholderMaterial : public Material {
  public:
    PlaceholderMaterial(const uint32_t material_id)
        : material_id(material_id) {}

    void ComputeScatteringFunctions(SurfaceInteraction *si, MemoryArena &arena,
                                    TransportMode mode,
                                    bool allowMultipleLobes) const {
        throw std::runtime_error(
            "PlaceholderMaterial::ComputeScatteringFunctions: not implemented");
    }

    static void Bump(const std::shared_ptr<Texture<Float>> &d,
                     SurfaceInteraction *si);

    MaterialType GetType() const { return MaterialType::Placeholder; }

    uint32_t GetMaterialId() const { return material_id; }

  private:
    uint32_t material_id;
};

class CloudBVH : public Aggregate {
  public:
    struct TreeletInfo {
        std::set<uint32_t> children{};
        std::map<uint32_t, uint64_t> instances{};
    };

    CloudBVH(const uint32_t bvh_root, const bool preload_all,
             const bool load_materials);

    ~CloudBVH();

    CloudBVH(const CloudBVH &) = delete;
    CloudBVH &operator=(const CloudBVH &) = delete;

    Bounds3f WorldBound() const;
    Float RootSurfaceAreas(Transform txfm = Transform()) const;
    Float SurfaceAreaUnion() const;

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

    bool Intersect(const Ray &ray, SurfaceInteraction *isect, uint32_t) const;
    bool IntersectP(const Ray &ray, uint32_t) const;

    void Trace(RayState &rayState) const;

    void LoadTreelet(const uint32_t root_id, const char *buffer = nullptr,
                     const size_t length = 0) const;

    const TreeletInfo &GetInfo(const uint32_t treelet_id) {
        throw std::runtime_error("not implemented");
    }

    struct TreeletNode {
        Bounds3f bounds{};
        uint8_t axis{};

        union {
            struct {
                uint16_t child_treelet[2];
                uint32_t child_node[2];
            };
            struct {
                uint32_t leaf_tag{0};
                uint32_t primitive_offset{0};
                uint32_t primitive_count{0};
            };
        };

        TreeletNode() {}

        TreeletNode(const Bounds3f &bounds, const uint8_t axis)
            : bounds(bounds), axis(axis) {}

        bool is_leaf() const { return leaf_tag == ~0; }
    };

  private:
    enum Child { LEFT = 0, RIGHT = 1 };

    struct UnfinishedTransformedPrimitive {
        size_t primitive_index;
        uint64_t instance_ref;
        uint16_t instance_group;
        AnimatedTransform primitive_to_world;

        UnfinishedTransformedPrimitive(const size_t primitive_index,
                                       const uint64_t instance_ref,
                                       AnimatedTransform &&primitive_to_world)
            : primitive_index(primitive_index),
              instance_ref(instance_ref),
              primitive_to_world(std::move(primitive_to_world)) {}
    };

    struct UnfinishedGeometricPrimitive {
        size_t primitive_index;
        uint32_t material_id;
        uint32_t area_light_id;
        std::shared_ptr<Shape> shape;

        UnfinishedGeometricPrimitive(const size_t primitive_index,
                                     const uint32_t material_id,
                                     const uint32_t area_light_id,
                                     std::shared_ptr<Shape> &&shape)
            : primitive_index(primitive_index),
              material_id(material_id),
              area_light_id(area_light_id),
              shape(std::move(shape)) {}
    };

    struct Treelet {
        std::vector<TreeletNode> nodes{};
        std::vector<std::unique_ptr<Primitive>> primitives{};
        std::list<std::unique_ptr<Transform>> transforms{};
        std::map<uint64_t, std::shared_ptr<Primitive>> instances{};

        std::shared_ptr<char> mesh_storage{};
        std::map<uint32_t, std::shared_ptr<TriangleMesh>> meshes{};

        std::set<uint32_t> required_materials{};
        std::set<uint64_t> required_instances{};

        std::vector<UnfinishedTransformedPrimitive> unfinished_transformed{};
        std::vector<UnfinishedGeometricPrimitive> unfinished_geometric{};
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

    class ExternalInstance : public Aggregate {
      public:
        ExternalInstance(const CloudBVH &bvh, uint32_t root_id)
            : bvh_(bvh), root_id_(root_id) {}

        Bounds3f WorldBound() const { return bvh_.WorldBound(); }

        bool Intersect(const Ray &ray, SurfaceInteraction *isect) const {
            return bvh_.Intersect(ray, isect, root_id_);
        }

        bool IntersectP(const Ray &ray) const {
            return bvh_.IntersectP(ray, root_id_);
        }

        uint32_t RootID() { return root_id_; }

      private:
        uint32_t root_id_;
        const CloudBVH &bvh_;
    };

    const std::string bvh_path_;
    const uint32_t bvh_root_;
    bool preloading_done_{false};
    bool load_materials_{false};

    mutable std::vector<std::unique_ptr<Treelet>> treelets_;
    mutable std::map<uint64_t, std::shared_ptr<Primitive>> bvh_instances_;
    mutable std::map<uint32_t, std::shared_ptr<Material>> materials_;
    mutable std::map<uint64_t, std::shared_ptr<Texture<Float>>> ftex_;
    mutable std::map<uint64_t, std::shared_ptr<Texture<Spectrum>>> stex_;
    mutable std::map<uint32_t, std::pair<ParamSet, Transform>>
        area_light_params_;

    void finalizeTreeletLoad(const uint32_t root_id) const;
    void loadTreeletBase(const uint32_t root_id, const char *buffer = nullptr,
                         size_t length = 0) const;

    void clear() const;

    // returns array of Bounds3f with structure of Treelet's internal BVH
    // nodes
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
