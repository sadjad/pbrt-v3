#ifndef PBRT_INCLUDE_COMMON_H
#define PBRT_INCLUDE_COMMON_H

namespace pbrt {

using TreeletId = uint32_t;
using ObjectID = size_t;

enum class ObjectType {
    Treelet,
    TriangleMesh,
    Lights,
    Sampler,
    Camera,
    Scene,
    Material,
    FloatTexture,
    SpectrumTexture,
    Manifest,
    Texture,
    TreeletInfo,
    StaticAssignment,
    COUNT
};

struct ObjectKey {
    ObjectType type;
    ObjectID id;

    bool operator<(const ObjectKey& other) const {
        if (type == other.type) {
            return id < other.id;
        }
        return type < other.type;
    }
};

}  // namespace pbrt

#endif /* PBRT_INCLUDE_COMMON_H */
