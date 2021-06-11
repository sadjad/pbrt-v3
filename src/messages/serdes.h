#ifndef PBRT_MESSAGES_NEW_UTILS_H
#define PBRT_MESSAGES_NEW_UTILS_H

#include <memory>
#include <string>

#include "accelerators/cloud.h"
#include "shapes/triangle.h"

namespace pbrt::serdes {

namespace triangle_mesh {

std::string serialize(const TriangleMesh& tm);

TriangleMesh deserialize(const std::string& input);

}  // namespace triangle_mesh

namespace cloudbvh {

struct Node {
    Bounds3f bounds;

    struct __attribute__((packed, aligned(1))) {
        int64_t left_ref;
        int64_t right_ref;
        uint8_t axis;

        uint16_t transformed_primitives_count{0};
        uint16_t triangles_count{0};
    };
};

struct TransformedPrimitive {
    Matrix4x4 start_transform;
    Matrix4x4 end_transform;

    struct __attribute__((packed, aligned(1))) {
        Float start_time;
        Float end_time;
        int64_t root_ref;
    };
};

struct __attribute__((packed, aligned(1))) Triangle {
    uint32_t mesh_id;
    uint32_t tri_number;
};

}  // namespace cloudbvh

}  // namespace pbrt::serdes

#endif /* PBRT_MESSAGES_NEW_UTILS_H */
