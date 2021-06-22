#ifndef PBRT_MESSAGES_NEW_UTILS_H
#define PBRT_MESSAGES_NEW_UTILS_H

#include <memory>
#include <string>

#include "accelerators/cloud.h"
#include "shapes/triangle.h"

namespace pbrt::serdes {

namespace triangle_mesh {

std::string serialize(const TriangleMesh& tm);

}  // namespace triangle_mesh

namespace cloudbvh {

struct TransformedPrimitive {
    Matrix4x4 start_transform;
    Matrix4x4 end_transform;

    struct __attribute__((packed, aligned(1))) {
        Float start_time;
        Float end_time;
        uint64_t root_ref;
    };
};

struct __attribute__((packed, aligned(1))) Triangle {
    uint32_t mesh_id;
    uint32_t tri_number;
};

}  // namespace cloudbvh

}  // namespace pbrt::serdes

#endif /* PBRT_MESSAGES_NEW_UTILS_H */
