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

}  // namespace pbrt::serdes

#endif /* PBRT_MESSAGES_NEW_UTILS_H */
