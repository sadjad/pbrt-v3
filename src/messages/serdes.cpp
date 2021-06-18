#include "serdes.h"

using namespace std;

namespace pbrt::serdes {

namespace triangle_mesh {

size_t serialized_length(const TriangleMesh& tm) {
    size_t len = sizeof(int) /* nTriangles */ + sizeof(int) /* nVertices */ +
                 sizeof(int) * 3 * tm.nTriangles /* vertexIndices */ +
                 sizeof(tm.p[0]) * tm.nVertices /* p */ +
                 sizeof(bool) * 4 /* has n, s, uv and faceIndices */ +
                 (tm.n ? sizeof(tm.n[0]) * tm.nVertices : 0) +
                 (tm.s ? sizeof(tm.s[0]) * tm.nVertices : 0) +
                 (tm.uv ? sizeof(tm.uv[0]) * tm.nVertices : 0) +
                 (tm.faceIndices ? sizeof(int) * tm.nTriangles : 0);

    return len;
}

void do_copy(string& dst, size_t& offset, const void* src, const size_t len) {
    if (offset + len > dst.size()) {
        throw runtime_error("out of bounds");
    }

    memcpy(&dst[0] + offset, reinterpret_cast<const char*>(src), len);
    offset += len;
}

string serialize(const TriangleMesh& tm) {
    string output;

    const auto output_len = serialized_length(tm);
    output.resize(output_len);

    size_t offset = 0;

    bool has_n = (tm.n != nullptr);
    bool has_s = (tm.s != nullptr);
    bool has_uv = (tm.uv != nullptr);
    bool has_face_indices = (tm.faceIndices != nullptr);

    do_copy(output, offset, &tm.nTriangles, sizeof(int));
    do_copy(output, offset, &tm.nVertices, sizeof(int));
    do_copy(output, offset, tm.vertexIndices, sizeof(int) * tm.nTriangles * 3);

    do_copy(output, offset, tm.p, sizeof(tm.p[0]) * tm.nVertices);

    do_copy(output, offset, &has_n, sizeof(bool));

    if (has_n) {
        do_copy(output, offset, tm.n, sizeof(tm.n[0]) * tm.nVertices);
    }

    do_copy(output, offset, &has_s, sizeof(bool));

    if (has_s) {
        do_copy(output, offset, tm.s, sizeof(tm.s[0]) * tm.nVertices);
    }

    do_copy(output, offset, &has_uv, sizeof(bool));

    if (has_uv) {
        do_copy(output, offset, tm.uv, sizeof(tm.uv[0]) * tm.nVertices);
    }

    do_copy(output, offset, &has_face_indices, sizeof(bool));

    if (has_face_indices) {
        do_copy(output, offset, tm.faceIndices, sizeof(int) * tm.nTriangles);
    }

    if (offset != output_len) {
        throw runtime_error("offset != output_len");
    }

    return output;
}

}  // namespace triangle_mesh

}  // namespace pbrt::serdes
