#include "serdes.h"

using namespace std;

namespace pbrt::serdes {

namespace triangle_mesh {

size_t serialized_length(const TriangleMesh& tm) {
    size_t len = sizeof(int64_t) /* id */ + sizeof(int64_t) /* material_id */ +
                 sizeof(int) /* nTriangles */ + sizeof(int) /* nVertices */ +
                 sizeof(int) * 3 * tm.nTriangles /* vertexIndices */ +
                 sizeof(tm.p[0]) * tm.nVertices /* p */ +
                 sizeof(bool) * 3 /* has n, s and us */;

    if (tm.n) {
        len += sizeof(tm.n[0]) * tm.nVertices;
    }

    if (tm.s) {
        len += sizeof(tm.s[0]) * tm.nVertices;
    }

    if (tm.uv) {
        len += sizeof(tm.uv[0]) * tm.nVertices;
    }

    return len;
}

void do_copy(string& dst, size_t& offset, const void* src, const size_t len) {
    if (offset + len > dst.size()) {
        throw runtime_error("out of bounds");
    }

    memcpy(&dst[0] + offset, reinterpret_cast<const char*>(src), len);
    offset += len;
}

string serialize(const TriangleMesh& tm, const int64_t id,
                 const int64_t material_id) {
    string output;

    const auto output_len = serialized_length(tm);
    output.resize(output_len);

    size_t offset = 0;

    bool has_n = (tm.n != nullptr);
    bool has_s = (tm.s != nullptr);
    bool has_uv = (tm.uv != nullptr);

    do_copy(output, offset, &id, sizeof(int64_t));
    do_copy(output, offset, &material_id, sizeof(int64_t));

    do_copy(output, offset, &tm.nTriangles, sizeof(int));
    do_copy(output, offset, &tm.nVertices, sizeof(int));
    do_copy(output, offset, tm.vertexIndices.data(),
            sizeof(int) * tm.nTriangles * 3);

    do_copy(output, offset, tm.p.get(), sizeof(tm.p[0]) * tm.nVertices);

    do_copy(output, offset, &has_n, sizeof(bool));

    if (has_n) {
        do_copy(output, offset, tm.n.get(), sizeof(tm.n[0]) * tm.nVertices);
    }

    do_copy(output, offset, &has_s, sizeof(bool));

    if (has_s) {
        do_copy(output, offset, tm.s.get(), sizeof(tm.s[0]) * tm.nVertices);
    }

    do_copy(output, offset, &has_uv, sizeof(bool));

    if (has_uv) {
        do_copy(output, offset, tm.uv.get(), sizeof(tm.uv[0]) * tm.nVertices);
    }

    if (offset != output_len) {
        throw runtime_error("offset != output_len");
    }

    return output;
}

TriangleMesh deserialize(const string& input) {
    size_t offset = 2 * sizeof(int64_t);

    const char* data = input.data();

    const int nTriangles = *reinterpret_cast<const int*>(data + offset);
    offset += sizeof(int);

    const int nVertices = *reinterpret_cast<const int*>(data + offset);
    offset += sizeof(int);

    const int* vertex_indices = reinterpret_cast<const int*>(data + offset);
    offset += sizeof(int) * nTriangles * 3;

    const Point3f* p = reinterpret_cast<const Point3f*>(data + offset);
    offset += sizeof(Point3f) * nVertices;

    // n
    const bool has_n = *reinterpret_cast<const bool*>(data + offset);
    offset += sizeof(bool);

    const Normal3f* n =
        has_n ? reinterpret_cast<const Normal3f*>(data + offset) : nullptr;
    offset += has_n ? sizeof(Normal3f) * nVertices : 0;

    // s
    const bool has_s = *reinterpret_cast<const bool*>(data + offset);
    offset += sizeof(bool);

    const Vector3f* s =
        has_s ? reinterpret_cast<const Vector3f*>(data + offset) : nullptr;
    offset += has_s ? sizeof(Vector3f) * nVertices : 0;

    // uv
    const bool has_uv = *reinterpret_cast<const bool*>(data + offset);
    offset += sizeof(bool);

    const Point2f* uv =
        has_uv ? reinterpret_cast<const Point2f*>(data + offset) : nullptr;
    offset += has_uv ? sizeof(Point2f) * nVertices : 0;

    Transform identity;

    return TriangleMesh(identity, nTriangles, vertex_indices, nVertices, p, s,
                        n, uv, nullptr, nullptr, nullptr);
}

}  // namespace triangle_mesh

}  // namespace pbrt::serdes
