#include "serdes.h"

using namespace std;

namespace pbrt::serdes {

namespace triangle_mesh {

size_t serialized_length(const TriangleMesh& tm) {
    size_t len = sizeof(int) /* nTriangles */ + sizeof(int) /* nVertices */ +
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

void do_copy(void* dst, size_t& offset, const void* src, const size_t len) {
    memcpy(reinterpret_cast<char*>(dst) + offset,
           reinterpret_cast<const char*>(src), len);
    offset += len;
}

string serialize(const TriangleMesh& tm) {
    string output;

    const auto output_len = serialized_length(tm);
    output.resize(output_len);

    size_t offset = 0;
    char* data = &output[0];

    bool has_n = (tm.n != nullptr);
    bool has_s = (tm.s != nullptr);
    bool has_uv = (tm.uv != nullptr);

    do_copy(data, offset, &tm.nTriangles, sizeof(int));
    do_copy(data, offset, &tm.nVertices, sizeof(int));
    do_copy(data, offset, tm.vertexIndices.data(),
            sizeof(int) * tm.nVertices * 3);
    do_copy(data, offset, tm.p.get(), sizeof(tm.p[0]) * tm.nVertices);

    do_copy(data, offset, &has_n, sizeof(bool));

    if (has_n) {
        do_copy(data, offset, tm.n.get(), sizeof(tm.n[0]) * tm.nVertices);
    }

    do_copy(data, offset, &has_s, sizeof(bool));

    if (has_s) {
        do_copy(data, offset, tm.s.get(), sizeof(tm.s[0]) * tm.nVertices);
    }

    do_copy(data, offset, &has_uv, sizeof(bool));

    if (has_uv) {
        do_copy(data, offset, tm.uv.get(), sizeof(tm.uv[0]) * tm.nVertices);
    }

    if (offset != output_len) {
        throw runtime_error("offset != output_len");
    }

    return output;
}

}  // namespace triangle_mesh

}  // namespace pbrt::serdes
