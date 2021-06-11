#include <fstream>
#include <iostream>
#include <string>

#include "accelerators/cloud.h"
#include "messages/serdes.h"
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "shapes/triangle.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char* argv0) {
    cerr << argv0 << " OLD-TREELET NEW-TREELET" << endl;
}

int main(int argc, char* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        string old_filename{argv[1]};
        string new_filename{argv[2]};

        protobuf::RecordReader reader{old_filename};
        protobuf::RecordWriter writer{new_filename};

        uint32_t num_triangle_meshes = 0;
        reader.read(&num_triangle_meshes);

        writer.write(num_triangle_meshes);

        for (int i = 0; i < num_triangle_meshes; ++i) {
            protobuf::TriangleMesh tm_proto;
            reader.read(&tm_proto);

            const uint64_t id = tm_proto.id();
            const uint64_t material_id = tm_proto.material_id();

            const string serialized_tm =
                serdes::triangle_mesh::serialize(from_protobuf(tm_proto));

            writer.write(id);
            writer.write(material_id);
            writer.write(serialized_tm);
        }

        while (not reader.eof()) {
            protobuf::BVHNode proto_node;
            reader.read(&proto_node);
            writer.write(proto_node);
        }
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
