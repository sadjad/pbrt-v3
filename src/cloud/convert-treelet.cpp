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

        serdes::cloudbvh::Node node;
        serdes::cloudbvh::TransformedPrimitive primitive;
        serdes::cloudbvh::Triangle triangle;

        while (not reader.eof()) {
            protobuf::BVHNode node_proto;
            reader.read(&node_proto);

            node.bounds = from_protobuf(node_proto.bounds());
            node.left_ref = node_proto.left_ref();
            node.right_ref = node_proto.right_ref();
            node.axis = static_cast<uint8_t>(node_proto.axis());
            node.transformed_primitives_count =
                node_proto.transformed_primitives_size();
            node.triangles_count = node_proto.triangles_size();

            writer.write(reinterpret_cast<const char*>(&node), sizeof(node));

            for (int i = 0; i < node.transformed_primitives_count; i++) {
                auto& tp_proto = node_proto.transformed_primitives(i);

                primitive.start =
                    from_protobuf(tp_proto.transform().start_transform());
                primitive.end =
                    from_protobuf(tp_proto.transform().end_transform());
                primitive.start_time = tp_proto.transform().start_time();
                primitive.end_time = tp_proto.transform().end_time();

                primitive.root_ref = tp_proto.root_ref();
                writer.write(reinterpret_cast<const char*>(&primitive),
                             sizeof(primitive));
            }

            for (int i = 0; i < node.triangles_count; i++) {
                auto& t_proto = node_proto.triangles(i);
                triangle.mesh_id = t_proto.mesh_id();
                triangle.tri_number = t_proto.tri_number();
                writer.write(reinterpret_cast<const char*>(&triangle),
                             sizeof(triangle));
            }
        }
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
