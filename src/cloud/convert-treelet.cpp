#include <fstream>
#include <iostream>
#include <string>

#include "accelerators/cloud.h"
#include "messages/lite.h"
#include "messages/serdes.h"
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "shapes/triangle.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char* argv0) {
    cerr << argv0 << " OLD-TREELET NEW-TREELET TREELET-ID" << endl;
}

int main(int argc, char* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        string old_filename{argv[1]};
        string new_filename{argv[2]};
        const uint32_t root_id{static_cast<uint32_t>(stoul(argv[3]))};

        LiteRecordWriter writer{new_filename};
        unique_ptr<protobuf::RecordReader> reader =
            make_unique<protobuf::RecordReader>(old_filename);

        uint32_t num_triangle_meshes = 0;
        reader->read(&num_triangle_meshes);

        writer.write(num_triangle_meshes);

        for (int i = 0; i < num_triangle_meshes; ++i) {
            protobuf::TriangleMesh tm_proto;
            reader->read(&tm_proto);

            const uint64_t id = tm_proto.id();
            const uint64_t material_id = tm_proto.material_id();
            const uint32_t area_light_id = 0;

            const string serialized_tm =
                serdes::triangle_mesh::serialize(from_protobuf(tm_proto));

            writer.write(id);
            writer.write(material_id);
            writer.write(area_light_id);
            writer.write(serialized_tm);
        }

        uint32_t node_count = 0;
        uint32_t primitive_count = 0;

        while (not reader->eof()) {
            protobuf::BVHNode node_proto;
            reader->read(&node_proto);
            node_count++;
            primitive_count += node_proto.transformed_primitives_size();
            primitive_count += node_proto.triangles_size();
        }

        writer.write(node_count);
        writer.write(primitive_count);

        // resetting the reader
        reader = make_unique<protobuf::RecordReader>(old_filename);
        reader->skip(1 + num_triangle_meshes);

        std::vector<CloudBVH::TreeletNode> nodes{};
        nodes.reserve(node_count);

        enum Child { LEFT = 0, RIGHT = 1 };
        stack<pair<uint32_t, Child>> q;

        size_t current_primitive_offset = 0;

        while (not reader->eof()) {
            protobuf::BVHNode node_proto;
            reader->read(&node_proto);

            nodes.emplace_back(from_protobuf(node_proto.bounds()),
                               static_cast<uint8_t>(node_proto.axis()));

            auto& node = nodes.back();
            const uint32_t index = nodes.size() - 1;

            if (not q.empty()) {
                auto parent = q.top();
                q.pop();

                nodes[parent.first].child_treelet[parent.second] = root_id;
                nodes[parent.first].child_node[parent.second] = index;
            }

            bool is_leaf = node_proto.transformed_primitives_size() ||
                           node_proto.triangles_size();

            if (node_proto.right_ref()) {
                uint64_t right_ref = node_proto.right_ref();
                uint16_t treelet_id = (uint16_t)(right_ref >> 32);
                node.child_treelet[RIGHT] = treelet_id;
                node.child_node[RIGHT] = (uint32_t)right_ref;
            } else if (!is_leaf) {
                q.emplace(index, RIGHT);
            }

            if (node_proto.left_ref()) {
                uint64_t left_ref = node_proto.left_ref();
                uint16_t treelet_id = (uint16_t)(left_ref >> 32);
                node.child_treelet[LEFT] = treelet_id;
                node.child_node[LEFT] = (uint32_t)left_ref;
            } else if (!is_leaf) {
                q.emplace(index, LEFT);
            }

            if (is_leaf) {
                node.leaf_tag = ~0;
                node.primitive_offset = current_primitive_offset;
                node.primitive_count =
                    node_proto.transformed_primitives_size() +
                    node_proto.triangles_size();

                current_primitive_offset += node.primitive_count;
            }
        }

        writer.write(reinterpret_cast<const char*>(nodes.data()),
                     sizeof(CloudBVH::TreeletNode) * nodes.size());

        // resetting the reader
        reader = make_unique<protobuf::RecordReader>(old_filename);
        reader->skip(1 + num_triangle_meshes);

        serdes::cloudbvh::TransformedPrimitive primitive;
        serdes::cloudbvh::Triangle triangle;

        while (not reader->eof()) {
            protobuf::BVHNode node_proto;
            reader->read(&node_proto);

            writer.write(static_cast<uint32_t>(
                node_proto.transformed_primitives_size()));

            writer.write(static_cast<uint32_t>(node_proto.triangles_size()));

            for (int i = 0; i < node_proto.transformed_primitives_size(); i++) {
                auto& tp_proto = node_proto.transformed_primitives(i);

                primitive.start_transform =
                    from_protobuf(tp_proto.transform().start_transform());
                primitive.end_transform =
                    from_protobuf(tp_proto.transform().end_transform());

                primitive.start_time = tp_proto.transform().start_time();
                primitive.end_time = tp_proto.transform().end_time();

                primitive.root_ref = tp_proto.root_ref();
                writer.write(reinterpret_cast<const char*>(&primitive),
                             sizeof(primitive));
            }

            for (int i = 0; i < node_proto.triangles_size(); i++) {
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
