#include <iostream>
#include <string>

#include "cloud/manager.h"
#include "include/pbrt/common.h"
#include "messages/utils.h"
#include "util/exception.h"
#include "util/path.h"

using namespace std;
using namespace pbrt;

auto& _manager = global::manager;

void usage(const char* argv0) { cerr << argv0 << " SCENE-PATH" << endl; }

void print_deps(const protobuf::Manifest& manifest, const ObjectKey& root,
                const int level) {
    for (const auto& obj_proto : manifest.objects()) {
        auto obj = from_protobuf(obj_proto.id());
        if (obj == root) {
            for (const auto& dep_proto : obj_proto.dependencies()) {
                auto dep = from_protobuf(dep_proto);
                cout << string(level * 2, ' ') << "\u21b3 "
                     << _manager.getFileName(dep.type, dep.id) << "  \033[38;5;242m"
                     << format_bytes(roost::file_size(
                            _manager.getFilePath(dep.type, dep.id)))
                     << "\033[0m" << endl;
                print_deps(manifest, dep, level + 1);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 2) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
        const string scene_path{argv[1]};
        _manager.init(scene_path);

        // read the manifest file
        protobuf::Manifest manifest_proto;
        _manager.GetReader(ObjectType::Manifest)->read(&manifest_proto);

        for (size_t i = 0; i < _manager.treeletCount(); i++) {
            cout << "T" << i << "  ("
                 << format_bytes(roost::file_size(
                        _manager.getFilePath(ObjectType::Treelet, i)))
                 << ")" << endl;
            print_deps(manifest_proto, {ObjectType::Treelet, i}, 0);
        }
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
