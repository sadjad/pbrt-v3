#include "core/geometry.h"
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include "messages/utils.h"
#include "messages/serialization.h"
#include "pbrt.pb.h"
#include "cloud/manager.h"
#include <sys/stat.h>

using namespace pbrt;
using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cerr << argv[0] << " SRC DST" << endl;
        exit(1);
    }

    const string sceneDir(argv[1]);
    const string bboxDir(argv[2]);

    mkdir(bboxDir.c_str(), 0777);

    global::manager.init(sceneDir);
    uint32_t numTreelets = global::manager.treeletCount();

    for (uint32_t i = 0; i < numTreelets; i++) {
        auto treelet = global::manager.GetReader(ObjectType::Treelet, i);
        uint32_t numMeshes;
        treelet->read(&numMeshes);
        treelet->skip(numMeshes);
        protobuf::BVHNode root;
        bool success = treelet->read(&root);
        CHECK_EQ(success, true);

        protobuf::Bounds3f bounds = root.bounds();
        cout << from_protobuf(bounds) << endl;
        protobuf::RecordWriter boundsWriter(bboxDir + "/B" + to_string(i));
        boundsWriter.write(bounds);
    }
}
