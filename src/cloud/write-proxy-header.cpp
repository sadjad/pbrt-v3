#include "core/geometry.h"
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "cloud/manager.h"

using namespace pbrt;
using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << argv[0] << " DIR" << endl;
        exit(1);
    }

    const string sceneDir(argv[1]);

    SceneManager mgr;
    mgr.init(sceneDir);

    ofstream header(sceneDir + "/HEADER");

    protobuf::Scene sceneProto;
    auto sceneReader = mgr.GetReader(ObjectType::Scene);
    sceneReader->read(&sceneProto);
    Bounds3f root = from_protobuf(sceneProto.world_bound());

    header.write(reinterpret_cast<char *>(&root), sizeof(Bounds3f));
    uint64_t treeletSize;
    header.write(reinterpret_cast<char *>(&treeletSize), sizeof(uint64_t));

    uint64_t nodeCount = (uint64_t)-1;
    header.write(reinterpret_cast<char *>(&nodeCount), sizeof(uint64_t));

    uint64_t numDependencies = 0;
    header.write(reinterpret_cast<char *>(&numDependencies), sizeof(uint64_t));

    return 0;
}
