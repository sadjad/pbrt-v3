#include "accelerators/proxy.h"
#include "cloud/manager.h"

using namespace std;

namespace pbrt {

vector<unique_ptr<protobuf::RecordReader>> ProxyBVH::GetReaders() const {
    vector<unique_ptr<protobuf::RecordReader>> readers;

    SceneManager mgr;
    mgr.init(PbrtOptions.proxyDir + "/" + name_);

    size_t numTreelets = mgr.treeletCount();
    for (size_t i = 0; i < numTreelets; i++) {
        readers.emplace_back(mgr.GetReader(ObjectType::Treelet, i));
    }

    return move(readers);
}

}
