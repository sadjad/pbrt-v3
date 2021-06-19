#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

#include "core/paramset.h"
#include "messages/serialization.h"
#include "pbrt/common.h"
#include "util/optional.h"
#include "util/path.h"
#include "util/util.h"

namespace pbrt {

class SceneManager {
  public:
    using ObjectID = size_t;

    struct Object {
        ObjectID id;
        off_t size;

        Object(const size_t id, const off_t size) : id(id), size(size) {}
    };

    SceneManager() {}

    using ReaderPtr = std::unique_ptr<protobuf::RecordReader>;
    using WriterPtr = std::unique_ptr<protobuf::RecordWriter>;

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }
    ReaderPtr GetReader(const ObjectType type, const uint32_t id = 0) const;
    WriterPtr GetWriter(const ObjectType type, const uint32_t id = 0) const;

    /* used during dumping */
    uint32_t getId(const void* ptr) const { return ptrIds.at(ptr); }
    uint32_t getNextId(const ObjectType type, const void* ptr = nullptr);
    uint32_t getTextureId(const std::string& path);
    bool hasId(const void* ptr) const { return ptrIds.count(ptr) > 0; }
    void recordDependency(const ObjectKey& from, const ObjectKey& to);
    protobuf::Manifest makeManifest() const;

    template <class T>
    void recordParams(const T* obj, const TextureParams& params);

    template <class T>
    TextureParams& getParams(const T* obj);

    template <class T>
    bool hasParams(const T* obj);

    static std::string getFileName(const ObjectType type, const uint32_t id);
    const std::string& getScenePath() const { return scenePath; }

    std::vector<double> getTreeletProbs() const;

    const std::set<ObjectKey>& getTreeletDependencies(const ObjectID treeletId);

    size_t treeletCount();

  private:
    void loadManifest();
    void loadTreeletDependencies();

    std::set<ObjectKey> getRecursiveDependencies(const ObjectKey& object);

    size_t autoIds[to_underlying(ObjectType::COUNT)] = {0};
    std::string scenePath{};
    Optional<FileDescriptor> sceneFD{};
    std::unordered_map<const void*, uint32_t> ptrIds{};
    std::map<std::string, uint32_t> textureNameToId;
    std::map<ObjectKey, uint64_t> objectSizes{};
    std::map<ObjectKey, std::set<ObjectKey>> dependencies;
    std::map<std::type_index, std::map<const void*, TextureParams>>
        recordedParams;

    std::map<ObjectID, std::set<ObjectKey>> treeletDependencies;
};

template <class T>
void SceneManager::recordParams(const T* obj, const TextureParams& params) {
    recordedParams[typeid(T)].emplace(reinterpret_cast<const void*>(obj),
                                      params);
}

template <class T>
TextureParams& SceneManager::getParams(const T* obj) {
    return recordedParams.at(typeid(T)).at(reinterpret_cast<const void*>(obj));
}

template <class T>
bool SceneManager::hasParams(const T* obj) {
    return recordedParams.count(typeid(T)) &&
           recordedParams[typeid(T)].count(reinterpret_cast<const void*>(obj));
}

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
