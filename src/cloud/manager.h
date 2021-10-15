#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

#include "core/material.h"
#include "core/paramset.h"
#include "messages/serialization.h"
#include "pbrt/common.h"
#include "util/optional.h"
#include "util/path.h"
#include "util/util.h"

namespace pbrt {

class TriangleMesh;

struct MaterialBlueprint {
  public:
    /* Parameter := <type, name, isTexture> */
    using Parameter = std::tuple<std::type_index, std::string, bool>;

  private:
    const std::vector<Parameter> parameters;

  public:
    MaterialBlueprint(const std::vector<Parameter>& params)
        : parameters(params) {}

    ParamSet FilterParamSet(const ParamSet& src);
};

class SceneManager {
  public:
    using ObjectID = size_t;

    struct Object {
        ObjectID id;
        off_t size;

        Object(const size_t id, const off_t size) : id(id), size(size) {}
    };

    static std::map<MaterialType, MaterialBlueprint> MaterialBlueprints;

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
    uint32_t getTextureFileId(const std::string& path);
    bool hasId(const void* ptr) const { return ptrIds.count(ptr) > 0; }
    void recordDependency(const ObjectKey& from, const ObjectKey& to);
    protobuf::Manifest makeManifest() const;

    static std::string getFileName(const ObjectType type, const uint32_t id);
    const std::string& getScenePath() const { return scenePath; }

    std::string getFilePath(const ObjectType type, const uint32_t id) {
        return getScenePath() + "/" + getFileName(type, id);
    }

    void recordMeshMaterialId(const TriangleMesh* tm, const uint32_t mtl) {
        tmMaterialIds[tm] = mtl;
    }

    uint32_t getMeshMaterialId(const TriangleMesh* tm) const {
        return tmMaterialIds.at(tm);
    }

    std::vector<uint32_t> getAllMaterialIds() const;

    void addToCompoundTexture(
        const std::vector<std::string>& texKey,
        const std::vector<std::string>& partKey,
        std::shared_ptr<std::map<uint32_t, uint32_t>> oldToNew) {
        compoundTextures[texKey].emplace_back(partKey, std::move(oldToNew));
    }

    bool isCompoundTexture(const std::vector<std::string>& texKey) {
        return compoundTextures.count(texKey);
    }

    std::vector<std::pair<std::vector<std::string>,
                          std::shared_ptr<std::map<uint32_t, uint32_t>>>>&
    getCompoundTexture(const std::vector<std::string>& texKey) {
        return compoundTextures.at(texKey);
    }

    void addToCompoundMaterial(
        const uint32_t originalMtlId, const uint32_t partitionMtlId,
        std::shared_ptr<std::map<uint32_t, uint32_t>> oldToNew) {
        compoundMaterials[originalMtlId].emplace(partitionMtlId,
                                                 std::move(oldToNew));
    }

    bool isCompoundMaterial(const uint32_t mtl) {
        return compoundMaterials.count(mtl);
    }

    std::map<uint32_t, std::shared_ptr<std::map<uint32_t, uint32_t>>>&
    getCompoundMaterial(const uint32_t mtl) {
        return compoundMaterials.at(mtl);
    }

    void recordMaterialTreeletId(const uint32_t mtlId, const uint32_t tid) {
        materialToTreelet[mtlId] = tid;
    }

    uint32_t getMaterialTreeletId(const uint32_t mtlId) {
        return materialToTreelet.at(mtlId);
    }

    void recordMeshAreaLightId(const TriangleMesh* tm, const uint32_t light) {
        tmAreaLightIds[tm] = light;
    }

    uint32_t getMeshAreaLightId(const TriangleMesh* tm) {
        return tmAreaLightIds.count(tm) ? tmAreaLightIds.at(tm)
                                        : std::numeric_limits<uint32_t>::max();
    }

    std::vector<double> getTreeletProbs() const;

    const std::set<ObjectKey>& getTreeletDependencies(const ObjectID treeletId);

    const std::map<ObjectKey, std::set<ObjectKey>>& getDependenciesMap() const {
        return dependencies;
    }

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

    std::map<const TriangleMesh*, uint32_t> tmMaterialIds;
    std::map<const TriangleMesh*, uint32_t> tmAreaLightIds;

    std::map<
        std::vector<std::string>,
        std::vector<std::pair<std::vector<std::string>,
                              std::shared_ptr<std::map<uint32_t, uint32_t>>>>>
        compoundTextures;

    std::map<uint32_t,
             std::map<uint32_t, std::shared_ptr<std::map<uint32_t, uint32_t>>>>
        compoundMaterials;  // origMtl -> {newMtl -> {oldFace -> newFace}}

    std::map<uint32_t, uint32_t> materialToTreelet;

    std::map<ObjectID, std::set<ObjectKey>> treeletDependencies;
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
