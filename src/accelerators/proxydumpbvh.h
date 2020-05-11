#ifndef PBRT_ACCELERATORS_PROXY_DUMP_BVH_H
#define PBRT_ACCELERATORS_PROXY_DUMP_BVH_H

#include "accelerators/bvh.h"
#include "accelerators/cloud.h"
#include "accelerators/proxy.h"
#include "pbrt.h"
#include "primitive.h"
#include <atomic>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

namespace std {
    template <> struct hash<unordered_set<const pbrt::ProxyBVH *>> {
        size_t operator()(const unordered_set<const pbrt::ProxyBVH *> &proxies) const {
            size_t h = 0;
            for (const pbrt::ProxyBVH *ptr : proxies) {
                h ^= std::hash<const void *>{}(ptr);
            }
            return h;
        }
    };
}

namespace pbrt {

class ProxyDumpBVH : public BVHAccel {
  public:
    enum class TraversalAlgorithm {
        CheckSend,
        SendCheck
    };

    enum class PartitionAlgorithm {
        Topological,
        MergedGraph,
        Nvidia,
    };

    struct Edge {
        uint64_t src;
        uint64_t dst;
        float weight;

        Edge(uint64_t src, uint64_t dst, float weight)
            : src(src), dst(dst), weight(weight)
        {}
    };

    struct IntermediateTraversalGraph {
        std::deque<Edge> edges;
        std::vector<uint64_t> depthFirst;
        std::vector<float> incomingProb;

        std::deque<std::pair<uint64_t, uint64_t>> outgoing;
    };

    struct TraversalGraph {
        std::vector<Edge> edges;
        std::vector<uint64_t> depthFirst;
        std::vector<float> incomingProb;

        std::vector<std::pair<Edge *, uint64_t>> outgoing;
    };

    using TreeletMap = std::array<std::vector<uint32_t>, 8>;
    using RayCountMap = std::vector<std::unordered_map<uint64_t, std::atomic_uint64_t>>;

    ProxyDumpBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                 int maxTreeletBytes,
                 int copyableThreshold,
                 bool writeHeader,
                 bool inlineProxies,
                 TraversalAlgorithm traversal,
                 PartitionAlgorithm partition,
                 int maxPrimsInNode = 1,
                 SplitMethod splitMethod = SplitMethod::SAH);

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

    using ProxySetPtr = const std::unordered_set<const ProxyBVH *> *;
  private:
    struct TreeletInfo {
        std::list<uint64_t> nodes {}; 
        ProxySetPtr proxies {nullptr};
        uint64_t noProxySize {0};
        uint64_t proxySize {0};
        int dirIdx {-1};
        float totalProb {0};
    };

    void SetNodeInfo(int maxTreeletBytes, int copyableThreshold);

    uint64_t GetProxyBytes(ProxySetPtr proxies) const;

    ProxySetPtr ProxyUnion(ProxySetPtr a, ProxySetPtr b) const;

    std::unordered_map<uint32_t, TreeletInfo> MergeDisjointTreelets(int dirIdx, int maxTreeletBytes, const TraversalGraph &graph);
    void OrderTreeletNodesDepthFirst(int numDirs, std::vector<TreeletInfo> &treelets);

    std::vector<TreeletInfo> AllocateUnspecializedTreelets(int maxTreeletBytes);
    std::vector<TreeletInfo> AllocateDirectionalTreelets(int maxTreeletBytes);
    std::vector<TreeletInfo> AllocateTreelets(int maxTreeletBytes);

    IntermediateTraversalGraph CreateTraversalGraphSendCheck(const Vector3f &rayDir, int depthReduction) const;

    IntermediateTraversalGraph CreateTraversalGraphCheckSend(const Vector3f &rayDir, int depthReduction) const;

    TraversalGraph CreateTraversalGraph(const Vector3f &rayDir, int depthReduction) const;

    std::vector<uint32_t>
        ComputeTreeletsTopological(const TraversalGraph &graph,
                                   uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> ComputeTreeletsNvidia(const uint64_t) const;

    std::vector<uint32_t> ComputeTreelets(const TraversalGraph &graph,
                                          uint64_t maxTreeletBytes) const;

    void DumpHeader() const;

    void DumpSanityCheck(const std::vector<std::unordered_map<uint64_t, uint32_t>> &treeletNodeLocations) const;

    std::vector<uint32_t> DumpTreelets(bool root, bool inlineProxies) const;

    TreeletMap treeletAllocations{};

    TraversalAlgorithm traversalAlgo;
    PartitionAlgorithm partitionAlgo;
    std::vector<uint64_t> nodeParents;
    std::vector<uint64_t> nodeSizes;
    std::vector<uint64_t> subtreeSizes;

    std::unordered_set<std::unordered_set<const ProxyBVH *>> proxySets;
    std::unordered_map<ProxySetPtr, uint64_t> proxySizeCache;

    std::vector<ProxySetPtr> nodeProxies;
    std::vector<ProxySetPtr> subtreeProxies;
    std::unordered_set<const ProxyBVH *> largeProxies;
    std::unordered_set<const ProxyBVH *> allProxies;
    std::unordered_map<const ProxyBVH *, uint32_t> proxyOrder;

    std::vector<TreeletInfo> allTreelets;
};

std::shared_ptr<ProxyDumpBVH> CreateProxyDumpBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
