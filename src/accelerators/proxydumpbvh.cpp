#include "proxydumpbvh.h"
#include "accelerators/cloud.h"
#include "paramset.h"
#include "stats.h"
#include <algorithm>
#include <fstream>
#include <type_traits>

#include "messages/utils.h"
#include "pbrt.pb.h"
#include <iomanip>
#include <chrono>
using namespace std;

namespace pbrt {

STAT_COUNTER("BVH/Total Ray Transfers", totalRayTransfers);

namespace SizeEstimates {
    constexpr uint64_t nodeSize = sizeof(CloudBVH::TreeletNode);
    // triNum, faceIndex, pointer to mesh, 3 indices for triangle
    // assume on average 2 unique vertices, normals etc per triangle
    constexpr uint64_t triSize = sizeof(int) + sizeof(int) + sizeof(uintptr_t) + 
        3 * sizeof(int) + 2 * (sizeof(Point3f) + sizeof(Normal3f) +
        sizeof(Vector3f) + sizeof(Point2f));
    constexpr uint64_t instSize = 32 * sizeof(float) + sizeof(int);
}

ProxyDumpBVH::ProxyDumpBVH(vector<shared_ptr<Primitive>> &&p,
                               int maxTreeletBytes,
                               int copyableThreshold,
                               bool writeHeader,
                               bool inlineProxies,
                               ProxyDumpBVH::TraversalAlgorithm travAlgo,
                               ProxyDumpBVH::PartitionAlgorithm partAlgo,
                               int maxPrimsInNode,
                               SplitMethod splitMethod)
        : BVHAccel(p, maxPrimsInNode, splitMethod),
          traversalAlgo(travAlgo),
          partitionAlgo(partAlgo)
{
    SetNodeInfo(maxTreeletBytes, copyableThreshold);
    allTreelets = AllocateTreelets(maxTreeletBytes);
    
    if (writeHeader) {
        DumpHeader();
    }
    
    if (PbrtOptions.dumpScene) {
        DumpTreelets(true, inlineProxies);
    }
}

shared_ptr<ProxyDumpBVH> CreateProxyDumpBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps) {
    int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);
    int copyableThreshold = ps.FindOneInt("copyablethreshold", maxTreeletBytes / 2);

    string travAlgoName = ps.FindOneString("traversal", "sendcheck");
    ProxyDumpBVH::TraversalAlgorithm travAlgo;
    if (travAlgoName == "sendcheck")
        travAlgo = ProxyDumpBVH::TraversalAlgorithm::SendCheck;
    else if (travAlgoName == "checksend")
        travAlgo = ProxyDumpBVH::TraversalAlgorithm::CheckSend;
    else {
        Warning("BVH traversal algorithm \"%s\" unknown. Using \"SendCheck\".",
                travAlgoName.c_str());
    }

    string partAlgoName = ps.FindOneString("partition", "topological");
    ProxyDumpBVH::PartitionAlgorithm partAlgo;
    if (partAlgoName == "topological")
        partAlgo = ProxyDumpBVH::PartitionAlgorithm::Topological;
    else if (partAlgoName == "mergedgraph")
        partAlgo = ProxyDumpBVH::PartitionAlgorithm::MergedGraph;
    else if (partAlgoName == "nvidia")
        partAlgo = ProxyDumpBVH::PartitionAlgorithm::Nvidia;
    else {
        Warning("BVH partition algorithm \"%s\" unknown. Using \"Topological\".",
                partAlgoName.c_str());
    }

    bool writeHeader = ps.FindOneBool("writeheader", false);
    bool inlineProxies = ps.FindOneBool("inlineproxies", true);

    string splitMethodName = ps.FindOneString("splitmethod", "sah");
    BVHAccel::SplitMethod splitMethod;
    if (splitMethodName == "sah")
        splitMethod = BVHAccel::SplitMethod::SAH;
    else if (splitMethodName == "hlbvh")
        splitMethod = BVHAccel::SplitMethod::HLBVH;
    else if (splitMethodName == "middle")
        splitMethod = BVHAccel::SplitMethod::Middle;
    else if (splitMethodName == "equal")
        splitMethod = BVHAccel::SplitMethod::EqualCounts;
    else {
        Warning("BVH split method \"%s\" unknown.  Using \"sah\".",
                splitMethodName.c_str());
        splitMethod = BVHAccel::SplitMethod::SAH;
    }
    int maxPrimsInNode = ps.FindOneInt("maxnodeprims", 4);

    // Top level BVH should have as many prims to work with as possible
    if (inlineProxies) {
        maxPrimsInNode = 1;
    }

    return make_shared<ProxyDumpBVH>(move(prims), maxTreeletBytes,
                                     copyableThreshold,
                                     writeHeader, inlineProxies,
                                     travAlgo, partAlgo,
                                     maxPrimsInNode, splitMethod);
}

void ProxyDumpBVH::SetNodeInfo(int maxTreeletBytes, int copyableThreshold) {
    printf("Building general BVH node information\n");
    nodeSizes.resize(nodeCount);
    subtreeSizes.resize(nodeCount);
    nodeParents.resize(nodeCount);
    nodeProxies.resize(nodeCount);
    nodeUnsharedProxies.resize(nodeCount);
    subtreeProxies.resize(nodeCount);
    nodeProxySizes.resize(nodeCount);
    subtreeProxySizes.resize(nodeCount);
    nodeUnsharedProxySizes.resize(nodeCount);
    nodeBounds.resize(nodeCount);

    uint64_t totalNodeBytes = 0;
    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        const LinearBVHNode &node = nodes[nodeIdx];

        uint64_t totalSize = SizeEstimates::nodeSize;

        unordered_set<const ProxyBVH *> includedProxies;

        for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
            auto &prim  = primitives[node.primitivesOffset + primIdx];
            if (prim->GetType() == PrimitiveType::Geometric) {
                totalSize += SizeEstimates::triSize;
            } else if (prim->GetType() == PrimitiveType::Transformed) {
                totalSize += SizeEstimates::instSize;

                shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                shared_ptr<ProxyBVH> proxy = dynamic_pointer_cast<ProxyBVH>(tp->GetPrimitive());
                CHECK_NOTNULL(proxy.get());

                auto processDep = [&](const ProxyBVH *proxy) {
                    allProxies.emplace(proxy);

                    if (proxy->Size() > copyableThreshold) {
                        largeProxies.emplace(proxy);
                    } else {
                        if (proxy->UsageCount() == 1) {
                            totalSize += proxy->Size();
                            nodeUnsharedProxies[nodeIdx].push_back(proxy);
                            nodeUnsharedProxySizes[nodeIdx] += proxy->Size();
                        } else {
                            includedProxies.emplace(proxy);
                        }
                    }
                };

                processDep(proxy.get());

                for (const ProxyBVH *dep : proxy->Dependencies()) {
                    processDep(dep);
                }
            }
        }

        auto kv = proxySets.emplace(move(includedProxies));
        nodeProxies[nodeIdx] = &*kv.first;

        nodeSizes[nodeIdx] = totalSize;

        if (node.nPrimitives == 0) {
            nodeParents[nodeIdx + 1] = nodeIdx;
            nodeParents[node.secondChildOffset] = nodeIdx;
        }

	totalNodeBytes += nodeSizes[nodeIdx];
    }

    // Specific to NVIDIA algorithm
    const float AREA_EPSILON = nodes[0].bounds.SurfaceArea() * maxTreeletBytes / (totalNodeBytes * 10);

    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        const LinearBVHNode &node = nodes[nodeIdx];

        nodeBounds[nodeIdx] = nodes[nodeIdx].bounds.SurfaceArea() + AREA_EPSILON;
    }

    uint32_t proxyIdx = 0;
    for (const ProxyBVH *proxy : allProxies) {
        proxyOrder.emplace(proxy, proxyIdx);
        proxyIdx++;
    }

    auto unorderedUnion = [](const unordered_set<const ProxyBVH *> *a,
                             const unordered_set<const ProxyBVH *> *b) {
        auto setUnion = *a;
        for (const ProxyBVH *ptr : *b) {
            setUnion.emplace(ptr);
        }

        return setUnion;
    };

    for (uint64_t nodeIdx = nodeCount; nodeIdx-- > 0;) {
        const LinearBVHNode &node = nodes[nodeIdx];
        subtreeSizes[nodeIdx] = nodeSizes[nodeIdx];
        subtreeProxies[nodeIdx] = nodeProxies[nodeIdx];
        if (node.nPrimitives == 0) {
            subtreeProxies[nodeIdx] = ProxyUnion(subtreeProxies[nodeIdx + 1],
                                                 subtreeProxies[node.secondChildOffset]);
            
            subtreeSizes[nodeIdx] += subtreeSizes[nodeIdx + 1] +
                                     subtreeSizes[node.secondChildOffset];
        }
    }

    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        nodeProxySizes[nodeIdx] = GetProxyBytes(nodeProxies[nodeIdx]);
        subtreeProxySizes[nodeIdx] = GetProxyBytes(subtreeProxies[nodeIdx]);
    }

    printf("Done building general BVH node information\n");
}

uint64_t ProxyDumpBVH::GetProxyBytes(ProxySetPtr proxies) const {
    auto iter = proxySizeCache.find(proxies);
    if (iter != proxySizeCache.end()) {
        return iter->second;
    }

    uint64_t totalProxiesSize = 0;
    for (const ProxyBVH *proxy : *proxies) {
        totalProxiesSize += proxy->Size();
    }

    const_cast<unordered_map<ProxySetPtr, uint64_t> *>(&proxySizeCache)->emplace(proxies, totalProxiesSize);

    return totalProxiesSize;
}

ProxyDumpBVH::ProxySetPtr ProxyDumpBVH::ProxyUnion(ProxyDumpBVH::ProxySetPtr a,
                                                   ProxyDumpBVH::ProxySetPtr b) const {
    if (a == b) {
        return a;
    }

    if (a == nullptr) {
        return b;
    }

    if (b == nullptr) {
        return a;
    }

    auto setUnion = *a;

    for (const ProxyBVH *ptr : *b) {
        setUnion.emplace(ptr);
    }

    auto kv = const_cast<unordered_set<unordered_set<const ProxyBVH *>> *>(&proxySets)->emplace(move(setUnion));
    return &*kv.first;
}

ProxyDumpBVH::TreeletInfo::TreeletInfo(IntermediateTreeletInfo &&info)
    : nodes(move(info.nodes)),
      proxies(),
      noProxySize(info.noProxySize),
      proxySize(info.proxySize + info.unsharedProxySize),
      dirIdx(info.dirIdx),
      totalProb(info.totalProb)
{
    proxies.insert(proxies.end(), info.proxies->begin(), info.proxies->end());
    proxies.insert(proxies.end(), info.unsharedProxies.begin(), info.unsharedProxies.end());
}

unordered_map<uint32_t, ProxyDumpBVH::IntermediateTreeletInfo> ProxyDumpBVH::MergeDisjointTreelets(int dirIdx, int maxTreeletBytes, const TraversalGraph &graph) {
    unordered_map<uint32_t, IntermediateTreeletInfo> treelets;
    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t curTreelet = treeletAllocations[dirIdx][nodeIdx];
        IntermediateTreeletInfo &treelet = treelets[curTreelet];
        treelet.proxies = ProxyUnion(treelet.proxies, nodeProxies[nodeIdx]);
        treelet.unsharedProxies.insert(treelet.unsharedProxies.end(), nodeUnsharedProxies[nodeIdx].begin(),
                                       nodeUnsharedProxies[nodeIdx].end());
        treelet.dirIdx = dirIdx;
        treelet.nodes.push_back(nodeIdx);
        // Don't want to count unshared proxies as raw node sizes, will double count
        treelet.noProxySize += nodeSizes[nodeIdx] - nodeUnsharedProxySizes[nodeIdx];
        treelet.unsharedProxySize += nodeUnsharedProxySizes[nodeIdx];

        auto outgoingBounds = graph.outgoing[nodeIdx];
        for (uint64_t edgeIdx = 0; edgeIdx < outgoingBounds.second; edgeIdx++) {
            Edge *edge = outgoingBounds.first + edgeIdx;
            uint32_t dstTreelet = treeletAllocations[dirIdx][edge->dst];
            if (curTreelet != dstTreelet) {
                IntermediateTreeletInfo &dstTreeletInfo = treelets[dstTreelet];
                dstTreeletInfo.totalProb += edge->weight;
            }
        }
    }

    for (auto &kv : treelets) {
        kv.second.proxySize = 0;
        for (const ProxyBVH *proxy : *(kv.second.proxies)) {
            kv.second.proxySize += proxy->Size();
        }
    }

    IntermediateTreeletInfo &rootTreelet = treelets.at(treeletAllocations[dirIdx][0]);
    rootTreelet.totalProb += 1.0;

    struct TreeletSortKey {
        uint32_t treeletID;
        uint64_t treeletSize;

        TreeletSortKey(uint32_t treeletID, uint64_t treeletSize)
            : treeletID(treeletID), treeletSize(treeletSize)
        {}
    };

    struct TreeletCmp {
        bool operator()(const TreeletSortKey &a, const TreeletSortKey &b) const {
            if (a.treeletSize < b.treeletSize) {
                return true;
            }

            if (a.treeletSize > b.treeletSize) {
                return false;
            }

            return a.treeletID < b.treeletID;
        }
    };

    map<TreeletSortKey, IntermediateTreeletInfo, TreeletCmp> sortedTreelets;
    for (auto &kv : treelets) {
        CHECK_NE(kv.first, 0);
        CHECK_LE(kv.second.noProxySize + kv.second.proxySize + kv.second.unsharedProxySize, maxTreeletBytes);
        sortedTreelets.emplace(piecewise_construct,
                forward_as_tuple(kv.first, kv.second.noProxySize + kv.second.proxySize + kv.second.unsharedProxySize),
                forward_as_tuple(move(kv.second)));
    }

    // Merge treelets together
    unordered_map<uint32_t, IntermediateTreeletInfo> mergedTreelets;

    auto iter = sortedTreelets.begin();
    while (iter != sortedTreelets.end()) {
        IntermediateTreeletInfo &info = iter->second;

        auto candidateIter = next(iter);
        while (candidateIter != sortedTreelets.end()) {
            auto nextCandidateIter = next(candidateIter);
            IntermediateTreeletInfo &candidateInfo = candidateIter->second;

            uint64_t noProxySize = info.noProxySize + candidateInfo.noProxySize;
            if (noProxySize > maxTreeletBytes) {
                candidateIter = nextCandidateIter;
                continue;
            }

            ProxySetPtr mergedProxies = ProxyUnion(info.proxies, candidateInfo.proxies);
            uint64_t mergedProxySize = GetProxyBytes(mergedProxies);
            uint64_t mergedUnsharedProxySize = info.unsharedProxySize +
                candidateInfo.unsharedProxySize;

            uint64_t totalSize = noProxySize + mergedProxySize + mergedUnsharedProxySize;
            if (totalSize <= maxTreeletBytes) {
                if (info.nodes.front() < candidateInfo.nodes.front()) {
                    info.nodes.splice(info.nodes.end(), move(candidateInfo.nodes));
                } else {
                    candidateInfo.nodes.splice(candidateInfo.nodes.end(), move(info.nodes));
                    info.nodes = move(candidateInfo.nodes);
                }
                info.proxies = mergedProxies;
                info.unsharedProxies.splice(info.unsharedProxies.end(), move(candidateInfo.unsharedProxies));
                info.noProxySize = noProxySize;
                info.proxySize = mergedProxySize;
                info.unsharedProxySize = mergedUnsharedProxySize;
                info.totalProb += candidateInfo.totalProb;
                sortedTreelets.erase(candidateIter);
            }

            // No point searching further
            if (totalSize >= maxTreeletBytes - SizeEstimates::nodeSize) {
                break;
            }

            candidateIter = nextCandidateIter;
        }

        auto nextIter = next(iter);

        mergedTreelets.emplace(iter->first.treeletID, move(info));

        sortedTreelets.erase(iter);

        iter = nextIter;
    }

    return mergedTreelets;
}

void ProxyDumpBVH::OrderTreeletNodesDepthFirst(int numDirs, vector<TreeletInfo> &treelets) {
    // Reorder nodes to be depth first (left then right) for serialization
    for (uint32_t treeletID = 0; treeletID < treelets.size(); treeletID++) {
        TreeletInfo &treelet = treelets[treeletID];
        for (int nodeIdx : treelet.nodes) {
            treeletAllocations[treelet.dirIdx][nodeIdx] = treeletID;
        }
        treelet.nodes.clear();
    }

    for (int dirIdx = 0; dirIdx < numDirs; dirIdx++) {
        stack<uint64_t> depthFirst;
        depthFirst.push(0);

        while (!depthFirst.empty()) {
            uint64_t start = depthFirst.top();
            depthFirst.pop();

            uint32_t treeletID = treeletAllocations[dirIdx][start];
            TreeletInfo &info = treelets[treeletID];

            stack<uint64_t> depthFirstInTreelet;
            depthFirstInTreelet.push(start);
            while (!depthFirstInTreelet.empty()) {
                uint64_t nodeIdx = depthFirstInTreelet.top();
                depthFirstInTreelet.pop();
                info.nodes.push_back(nodeIdx);
                const LinearBVHNode &node = nodes[nodeIdx];
                if (node.nPrimitives == 0) {
                    uint32_t rightTreeletID = treeletAllocations[dirIdx][node.secondChildOffset];
                    if (rightTreeletID == treeletID) {
                        depthFirstInTreelet.push(node.secondChildOffset);
                    } else {
                        depthFirst.push(node.secondChildOffset);
                    }

                    uint32_t leftTreeletID = treeletAllocations[dirIdx][nodeIdx + 1];
                    if (leftTreeletID == treeletID) {
                        depthFirstInTreelet.push(nodeIdx + 1);
                    } else {
                        depthFirst.push(nodeIdx + 1);
                    }
                }
            }
        }
    }
}

vector<ProxyDumpBVH::TreeletInfo> ProxyDumpBVH::AllocateUnspecializedTreelets(int maxTreeletBytes) {
    TraversalGraph graph;
    graph.outgoing.resize(nodeCount);
    graph.incomingProb.resize(nodeCount);

    if (partitionAlgo == PartitionAlgorithm::MergedGraph) {
        vector<unordered_map<uint64_t, float>> mergedEdges;
        mergedEdges.resize(nodeCount);
        for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
            Vector3f dir = ComputeRayDir(dirIdx);
            TraversalGraph g = CreateTraversalGraph(dir, 0);
            if (dirIdx == 0) {
                graph.depthFirst = move(g.depthFirst);
            }
            for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
                graph.incomingProb[nodeIdx] += g.incomingProb[nodeIdx];

                const auto bounds = g.outgoing[nodeIdx];
                for (Edge *edge = bounds.first; edge < bounds.first + bounds.second; edge++) {
                    mergedEdges[edge->src][edge->dst] += edge->weight;
                    mergedEdges[edge->dst][edge->src] += edge->weight;
                }
            }
        }

        uint64_t totalEdges = 0;
        for (auto &outgoing : mergedEdges) {
            totalEdges += outgoing.size();
        }
        graph.edges.reserve(totalEdges);
        for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            auto &outgoing = mergedEdges[nodeIdx];
            size_t start = graph.edges.size();
            for (auto &kv : outgoing) {
                graph.edges.emplace_back(nodeIdx, kv.first, kv.second);
            }
            graph.outgoing[nodeIdx] = make_pair(graph.edges.data() + start, outgoing.size());
        }
    }

    treeletAllocations[0] = ComputeTreelets(graph, maxTreeletBytes);
    auto intermediateTreelets = MergeDisjointTreelets(0, maxTreeletBytes, graph);

    vector<TreeletInfo> finalTreelets;
    for (auto iter = intermediateTreelets.begin(); iter != intermediateTreelets.end(); iter++) {
        IntermediateTreeletInfo &info = iter->second;
        if (info.nodes.front() == 0) {
            finalTreelets.emplace_back(move(info));
            intermediateTreelets.erase(iter);
            break;
        }
    }

    CHECK_EQ(finalTreelets.size(), 1);

    for (auto iter = intermediateTreelets.begin(); iter != intermediateTreelets.end(); iter++) {
        finalTreelets.emplace_back(move(iter->second));
    }

    OrderTreeletNodesDepthFirst(1, finalTreelets);

    // Check that every node is in one treelet exactly once
    vector<uint64_t> nodeCheck(nodeCount);
    for (const TreeletInfo &treelet : finalTreelets) {
        for (uint64_t nodeIdx : treelet.nodes) {
            nodeCheck[nodeIdx] += 1;
        }
    }

    for (uint64_t count : nodeCheck) {
        CHECK_EQ(count, 1);
    }

    return finalTreelets;
}

vector<ProxyDumpBVH::TreeletInfo> ProxyDumpBVH::AllocateDirectionalTreelets(int maxTreeletBytes) {
    array<unordered_map<uint32_t, IntermediateTreeletInfo>, 8> intermediateTreelets;

    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        Vector3f dir = ComputeRayDir(dirIdx);
        TraversalGraph graph = CreateTraversalGraph(dir, 0);

        //rayCounts[i].resize(nodeCount);
        //// Init rayCounts so unordered_map isn't modified during intersection
        //for (uint64_t srcIdx = 0; srcIdx < nodeCount; srcIdx++) {
        //    auto outgoing = graph.outgoing[srcIdx];
        //    for (const Edge *outgoingEdge = outgoing.first;
        //         outgoingEdge < outgoing.first + outgoing.second;
        //         outgoingEdge++) {
        //        uint64_t dstIdx = outgoingEdge->dst;
        //        auto res = rayCounts[i][srcIdx].emplace(dstIdx, 0);
        //        CHECK_EQ(res.second, true);
        //    }
        //}

        treeletAllocations[dirIdx] = ComputeTreelets(graph, maxTreeletBytes);
        intermediateTreelets[dirIdx] = MergeDisjointTreelets(dirIdx, maxTreeletBytes, graph);
    }

    vector<TreeletInfo> finalTreelets;
    // Assign root treelets to IDs 0 to 8
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        // Search for treelet that holds node 0
        for (auto iter = intermediateTreelets[dirIdx].begin(); iter != intermediateTreelets[dirIdx].end(); iter++) {
            IntermediateTreeletInfo &info = iter->second;
            if (info.nodes.front() == 0) {
                finalTreelets.push_back(move(info));
                intermediateTreelets[dirIdx].erase(iter);
                break;
            }
        }
    }
    CHECK_EQ(finalTreelets.size(), 8);

    // Assign the rest contiguously
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        for (auto &p : intermediateTreelets[dirIdx]) {
            IntermediateTreeletInfo &treelet = p.second;
            finalTreelets.emplace_back(move(treelet));
        }
    }

    OrderTreeletNodesDepthFirst(8, finalTreelets);

    // Check that every node is in one treelet exactly once
    array<vector<uint64_t>, 8> nodeCheck;
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        nodeCheck[dirIdx] = vector<uint64_t>(nodeCount);
    }

    for (const TreeletInfo &treelet : finalTreelets) {
        for (uint64_t nodeIdx : treelet.nodes) {
            nodeCheck[treelet.dirIdx][nodeIdx] += 1;
        }
    }

    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        for (uint64_t count : nodeCheck[dirIdx]) {
            CHECK_EQ(count, 1);
        }
    }

    return finalTreelets;
}

vector<ProxyDumpBVH::TreeletInfo> ProxyDumpBVH::AllocateTreelets(int maxTreeletBytes) {
    if (partitionAlgo == PartitionAlgorithm::MergedGraph ||
        partitionAlgo == PartitionAlgorithm::Nvidia) {
        return AllocateUnspecializedTreelets(maxTreeletBytes);
    } else {
        return AllocateDirectionalTreelets(maxTreeletBytes);
    }
}

ProxyDumpBVH::IntermediateTraversalGraph
ProxyDumpBVH::CreateTraversalGraphSendCheck(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);
    g.incomingProb.resize(nodeCount);

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    auto addEdge = [this, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) { // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        g.incomingProb[dst] += prob;
    };

    vector<uint64_t> traversalStack {0};
    traversalStack.reserve(64);

    g.incomingProb[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = g.incomingProb[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

        uint64_t nextHit = 0, nextMiss = 0;
        if (traversalStack.size() > 0) {
            nextMiss = traversalStack.back();
        }

        if (node->nPrimitives == 0) {
            if (dirIsNeg[node->axis]) {
                traversalStack.push_back(curIdx + 1);
                traversalStack.push_back(node->secondChildOffset);
            } else {
                traversalStack.push_back(node->secondChildOffset);
                traversalStack.push_back(curIdx + 1);
            }

            nextHit = traversalStack.back();
            LinearBVHNode *nextHitNode = &nodes[nextHit];

            if (nextMiss == 0) {
                // Guaranteed move down in the BVH
                CHECK_GT(curProb, 0.99); // FP error (should be 1.0)
                addEdge(curIdx, nextHit, curProb);
            } else {
                LinearBVHNode *nextMissNode = &nodes[nextMiss];

                float curSA = node->bounds.SurfaceArea();
                float nextSA = nextHitNode->bounds.SurfaceArea();

                float condHitProb = nextSA / curSA;
                CHECK_LE(condHitProb, 1.0);
                float condMissProb = 1.0 - condHitProb;

                float hitPathProb = curProb * condHitProb;
                float missPathProb = curProb * condMissProb;

                addEdge(curIdx, nextHit, hitPathProb);
                addEdge(curIdx, nextMiss, missPathProb);
            }
        } else if (nextMiss != 0) {
            // If this is a leaf node with a non copyable instance at the end
            // of the primitive list, the edge from curIdx to nextMiss should
            // not exist, because in reality there should be an edge from
            // curIdx to the instance, and from the instance to nextMiss.
            // nextMiss should still receive the incomingProb since the instance
            // edges are never represented in the graph.
            bool skipEdge = false;
            auto &lastPrim = primitives[node->primitivesOffset + node->nPrimitives - 1];
            if (lastPrim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(lastPrim);
                shared_ptr<ProxyBVH> proxy = dynamic_pointer_cast<ProxyBVH>(tp->GetPrimitive());
                CHECK_NOTNULL(proxy.get());
                if (largeProxies.count(proxy.get())) {
                    skipEdge = true;
                }
            }

            // Leaf node, guaranteed move up in the BVH
            if (skipEdge) {
                g.incomingProb[nextMiss] += curProb;
            } else {
                addEdge(curIdx, nextMiss, curProb);
            }
        } else {
            // Termination point for all traversal paths
            CHECK_EQ(traversalStack.size(), 0);
            CHECK_GT(curProb, 0.99);
        }
    }

    return g;
}

ProxyDumpBVH::IntermediateTraversalGraph
ProxyDumpBVH::CreateTraversalGraphCheckSend(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);
    g.incomingProb.resize(nodeCount);

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    // FIXME this should just be a graph method
    auto addEdge = [this, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) { // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        g.incomingProb[dst] += prob;
    };

    vector<uint64_t> traversalStack {0};
    traversalStack.reserve(64);

    g.incomingProb[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = g.incomingProb[curIdx];
        CHECK_GE(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

        if (node->nPrimitives == 0) {
            if (dirIsNeg[node->axis]) {
                traversalStack.push_back(curIdx + 1);
                traversalStack.push_back(node->secondChildOffset);
            } else {
                traversalStack.push_back(node->secondChildOffset);
                traversalStack.push_back(curIdx + 1);
            }
        }

        // refer to SendCheck for explanation
        bool skipEdge = false;
        if (node->nPrimitives > 0) {
            auto &lastPrim = primitives[node->primitivesOffset + node->nPrimitives - 1];
            if (lastPrim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(lastPrim);
                shared_ptr<ProxyBVH> proxy = dynamic_pointer_cast<ProxyBVH>(tp->GetPrimitive());
                if (largeProxies.count(proxy.get())) {
                    skipEdge = true;
                }
            }
        }

        float runningProb = 1.0;
        for (uint64_t i = traversalStack.size(); i-- > 0; i--) {
            uint64_t nextNode = traversalStack[i];
            LinearBVHNode *nextHitNode = &nodes[nextNode];
            LinearBVHNode *parentHitNode = &nodes[nodeParents[nextNode]];
            
            // FIXME ask Pat about this
            float nextSA = nextHitNode->bounds.SurfaceArea();
            float parentSA = parentHitNode->bounds.SurfaceArea();

            float condHitProb = nextSA / parentSA;
            CHECK_LE(condHitProb, 1.0);
            float pathProb = curProb * runningProb * condHitProb;

            if (skipEdge) {
                g.incomingProb[nextNode] += pathProb;
            } else {
                addEdge(curIdx, nextNode, pathProb);
            }
            // runningProb can become 0 here if condHitProb == 1
            // could break, but then edges don't get added and Intersect
            // may crash if it turns out that edge gets taken
            runningProb *= 1.0 - condHitProb;
        }
        CHECK_LE(runningProb, 1.0);
        CHECK_GE(runningProb, 0.0);
    }

    return g;
}

ProxyDumpBVH::TraversalGraph
ProxyDumpBVH::CreateTraversalGraph(const Vector3f &rayDir, int depthReduction) const {
    cout << "Starting graph gen\n";
    IntermediateTraversalGraph intermediate;

    //FIXME fix probabilities here on up edges

    switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            intermediate = CreateTraversalGraphSendCheck(rayDir, depthReduction);
            break;
        case TraversalAlgorithm::CheckSend:
            intermediate = CreateTraversalGraphCheckSend(rayDir, depthReduction);
            break;
    }
    cout << "Intermediate finished\n";

    // Remake graph with contiguous vectors
    TraversalGraph graph;
    auto edgeIter = intermediate.edges.begin();
    while (edgeIter != intermediate.edges.end()) {
        graph.edges.push_back(*edgeIter);
        edgeIter++;
        intermediate.edges.pop_front();
    }

    graph.depthFirst = move(intermediate.depthFirst);
    graph.incomingProb = move(intermediate.incomingProb);

    auto adjacencyIter = intermediate.outgoing.begin();
    while (adjacencyIter != intermediate.outgoing.end()) {
        uint64_t idx = adjacencyIter->first;
        uint64_t weight = adjacencyIter->second;
        graph.outgoing.emplace_back(&graph.edges[idx], weight);
        adjacencyIter++;
        intermediate.outgoing.pop_front();
    }

    printf("Graph gen complete: %lu verts %lu edges\n",
           graph.depthFirst.size(), graph.edges.size());

    return graph;
}

vector<uint32_t>
ProxyDumpBVH::ComputeTreeletsTopological(const TraversalGraph &graph,
                                           uint64_t maxTreeletBytes) const {
    struct OutEdge {
        float weight;
        uint64_t dst;

        OutEdge(const Edge &edge)
            : weight(edge.weight),
              dst(edge.dst)
        {}
    };

    struct EdgeCmp {
        bool operator()(const OutEdge &a, const OutEdge &b) const {
            if (a.weight > b.weight) {
                return true;
            }

            if (a.weight < b.weight) {
                return false;
            }

            return a.dst < b.dst;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    list<uint64_t> depthFirst;
    vector<decltype(depthFirst)::iterator> sortLocs(nodeCount);
    for (uint64_t nodeIdx : graph.depthFirst) {
        depthFirst.push_back(nodeIdx);
        sortLocs[nodeIdx] = --depthFirst.end();
    }

    uint32_t curTreelet = 1;
    while (!depthFirst.empty()) {
        uint64_t curNode = depthFirst.front();
        depthFirst.pop_front();
        assignment[curNode] = curTreelet;

        set<OutEdge, EdgeCmp> cut;
        unordered_map<uint64_t, decltype(cut)::iterator> uniqueLookup;
        ProxySetPtr includedProxies = nodeProxies[curNode];

        // Accounts for size of this node + the size of new instances that would be pulled in
        auto getAdditionalSize = [this, &includedProxies](uint64_t nodeIdx) {
            const LinearBVHNode &node = nodes[nodeIdx];

            uint64_t totalSize = nodeSizes[nodeIdx];

            for (const ProxyBVH *proxy : *(nodeProxies[nodeIdx])) {
                if (!includedProxies->count(proxy)) {
                    totalSize += proxy->Size();
                }
            }

            return totalSize;
        };

        uint64_t rootSize = getAdditionalSize(curNode);
        // If this is false the node is too big to fit in any treelet
        CHECK_LE(rootSize, maxTreeletBytes);

        uint64_t remainingBytes = maxTreeletBytes - rootSize;

        while (remainingBytes >= sizeof(CloudBVH::TreeletNode)) {
            auto outgoingBounds = graph.outgoing[curNode];
            for (uint64_t i = 0; i < outgoingBounds.second; i++) {
                const Edge *edge = outgoingBounds.first + i;

                uint64_t nodeSize = getAdditionalSize(edge->dst);
                if (nodeSize > remainingBytes) continue;

                auto preexisting = uniqueLookup.find(edge->dst);
                if (preexisting == uniqueLookup.end()) {
                    auto res = cut.emplace(*edge);
                    CHECK_EQ(res.second, true);
                    uniqueLookup.emplace(edge->dst, res.first);
                } else {
                    auto &iter = preexisting->second;
                    OutEdge update = *iter;
                    CHECK_EQ(update.dst, edge->dst);
                    update.weight += edge->weight;

                    cut.erase(iter);
                    auto res = cut.insert(update);
                    CHECK_EQ(res.second, true);
                    iter = res.first;
                }
            }

            uint64_t usedBytes = 0;
            auto bestEdge = cut.end();

            auto edge = cut.begin();
            while (edge != cut.end()) {
                auto nextEdge = next(edge);
                uint64_t dst = edge->dst;
                uint64_t curBytes = getAdditionalSize(dst);
                float curWeight = edge->weight;

                // This node already belongs to a treelet
                if (assignment[dst] != 0 || curBytes > remainingBytes) {
                    cut.erase(edge);
                    auto eraseRes = uniqueLookup.erase(dst);
                    CHECK_EQ(eraseRes, 1);
                } else {
                    usedBytes = curBytes;
                    bestEdge = edge;
                    break;
                }

                edge = nextEdge;
            }

            // Treelet full
            if (bestEdge == cut.end()) {
                break;
            }

            cut.erase(bestEdge);
            auto eraseRes = uniqueLookup.erase(bestEdge->dst);
            CHECK_EQ(eraseRes, 1);

            curNode = bestEdge->dst;

            depthFirst.erase(sortLocs[curNode]);
            assignment[curNode] = curTreelet;
            remainingBytes -= usedBytes;
            includedProxies = ProxyUnion(includedProxies, nodeProxies[curNode]);
        }

        curTreelet++;
    }

    return assignment;
}

vector<uint32_t>
ProxyDumpBVH::ComputeTreelets(const TraversalGraph &graph,
                                uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment;
    switch (partitionAlgo) {
        case PartitionAlgorithm::Topological:
            assignment = ComputeTreeletsTopological(graph, maxTreeletBytes);
            break;
        case PartitionAlgorithm::MergedGraph:
            assignment = ComputeTreeletsTopological(graph, maxTreeletBytes);
            break;
        case PartitionAlgorithm::Nvidia:
            assignment = ComputeTreeletsNvidia(maxTreeletBytes);
            break;
    }

    uint64_t totalBytesStats = 0;
    map<uint32_t, uint64_t> sizes;
    unordered_map<uint32_t, ProxySetPtr> proxiesTracker;
    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t treelet = assignment[nodeIdx];
        CHECK_NE(treelet, 0);

        proxiesTracker[treelet] = ProxyUnion(proxiesTracker[treelet], nodeProxies[nodeIdx]);

        uint64_t bytes = nodeSizes[nodeIdx];

        sizes[treelet] += bytes;
        totalBytesStats += bytes;
    }

    for (auto &kv : proxiesTracker) {
        uint32_t treelet = kv.first;
        ProxySetPtr proxies = kv.second;
        for (const ProxyBVH *proxy : *proxies) {
            sizes[treelet] += proxy->Size();
            totalBytesStats += proxy->Size();
        }
    }

    printf("Generated %lu treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytesStats, nodeCount);

    for (auto &sz : sizes) {
        CHECK_LE(sz.second, maxTreeletBytes);
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return assignment;
}

struct NvidiaCut {
    uint64_t nodeIdx;
    uint64_t additionalNodeSize;
    uint64_t additionalSubtreeSize;
    NvidiaCut(uint64_t n, uint64_t nodeBytes, uint64_t subtreeBytes)
        : nodeIdx(n), additionalNodeSize(nodeBytes),
          additionalSubtreeSize(subtreeBytes)
    {}
};

vector<uint32_t> ProxyDumpBVH::ComputeTreeletsNvidia(const uint64_t maxTreeletBytes) const {
    vector<uint32_t> labels(nodeCount);

    /* pass one */
    vector<float> best_costs(nodeCount, 0);

    for (uint64_t root_index = nodeCount; root_index-- > 0;) {
        const LinearBVHNode & root_node = nodes[root_index];

        vector<NvidiaCut> cut;
        cut.emplace_back(root_index,
                         nodeSizes[root_index] + nodeProxySizes[root_index],
                         subtreeSizes[root_index] + subtreeProxySizes[root_index]);
        best_costs[root_index] = std::numeric_limits<float>::max();

        unordered_set<const ProxyBVH *> included_proxies;
        uint64_t remaining_size = maxTreeletBytes;

        while (true) {
            auto best_node_iter = cut.end();
            float best_score = std::numeric_limits<float>::lowest();

            for (auto iter = cut.begin(); iter != cut.end(); iter++) {
                auto n = iter->nodeIdx;
                if (iter->additionalNodeSize > remaining_size) continue;

                float gain = nodeBounds[n];

                uint64_t price = min(iter->additionalSubtreeSize, remaining_size);
                float score = gain / price;
                if (score > best_score) {
                    best_node_iter = iter;
                    best_score = score;
                }
            }

            if (best_node_iter == cut.end()) break;
            uint64_t best_node_index = best_node_iter->nodeIdx;
            remaining_size -= best_node_iter->additionalNodeSize;

            cut.erase(best_node_iter);

            auto &best_node_proxies = *(nodeProxies[best_node_index]);

            for (const ProxyBVH *proxy : best_node_proxies) {
                auto p = included_proxies.emplace(proxy);
                if (!p.second) continue;

                for (auto &cut_elem : cut) {
                    if (nodeProxies[cut_elem.nodeIdx]->count(proxy)) {
                        cut_elem.additionalNodeSize -= proxy->Size();
                    }

                    if (subtreeProxies[cut_elem.nodeIdx]->count(proxy)) {
                        cut_elem.additionalSubtreeSize -= proxy->Size();
                    }
                }
            }

            const LinearBVHNode & best_node = nodes[best_node_index];

            if (best_node.nPrimitives == 0) {
                uint64_t leftNodeAdditionalSize = nodeSizes[best_node_index + 1] + nodeProxySizes[best_node_index + 1];
                uint64_t leftSubtreeAdditionalSize = subtreeSizes[best_node_index + 1] + subtreeProxySizes[best_node_index + 1];

                uint64_t rightNodeAdditionalSize = nodeSizes[best_node.secondChildOffset] + nodeProxySizes[best_node.secondChildOffset];
                uint64_t rightSubtreeAdditionalSize = subtreeSizes[best_node.secondChildOffset] + subtreeProxySizes[best_node.secondChildOffset];

                for (const ProxyBVH *proxy : included_proxies) {
                    if (nodeProxies[best_node_index + 1]->count(proxy)) {
                        leftNodeAdditionalSize -= proxy->Size();
                    }

                    if (subtreeProxies[best_node_index + 1]->count(proxy)) {
                        leftSubtreeAdditionalSize -= proxy->Size();
                    }

                    if (nodeProxies[best_node.secondChildOffset]->count(proxy)) {
                        rightNodeAdditionalSize -= proxy->Size();
                    }

                    if (subtreeProxies[best_node.secondChildOffset]->count(proxy)) {
                        rightSubtreeAdditionalSize -= proxy->Size();
                    }
                }

                cut.emplace_back(best_node_index + 1, leftNodeAdditionalSize, leftSubtreeAdditionalSize);
                cut.emplace_back(best_node.secondChildOffset, rightNodeAdditionalSize, rightSubtreeAdditionalSize);
            }

            float this_cost = nodeBounds[root_index];
            for (const auto &cut_elem : cut) {
                this_cost += best_costs[cut_elem.nodeIdx];
            }
            best_costs[root_index] = std::min(best_costs[root_index], this_cost);
        }
    }

    auto float_equals = [](const float a, const float b) {
        return fabs(a - b) < 1e-4;
    };


    uint32_t current_treelet = 0;

    std::stack<uint64_t> q;
    q.push(0);

    uint64_t node_count = 0;

    while (not q.empty()) {
        const uint64_t root_index = q.top();

        q.pop();

        current_treelet++;

        const LinearBVHNode & root_node = nodes[root_index];
        vector<NvidiaCut> cut;
        cut.emplace_back(root_index,
                         nodeSizes[root_index] + nodeProxySizes[root_index],
                         subtreeSizes[root_index] + subtreeProxySizes[root_index]);

        uint64_t remaining_size = maxTreeletBytes;
        const float best_cost = best_costs[root_index];

        unordered_set<const ProxyBVH *> included_proxies;

        while (true) {
            auto best_node_iter = cut.end();
            float best_score = std::numeric_limits<float>::lowest();

            for (auto iter = cut.begin(); iter != cut.end(); iter++) {
                auto n = iter->nodeIdx;
                if (iter->additionalNodeSize > remaining_size) continue;

                float gain = nodeBounds[n];

                uint64_t price = min(iter->additionalSubtreeSize, remaining_size);
                float score = gain / price;
                if (score > best_score) {
                    best_node_iter = iter;
                    best_score = score;
                }
            }

            if (best_node_iter == cut.end()) break;

            remaining_size -= best_node_iter->additionalNodeSize;
            uint64_t best_node_index = best_node_iter->nodeIdx;

            cut.erase(best_node_iter);

            auto &best_node_proxies = *(nodeProxies[best_node_index]);
            for (const ProxyBVH *proxy : best_node_proxies) {
                auto p = included_proxies.emplace(proxy);
                if (!p.second) continue;

                for (auto &cut_elem : cut) {
                    if (nodeProxies[cut_elem.nodeIdx]->count(proxy)) {
                        cut_elem.additionalNodeSize -= proxy->Size();
                    }

                    if (subtreeProxies[cut_elem.nodeIdx]->count(proxy)) {
                        cut_elem.additionalSubtreeSize -= proxy->Size();
                    }
                }
            }

            const LinearBVHNode & best_node = nodes[best_node_index];

            if (best_node.nPrimitives == 0) {
                uint64_t leftNodeAdditionalSize = nodeSizes[best_node_index + 1] + nodeProxySizes[best_node_index + 1];
                uint64_t leftSubtreeAdditionalSize = subtreeSizes[best_node_index + 1] + subtreeProxySizes[best_node_index + 1];

                uint64_t rightNodeAdditionalSize = nodeSizes[best_node.secondChildOffset] + nodeProxySizes[best_node.secondChildOffset];
                uint64_t rightSubtreeAdditionalSize = subtreeSizes[best_node.secondChildOffset] + subtreeProxySizes[best_node.secondChildOffset];

                for (const ProxyBVH *proxy : included_proxies) {
                    if (nodeProxies[best_node_index + 1]->count(proxy)) {
                        leftNodeAdditionalSize -= proxy->Size();
                    }

                    if (subtreeProxies[best_node_index + 1]->count(proxy)) {
                        leftSubtreeAdditionalSize -= proxy->Size();
                    }

                    if (nodeProxies[best_node.secondChildOffset]->count(proxy)) {
                        rightNodeAdditionalSize -= proxy->Size();
                    }

                    if (subtreeProxies[best_node.secondChildOffset]->count(proxy)) {
                        rightSubtreeAdditionalSize -= proxy->Size();
                    }
                }

                cut.emplace_back(best_node_index + 1, leftNodeAdditionalSize, leftSubtreeAdditionalSize);
                cut.emplace_back(best_node.secondChildOffset, rightNodeAdditionalSize, rightSubtreeAdditionalSize);
            }

            labels[best_node_index] = current_treelet;

            float this_cost = nodeBounds[root_index];
            for (const auto &cut_elem : cut) {
                this_cost += best_costs[cut_elem.nodeIdx];
            }

            if (float_equals(this_cost, best_cost)) {
                break;
            }
        }

        for (const auto &cut_elem : cut) {
            q.push(cut_elem.nodeIdx);
        }
    }

    return labels;
}

void ProxyDumpBVH::DumpSanityCheck(const vector<unordered_map<uint64_t, uint32_t>> &treeletNodeLocations) const {
    enum CHILD {
        LEFT = 0,
        RIGHT = 1
    };

    // Sanity Check for deserializing
    for (uint32_t treeletID = 0; treeletID < allTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = allTreelets[treeletID];
        stack<tuple<uint64_t, uint32_t, CHILD>> q;
        uint32_t serializedLoc = 0;

        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];

            if (!q.empty()) {
                auto parent = q.top();
                q.pop();
                uint64_t parentIdx = get<0>(parent);
                uint32_t parentLoc = get<1>(parent);
                CHILD child = get<2>(parent);

                uint64_t realParent = nodeParents[nodeIdx];

                CHECK_EQ(parentIdx, realParent);
                const LinearBVHNode &parentNode = nodes[parentIdx];
                if (child == LEFT) {
                    CHECK_EQ(&node - &parentNode, 1);
                }
                if (child == RIGHT) {
                    CHECK_EQ(&node - &nodes[0], parentNode.secondChildOffset);
                }
            }

            if (node.nPrimitives == 0) {
                uint64_t leftNodeIdx = nodeIdx + 1;
                uint64_t rightNodeIdx = node.secondChildOffset;

                uint32_t leftTreelet = treeletAllocations[treelet.dirIdx][leftNodeIdx];
                uint32_t rightTreelet = treeletAllocations[treelet.dirIdx][rightNodeIdx];

                if (rightTreelet == treeletID) {
                    q.emplace(nodeIdx, serializedLoc, RIGHT);
                }

                if (leftTreelet == treeletID) {
                    q.emplace(nodeIdx, serializedLoc, LEFT);
                }
            }

            serializedLoc++;
        }
    }

}

void ProxyDumpBVH::DumpHeader() const {
    const string dir = global::manager.getScenePath();
    ofstream header(dir + "/HEADER");
    Bounds3f root = nodes[0].bounds;
    header.write(reinterpret_cast<char *>(&root), sizeof(Bounds3f));

    uint64_t allTreeletsSize = 0;
    for (const TreeletInfo &treelet : allTreelets) {
        allTreeletsSize += treelet.noProxySize;
    }
    header.write(reinterpret_cast<const char *>(&allTreeletsSize), sizeof(uint64_t));

    header.write(reinterpret_cast<const char *>(&nodeCount), sizeof(uint64_t));

    uint64_t numDeps = allProxies.size();
    header.write(reinterpret_cast<const char *>(&numDeps), sizeof(uint64_t));
    for (const ProxyBVH *proxy : allProxies) {
        string name = proxy->Name();
        uint64_t nameSize = name.size();
        header.write(reinterpret_cast<const char *>(&nameSize), sizeof(uint64_t));
        header.write(name.c_str(), nameSize);
    }

    header.close();
}

vector<uint32_t> ProxyDumpBVH::DumpTreelets(bool root, bool inlineProxies) const {
    // Assign IDs to each treelet
    for (const TreeletInfo &treelet : allTreelets) {
        global::manager.getNextId(ObjectType::Treelet, &treelet);
    }

    bool multiDir = false;
    for (const TreeletInfo &info : allTreelets) {
        if (info.dirIdx != 0) {
            multiDir = true;
            break;
        }
    }

    vector<unordered_map<uint64_t, uint32_t>> treeletNodeLocations(allTreelets.size());
    vector<unordered_map<const ProxyBVH *, uint32_t>> treeletProxyStarts(allTreelets.size());
    for (uint32_t treeletID = 0; treeletID < allTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = allTreelets[treeletID];
        uint32_t listIdx = 0;
        for (uint64_t nodeIdx : treelet.nodes) {
            treeletNodeLocations[treeletID][nodeIdx] = listIdx;
            listIdx++;
        }

        uint32_t proxyIdx = treelet.nodes.size();
        for (const ProxyBVH *proxy : treelet.proxies) {
            treeletProxyStarts[treeletID].emplace(proxy, proxyIdx);
            proxyIdx += proxy->nodeCount();
        }
    }

    DumpSanityCheck(treeletNodeLocations);

    auto copyTreelet = [&](unique_ptr<protobuf::RecordReader> &reader,
                           unique_ptr<protobuf::RecordWriter> &writer) {
        uint32_t numMeshes = 0;
        reader->read(&numMeshes);
        writer->write(numMeshes);

        for (int i = 0; i < numMeshes; i++) {
            protobuf::TriangleMesh tm;
            reader->read(&tm);
            writer->write(tm);
        }

        while (!reader->eof()) {
            protobuf::BVHNode proto_node;
            bool success = reader->read(&proto_node);
            CHECK_EQ(success, true);
            writer->write(proto_node);
        }
    };

    unordered_map<const ProxyBVH *, vector<uint32_t>> nonCopyableProxyRoots;

    if (inlineProxies) {
        for (const ProxyBVH *large : largeProxies) {
            auto readers = large->GetReaders();
            CHECK_GT(readers.size(), 0);
            // assign ids
            for (auto &reader : readers) {
                global::manager.getNextId(ObjectType::Treelet, reader.get());
            }

            if (!multiDir) {
                nonCopyableProxyRoots[large].push_back(global::manager.getId(readers[0].get()));
            } else {
                for (int i = 0; i < 8; i++) {
                    nonCopyableProxyRoots[large].push_back(global::manager.getId(readers[i].get()));
                }
            }

            // Redump 
            // FIXME if a large proxy references proxies that it expects to inline this will be wrong
            for (auto &reader : readers) {
                uint32_t id = global::manager.getId(reader.get());
                global::manager.recordDependency(
                    ObjectKey {ObjectType::Treelet, id},
                    ObjectKey {ObjectType::Material, 0});

                auto writer = global::manager.GetWriter(ObjectType::Treelet, id);

                copyTreelet(reader, writer);
            }
        }
    }

    for (uint32_t treeletID = 0; treeletID < allTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = allTreelets[treeletID];
        // Find which triangles / meshes are in treelet
        unordered_map<TriangleMesh *, vector<size_t>> trianglesInTreelet;
        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];
            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Geometric) {
                    shared_ptr<GeometricPrimitive> gp = dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint64_t triNum =
                        (tri->v - tri->mesh->vertexIndices.data()) / 3;

                    CHECK_GE(triNum, 0);
                    CHECK_LT(triNum * 3, tri->mesh->vertexIndices.size());
                    trianglesInTreelet[mesh].push_back(triNum);
                }
            }
        }

        uint32_t numProxyMeshes = 0;
        if (inlineProxies) {
            for (const ProxyBVH *proxy : treelet.proxies) {
                auto readers = proxy->GetReaders();
                // Definitely shouldn't be inlining a proxy that takes up more than 1 full treelet
                CHECK_EQ(readers.size(), 1);
                uint32_t numMeshes;
                readers[0]->read(&numMeshes);
                numProxyMeshes += numMeshes;
            }
        }

        unsigned sTreeletID = global::manager.getId(&treelet);
        auto writer = global::manager.GetWriter(ObjectType::Treelet, sTreeletID);
        uint32_t numTriMeshes = trianglesInTreelet.size() + numProxyMeshes;

        writer->write(numTriMeshes);

        // FIXME add material support
        global::manager.recordDependency(
            ObjectKey {ObjectType::Treelet, sTreeletID},
            ObjectKey {ObjectType::Material, 0});

        unordered_map<TriangleMesh *, unordered_map<size_t, size_t>> triNumRemap;
        unordered_map<TriangleMesh *, uint32_t> triMeshIDs;

        // Write out rewritten meshes with only triangles in treelet
        for (auto &kv : trianglesInTreelet) {
            TriangleMesh *mesh = kv.first;
            vector<size_t> &triNums = kv.second;

            size_t numTris = triNums.size();
            unordered_map<int, size_t> vertexRemap;
            size_t newIdx = 0;
            size_t newTriNum = 0;

            for (auto triNum : triNums) {
                for (int i = 0; i < 3; i++) {
                    int idx = mesh->vertexIndices[triNum * 3 + i];
                    if (vertexRemap.count(idx) == 0) {
                        vertexRemap.emplace(idx, newIdx++);
                    }
                }
                triNumRemap[mesh].emplace(triNum, newTriNum++);
            }
            size_t numVerts = newIdx;
            CHECK_EQ(numVerts, vertexRemap.size());

            vector<int> vertIdxs(numTris * 3);
            vector<Point3f> P(numVerts);
            vector<Vector3f> S(numVerts);
            vector<Normal3f> N(numVerts);
            vector<Point2f> uv(numVerts);
            vector<int> faceIdxs(numVerts);

            for (size_t i = 0; i < numTris; i++) {
                size_t triNum = triNums[i];
                for (int j = 0; j < 3; j++) {
                    int origIdx = mesh->vertexIndices[triNum * 3 + j];
                    int newIdx = vertexRemap.at(origIdx);
                    vertIdxs[i * 3 + j] = newIdx;
                }
                if (mesh->faceIndices.size() > 0) {
                    faceIdxs[i] = mesh->faceIndices[triNum];
                }
            }

            for (auto &kv : vertexRemap) {
                int origIdx = kv.first;
                int newIdx = kv.second;
                P[newIdx] = mesh->p[origIdx];
                if (mesh->s.get()) {
                    S[newIdx] = mesh->s[origIdx];
                }
                if (mesh->n.get()) {
                    N[newIdx] = mesh->n[origIdx];
                }
                if (mesh->uv.get()) {
                    uv[newIdx] = mesh->uv[origIdx];
                }
            }

            shared_ptr<TriangleMesh> newMesh = make_shared<TriangleMesh>(
                Transform(), numTris, vertIdxs.data(), numVerts,
                P.data(), mesh->s.get() ? S.data() : nullptr,
                mesh->n.get() ? N.data() : nullptr,
                mesh->uv.get() ? uv.data() : nullptr, mesh->alphaMask,
                mesh->shadowAlphaMask,
                mesh->faceIndices.size() > 0 ? faceIdxs.data() : nullptr);


            // Give triangle mesh an ID
            uint32_t sMeshID = global::manager.getNextId(ObjectType::TriangleMesh);

            triMeshIDs[mesh] = sMeshID;

            protobuf::TriangleMesh tmProto = to_protobuf(*newMesh);
            tmProto.set_id(sMeshID);
            tmProto.set_material_id(0);
            writer->write(tmProto);
        }

        // Write out the full triangle meshes for all the included proxies referenced by this treelet
        unordered_map<const ProxyBVH *, unordered_map<uint32_t, uint32_t>> proxyMeshIndices;
        if (inlineProxies) {
            for (const ProxyBVH *proxy : treelet.proxies) {
                auto readers = proxy->GetReaders();
                uint32_t numMeshes;
                readers[0]->read(&numMeshes);

                for (int i = 0; i < numMeshes; i++) {
                    protobuf::TriangleMesh tm;
                    readers[0]->read(&tm);

                    uint32_t oldId = tm.id();

                    uint32_t sMeshId = global::manager.getNextId(ObjectType::TriangleMesh);
                    tm.set_id(sMeshId);
                    tm.set_material_id(0);
                    writer->write(tm);
                    proxyMeshIndices[proxy].emplace(oldId, sMeshId);
                }
            }
        }

        // Write out nodes for treelet
        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];

            protobuf::BVHNode nodeProto;
            *nodeProto.mutable_bounds() = to_protobuf(node.bounds);
            nodeProto.set_axis(node.axis);

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp =
                        dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<ProxyBVH> proxy =
                        dynamic_pointer_cast<ProxyBVH>(tp->GetPrimitive());

                    CHECK_NOTNULL(proxy.get());
                    uint64_t instanceRef;
                    if (inlineProxies) {
                        if (!largeProxies.count(proxy.get())) {
                            instanceRef = treeletID;
                            instanceRef <<= 32;
                            instanceRef |= treeletProxyStarts[treeletID].at(proxy.get());
                        } else {
                            auto iter = nonCopyableProxyRoots.find(proxy.get());
                            CHECK_NE(iter == nonCopyableProxyRoots.end(), true);

                            instanceRef = iter->second[treelet.dirIdx];
                            instanceRef <<= 32;
                        }
                    } else {
                        instanceRef = proxyOrder.find(proxy.get())->second;
                        instanceRef <<= 32;
                    }

                    protobuf::TransformedPrimitive tpProto;
                    tpProto.set_root_ref(instanceRef);
                    *tpProto.mutable_transform() = to_protobuf(tp->GetTransform());

                    *nodeProto.add_transformed_primitives() = tpProto;
                } else {
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint32_t sMeshID = triMeshIDs.at(mesh);
                    int origTriNum = (tri->v - mesh->vertexIndices.data()) / 3;
                    int newTriNum = triNumRemap.at(mesh).at(origTriNum);

                    protobuf::Triangle triProto;
                    triProto.set_mesh_id(sMeshID);
                    triProto.set_tri_number(newTriNum);
                    *nodeProto.add_triangles() = triProto;
                }
            }

            if (node.nPrimitives == 0) {
                uint32_t leftTreeletID = treeletAllocations[treelet.dirIdx][nodeIdx + 1];
                if (leftTreeletID != treeletID) {
                    uint32_t sTreeletID = global::manager.getId(&allTreelets[leftTreeletID]);
                    uint64_t leftRef = sTreeletID;
                    leftRef <<= 32;
                    leftRef |=
                        treeletNodeLocations[leftTreeletID].at(nodeIdx + 1);
                    nodeProto.set_left_ref(leftRef);
                }

                uint32_t rightTreeletID = treeletAllocations[treelet.dirIdx][node.secondChildOffset];
                if (rightTreeletID != treeletID) {
                    uint32_t sTreeletID = global::manager.getId(&allTreelets[rightTreeletID]);
                    uint64_t rightRef = sTreeletID;
                    rightRef <<= 32;
                    rightRef |=
                        treeletNodeLocations[rightTreeletID].at(node.secondChildOffset);
                    nodeProto.set_right_ref(rightRef);
                }
            }
            writer->write(nodeProto);
        }

        // Write out nodes for instances
        if (inlineProxies) {
            for (const ProxyBVH *proxy : treelet.proxies) {
                auto readers = proxy->GetReaders();
                auto &reader = readers[0];

                // Skip over all meshes
                uint32_t numMeshes;
                reader->read(&numMeshes);
                reader->skip(numMeshes);

                // Read nodes
                while (!reader->eof()) {
                    protobuf::BVHNode nodeProto;
                    bool success = reader->read(&nodeProto);
                    CHECK_EQ(success, true);
    
                    for (int triIdx = 0; triIdx < nodeProto.triangles_size(); triIdx++) {
                        auto tri = nodeProto.mutable_triangles(triIdx);
                        tri->set_mesh_id(proxyMeshIndices.at(proxy).at(tri->mesh_id()));
                    }

                    for (int tIdx = 0; tIdx < nodeProto.transformed_primitives_size(); tIdx++) {
                        auto transformedProto = nodeProto.mutable_transformed_primitives(tIdx);
                        uint64_t rootRef = transformedProto->root_ref();
                        uint32_t proxyIdx = (uint32_t)(rootRef >> 32);
                        const ProxyBVH *dep = proxy->Dependencies()[proxyIdx];

                        uint64_t instanceRef = 0;
                        if (treeletProxyStarts[treeletID].count(dep)) {
                            instanceRef = treeletID;
                            instanceRef <<= 32;
                            instanceRef |= treeletProxyStarts[treeletID].at(dep);
                        } else {
                            CHECK_EQ(largeProxies.count(dep), 1);
                            instanceRef = nonCopyableProxyRoots.at(dep)[treelet.dirIdx];
                            instanceRef <<= 32;
                        }

                        transformedProto->set_root_ref(instanceRef);
                    }

                    writer->write(nodeProto);
                }
            }
        }
    }

#if 0
    if (root) {
        ofstream staticAllocOut(global::manager.getScenePath() + "/STATIC0_pre");
        for (const TreeletInfo &treelet : allTreelets) {
            uint32_t sTreeletID = global::manager.getId(&treelet);
            staticAllocOut << sTreeletID << " " << treelet.totalProb << endl;
        }

        for (auto &kv : nonCopyableInstanceTreelets) {
            const ProxyDumpBVH *inst = kv.first;
            for (const TreeletInfo &treelet : inst->allTreelets) {
                float instProb = instanceProbabilities[treelet.dirIdx][inst->instanceID];

                uint32_t sTreeletID = global::manager.getId(&treelet);
                staticAllocOut << sTreeletID << " " << treelet.totalProb * instProb << endl;
            }
        }
    }
#endif

    int numRoots = multiDir ? 8 : 1;

    vector<uint32_t> rootTreelets;
    for (int i = 0; i < numRoots; i++) {
        rootTreelets.push_back(global::manager.getId(&allTreelets[i]));
    }

    return rootTreelets;
}

bool ProxyDumpBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    Error("Unimplemented");
    return false;
}

bool ProxyDumpBVH::IntersectP(const Ray &ray) const {
    Error("Unimplemented");
    return false;
}

}
