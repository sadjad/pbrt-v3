#include "treeletdumpbvh.h"

#include <Ptexture.h>

#include <algorithm>
#include <fstream>
#include <iomanip>

#include "accelerators/cloud.h"
#include "messages/serdes.h"
#include "messages/utils.h"
#include "paramset.h"
#include "pbrt.pb.h"
#include "stats.h"

using namespace std;

namespace pbrt {

auto &_manager = global::manager;

STAT_COUNTER("BVH/Total Ray Transfers", totalRayTransfers);

namespace SizeEstimates {
constexpr uint64_t nodeSize = sizeof(CloudBVH::TreeletNode);
// triNum, faceIndex, pointer to mesh, 3 indices for triangle
// assume on average 2 unique vertices, normals etc per triangle
constexpr uint64_t triSize = sizeof(int) + sizeof(int) + sizeof(uintptr_t) +
                             3 * sizeof(int) +
                             2 * (sizeof(Point3f) + sizeof(Normal3f) +
                                  sizeof(Vector3f) + sizeof(Point2f));
constexpr uint64_t instSize = 32 * sizeof(float) + sizeof(int);
}  // namespace SizeEstimates

int TreeletDumpBVH::numInstances = 0;

TreeletDumpBVH::TreeletDumpBVH(vector<shared_ptr<Primitive>> &&p,
                               int maxTreeletBytes, int copyableThreshold,
                               bool rootBVH, bool writeHeader,
                               TreeletDumpBVH::TraversalAlgorithm travAlgo,
                               TreeletDumpBVH::PartitionAlgorithm partAlgo,
                               int maxPrimsInNode, SplitMethod splitMethod)
    : BVHAccel(p, maxPrimsInNode, splitMethod),
      rootBVH(rootBVH),
      traversalAlgo(travAlgo),
      partitionAlgo(partAlgo) {
    if (rootBVH) {
        SetNodeInfo(maxTreeletBytes);
        allTreelets = AllocateTreelets(maxTreeletBytes);

        if (writeHeader) {
            DumpHeader();
        }

        if (PbrtOptions.dumpScene) {
            DumpTreelets(true);
        }
    } else {
        instanceID = numInstances++;
        CHECK_LT(instanceID, InstanceMask::numInts * 64);

        for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            const LinearBVHNode &node = nodes[nodeIdx];
            totalBytes += SizeEstimates::nodeSize +
                          node.nPrimitives * SizeEstimates::triSize;
        }

        if (totalBytes < copyableThreshold) {
            copyable = true;
        } else {
            SetNodeInfo(maxTreeletBytes);
            allTreelets = AllocateTreelets(maxTreeletBytes);
        }
    }
}

shared_ptr<TreeletDumpBVH> CreateTreeletDumpBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps) {
    int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);
    int copyableThreshold =
        ps.FindOneInt("copyablethreshold", maxTreeletBytes / 2);

    string travAlgoName = ps.FindOneString("traversal", "sendcheck");
    TreeletDumpBVH::TraversalAlgorithm travAlgo;
    if (travAlgoName == "sendcheck")
        travAlgo = TreeletDumpBVH::TraversalAlgorithm::SendCheck;
    else if (travAlgoName == "checksend")
        travAlgo = TreeletDumpBVH::TraversalAlgorithm::CheckSend;
    else {
        Warning("BVH traversal algorithm \"%s\" unknown. Using \"SendCheck\".",
                travAlgoName.c_str());
    }

    string partAlgoName = ps.FindOneString("partition", "onebyone");
    TreeletDumpBVH::PartitionAlgorithm partAlgo;
    if (partAlgoName == "onebyone")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::OneByOne;
    else if (partAlgoName == "topohierarchical")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::TopologicalHierarchical;
    else if (partAlgoName == "greedysize")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::GreedySize;
    else if (partAlgoName == "agglomerative")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::PseudoAgglomerative;
    else if (partAlgoName == "mergedgraph")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::MergedGraph;
    else if (partAlgoName == "nvidia")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::Nvidia;
    else {
        Warning("BVH partition algorithm \"%s\" unknown. Using \"OneByOne\".",
                partAlgoName.c_str());
    }

    bool rootBVH = ps.FindOneBool("sceneaccelerator", false);
    bool writeHeader = ps.FindOneBool("writeheader", false);

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

    return make_shared<TreeletDumpBVH>(
        move(prims), maxTreeletBytes, copyableThreshold, rootBVH, writeHeader,
        travAlgo, partAlgo, maxPrimsInNode, splitMethod);
}

void TreeletDumpBVH::SetNodeInfo(int maxTreeletBytes) {
    printf("Building general BVH node information\n");
    nodeSizes.resize(nodeCount);
    subtreeSizes.resize(nodeCount);
    nodeParents.resize(nodeCount);
    nodeInstanceMasks.resize(nodeCount);
    subtreeInstanceMasks.resize(nodeCount);
    static_assert(sizeof(InstanceMask) ==
                  sizeof(uint64_t) * InstanceMask::numInts);
    CHECK_LE(numInstances, instanceSizes.size());

    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        const LinearBVHNode &node = nodes[nodeIdx];

        uint64_t totalSize = SizeEstimates::nodeSize;

        for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
            auto &prim = primitives[node.primitivesOffset + primIdx];
            if (prim->GetType() == PrimitiveType::Geometric) {
                totalSize += SizeEstimates::triSize;
            } else if (prim->GetType() == PrimitiveType::Transformed) {
                totalSize += SizeEstimates::instSize;

                shared_ptr<TransformedPrimitive> tp =
                    dynamic_pointer_cast<TransformedPrimitive>(prim);
                shared_ptr<TreeletDumpBVH> instance =
                    dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                if (!instance->copyable) continue;

                uniqueInstances[instance->instanceID] = instance.get();
                instanceSizes[instance->instanceID] = instance->totalBytes;

                nodeInstanceMasks[nodeIdx].Set(instance->instanceID);
            }
        }

        nodeSizes[nodeIdx] = totalSize;

        if (node.nPrimitives == 0) {
            nodeParents[nodeIdx + 1] = nodeIdx;
            nodeParents[node.secondChildOffset] = nodeIdx;
        }
    }

    for (uint64_t nodeIdx = nodeCount; nodeIdx-- > 0;) {
        const LinearBVHNode &node = nodes[nodeIdx];
        subtreeSizes[nodeIdx] = nodeSizes[nodeIdx];
        subtreeInstanceMasks[nodeIdx] = nodeInstanceMasks[nodeIdx];
        if (node.nPrimitives == 0) {
            subtreeSizes[nodeIdx] += subtreeSizes[nodeIdx + 1] +
                                     subtreeSizes[node.secondChildOffset];
            subtreeInstanceMasks[nodeIdx] |=
                subtreeInstanceMasks[nodeIdx + 1] |
                subtreeInstanceMasks[node.secondChildOffset];
        }
    }

    printf("Done building general BVH node information\n");
}

uint64_t TreeletDumpBVH::GetInstancesBytes(const InstanceMask &mask) const {
    auto iter = instanceSizeCache.find(mask);
    if (iter != instanceSizeCache.end()) {
        return iter->second;
    }

    uint64_t totalInstanceSize = 0;
    for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
        if (mask.Get(instanceIdx)) {
            totalInstanceSize += instanceSizes[instanceIdx];
        }
    }

    const_cast<unordered_map<InstanceMask, uint64_t> *>(&instanceSizeCache)
        ->emplace(mask, totalInstanceSize);

    return totalInstanceSize;
}

unordered_map<uint32_t, TreeletDumpBVH::TreeletInfo>
TreeletDumpBVH::MergeDisjointTreelets(int dirIdx, int maxTreeletBytes,
                                      const TraversalGraph &graph) {
    unordered_map<uint32_t, TreeletInfo> treelets;
    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t curTreelet = treeletAllocations[dirIdx][nodeIdx];
        TreeletInfo &treelet = treelets[curTreelet];
        treelet.dirIdx = dirIdx;
        treelet.nodes.push_back(nodeIdx);
        treelet.noInstanceSize += nodeSizes[nodeIdx];
        const LinearBVHNode &node = nodes[nodeIdx];

        for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
            auto &prim = primitives[node.primitivesOffset + primIdx];
            if (prim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp =
                    dynamic_pointer_cast<TransformedPrimitive>(prim);
                shared_ptr<TreeletDumpBVH> instance =
                    dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                CHECK_NOTNULL(instance.get());

                if (instance->copyable) {
                    if (!treelet.instanceMask.Get(instance->instanceID)) {
                        treelet.instanceMask.Set(instance->instanceID);
                        treelet.instanceSize += instance->totalBytes;
                    }
                } else {
                    instanceProbabilities[dirIdx][instance->instanceID] +=
                        graph.incomingProb[nodeIdx];
                }
            }
        }

        auto outgoingBounds = graph.outgoing[nodeIdx];
        for (uint64_t edgeIdx = 0; edgeIdx < outgoingBounds.second; edgeIdx++) {
            Edge *edge = outgoingBounds.first + edgeIdx;
            uint32_t dstTreelet = treeletAllocations[dirIdx][edge->dst];
            if (curTreelet != dstTreelet) {
                TreeletInfo &dstTreeletInfo = treelets[dstTreelet];
                dstTreeletInfo.totalProb += edge->weight;
            }
        }
    }
    TreeletInfo &rootTreelet = treelets.at(treeletAllocations[dirIdx][0]);
    rootTreelet.totalProb += 1.0;

    struct TreeletSortKey {
        uint32_t treeletID;
        uint64_t treeletSize;

        TreeletSortKey(uint32_t treeletID, uint64_t treeletSize)
            : treeletID(treeletID), treeletSize(treeletSize) {}
    };

    struct TreeletCmp {
        bool operator()(const TreeletSortKey &a,
                        const TreeletSortKey &b) const {
            if (a.treeletSize < b.treeletSize) {
                return true;
            }

            if (a.treeletSize > b.treeletSize) {
                return false;
            }

            return a.treeletID < b.treeletID;
        }
    };

    map<TreeletSortKey, TreeletInfo, TreeletCmp> sortedTreelets;
    for (auto &kv : treelets) {
        CHECK_NE(kv.first, 0);
        CHECK_LE(kv.second.noInstanceSize + kv.second.instanceSize,
                 maxTreeletBytes);
        sortedTreelets.emplace(
            piecewise_construct,
            forward_as_tuple(kv.first,
                             kv.second.noInstanceSize + kv.second.instanceSize),
            forward_as_tuple(move(kv.second)));
    }

    // Merge treelets together
    unordered_map<uint32_t, TreeletInfo> mergedTreelets;

    auto iter = sortedTreelets.begin();
    while (iter != sortedTreelets.end()) {
        TreeletInfo &info = iter->second;

        auto candidateIter = next(iter);
        while (candidateIter != sortedTreelets.end()) {
            auto nextCandidateIter = next(candidateIter);
            TreeletInfo &candidateInfo = candidateIter->second;

            uint64_t noInstSize =
                info.noInstanceSize + candidateInfo.noInstanceSize;
            if (noInstSize > maxTreeletBytes) {
                candidateIter = nextCandidateIter;
                continue;
            }

            InstanceMask mergedMask =
                info.instanceMask | candidateInfo.instanceMask;
            uint64_t unionInstanceSize = GetInstancesBytes(mergedMask);

            uint64_t totalSize = noInstSize + unionInstanceSize;
            if (totalSize <= maxTreeletBytes) {
                if (info.nodes.front() < candidateInfo.nodes.front()) {
                    info.nodes.splice(info.nodes.end(),
                                      move(candidateInfo.nodes));
                } else {
                    candidateInfo.nodes.splice(candidateInfo.nodes.end(),
                                               move(info.nodes));
                    info.nodes = move(candidateInfo.nodes);
                }
                info.instanceMask = mergedMask;
                info.instanceSize = unionInstanceSize;
                info.noInstanceSize = noInstSize;
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

    // Make final instance lists
    for (auto iter = mergedTreelets.begin(); iter != mergedTreelets.end();
         iter++) {
        TreeletInfo &info = iter->second;
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            if (info.instanceMask.Get(instanceIdx)) {
                info.instances.push_back(uniqueInstances[instanceIdx]);
            }
        }
    }

    return mergedTreelets;
}

void TreeletDumpBVH::OrderTreeletNodesDepthFirst(
    int numDirs, vector<TreeletInfo> &treelets) {
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
                    uint32_t rightTreeletID =
                        treeletAllocations[dirIdx][node.secondChildOffset];
                    if (rightTreeletID == treeletID) {
                        depthFirstInTreelet.push(node.secondChildOffset);
                    } else {
                        depthFirst.push(node.secondChildOffset);
                    }

                    uint32_t leftTreeletID =
                        treeletAllocations[dirIdx][nodeIdx + 1];
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

vector<TreeletDumpBVH::TreeletInfo>
TreeletDumpBVH::AllocateUnspecializedTreelets(int maxTreeletBytes) {
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
                for (Edge *edge = bounds.first;
                     edge < bounds.first + bounds.second; edge++) {
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
            graph.outgoing[nodeIdx] =
                make_pair(graph.edges.data() + start, outgoing.size());
        }
    }

    treeletAllocations[0] = ComputeTreelets(graph, maxTreeletBytes);
    auto intermediateTreelets =
        MergeDisjointTreelets(0, maxTreeletBytes, graph);

    vector<TreeletInfo> finalTreelets;
    for (auto iter = intermediateTreelets.begin();
         iter != intermediateTreelets.end(); iter++) {
        TreeletInfo &info = iter->second;
        if (info.nodes.front() == 0) {
            finalTreelets.emplace_back(move(info));
            intermediateTreelets.erase(iter);
            break;
        }
    }

    CHECK_EQ(finalTreelets.size(), 1);

    for (auto iter = intermediateTreelets.begin();
         iter != intermediateTreelets.end(); iter++) {
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

vector<TreeletDumpBVH::TreeletInfo> TreeletDumpBVH::AllocateDirectionalTreelets(
    int maxTreeletBytes) {
    array<unordered_map<uint32_t, TreeletInfo>, 8> intermediateTreelets;

    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        Vector3f dir = ComputeRayDir(dirIdx);
        TraversalGraph graph = CreateTraversalGraph(dir, 0);

        // rayCounts[i].resize(nodeCount);
        //// Init rayCounts so unordered_map isn't modified during intersection
        // for (uint64_t srcIdx = 0; srcIdx < nodeCount; srcIdx++) {
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
        intermediateTreelets[dirIdx] =
            MergeDisjointTreelets(dirIdx, maxTreeletBytes, graph);
    }

    vector<TreeletInfo> finalTreelets;
    // Assign root treelets to IDs 0 to 8
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        // Search for treelet that holds node 0
        for (auto iter = intermediateTreelets[dirIdx].begin();
             iter != intermediateTreelets[dirIdx].end(); iter++) {
            TreeletInfo &info = iter->second;
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
            TreeletInfo &treelet = p.second;
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

vector<TreeletDumpBVH::TreeletInfo> TreeletDumpBVH::AllocateTreelets(
    int maxTreeletBytes) {
    if (partitionAlgo == PartitionAlgorithm::MergedGraph ||
        partitionAlgo == PartitionAlgorithm::Nvidia) {
        return AllocateUnspecializedTreelets(maxTreeletBytes);
    } else {
        return AllocateDirectionalTreelets(maxTreeletBytes);
    }
}

TreeletDumpBVH::IntermediateTraversalGraph
TreeletDumpBVH::CreateTraversalGraphSendCheck(const Vector3f &rayDir,
                                              int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);
    g.incomingProb.resize(nodeCount);

    bool dirIsNeg[3] = {rayDir.x < 0, rayDir.y < 0, rayDir.z < 0};

    auto addEdge = [this, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) {  // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        g.incomingProb[dst] += prob;
    };

    vector<uint64_t> traversalStack{0};
    traversalStack.reserve(64);

    g.incomingProb[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = g.incomingProb[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001);  // FP error (should be 1.0)

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
                CHECK_GT(curProb, 0.99);  // FP error (should be 1.0)
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
            auto &lastPrim =
                primitives[node->primitivesOffset + node->nPrimitives - 1];
            if (lastPrim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp =
                    dynamic_pointer_cast<TransformedPrimitive>(lastPrim);
                shared_ptr<TreeletDumpBVH> instance =
                    dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                if (!instance->copyable) {
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

TreeletDumpBVH::IntermediateTraversalGraph
TreeletDumpBVH::CreateTraversalGraphCheckSend(const Vector3f &rayDir,
                                              int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);
    g.incomingProb.resize(nodeCount);

    bool dirIsNeg[3] = {rayDir.x < 0, rayDir.y < 0, rayDir.z < 0};

    // FIXME this should just be a graph method
    auto addEdge = [this, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) {  // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        g.incomingProb[dst] += prob;
    };

    vector<uint64_t> traversalStack{0};
    traversalStack.reserve(64);

    g.incomingProb[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = g.incomingProb[curIdx];
        CHECK_GE(curProb, 0.0);
        CHECK_LE(curProb, 1.0001);  // FP error (should be 1.0)

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
            auto &lastPrim =
                primitives[node->primitivesOffset + node->nPrimitives - 1];
            if (lastPrim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp =
                    dynamic_pointer_cast<TransformedPrimitive>(lastPrim);
                shared_ptr<TreeletDumpBVH> instance =
                    dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                if (!instance->copyable) {
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

TreeletDumpBVH::TraversalGraph TreeletDumpBVH::CreateTraversalGraph(
    const Vector3f &rayDir, int depthReduction) const {
    cout << "Starting graph gen\n";
    IntermediateTraversalGraph intermediate;

    // FIXME fix probabilities here on up edges

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

    printf("Graph gen complete: %lu verts %lu edges\n", graph.depthFirst.size(),
           graph.edges.size());

    return graph;
}

vector<uint32_t> TreeletDumpBVH::ComputeTreeletsAgglomerative(
    const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    return vector<uint32_t>(nodeCount);
#if 0
    vector<uint64_t> treeletSizes(nodeCount);
    vector<list<int>> treeletNodes(nodeCount);
    vector<unordered_map<uint64_t, float>> adjacencyList(nodeCount);
    vector<unordered_map<uint64_t, decltype(adjacencyList)::value_type::iterator>>
        reverseAdjacencyList(nodeCount);

    list<uint64_t> liveTreelets;
    // Map from original treelet ids to position in liveTreelets list
    vector<decltype(liveTreelets)::iterator> treeletLocs(nodeCount);

    auto addEdge =
        [&adjacencyList, &reverseAdjacencyList](uint64_t src, const Edge *edge) {
        if (edge) {
            auto res = adjacencyList[src].emplace(edge->dst,
                                                  edge->modelWeight);

            reverseAdjacencyList[edge->dst].emplace(src, res.first);
        }
    };

    // Start each node in unique treelet
    for (uint64_t vert : graph.topologicalVertices) {
        liveTreelets.push_back(vert);
        treeletLocs[vert] = --liveTreelets.end();

        treeletSizes[vert] = GetNodeSize(vert);
        treeletNodes[vert].push_back(vert);

        const OutEdges &outEdges = graph.adjacencyList[vert];
        addEdge(vert, outEdges.hitEdge);
        addEdge(vert, outEdges.missEdge);
    }

    while (true) {
        bool treeletsCombined = false;

        auto treeletIter = liveTreelets.begin();
        while (treeletIter != liveTreelets.end()) {
            uint64_t curTreelet = *treeletIter;
            uint64_t srcSize = treeletSizes[curTreelet];

            auto bestEdgeIter = adjacencyList[curTreelet].end();
            float maxWeight = 0;
            auto edgeIter = adjacencyList[curTreelet].begin();
            while (edgeIter != adjacencyList[curTreelet].end()) {
                auto nextIter = next(edgeIter);
                uint64_t dstTreelet = edgeIter->first;
                float dstWeight = edgeIter->second;
                uint64_t dstSize = treeletSizes[dstTreelet];
S
A                if (srcSize + dstSize > maxTreeletBytes) {
                    adjacencyList[curTreelet].erase(edgeIter);
                    size_t erased = reverseAdjacencyList[dstTreelet].erase(curTreelet);
                    CHECK_EQ(erased, 1);
                } else if (dstWeight > maxWeight) {
                    bestEdgeIter = edgeIter;
                    maxWeight = dstWeight;
                }

                edgeIter = nextIter;
            }

            if (bestEdgeIter != adjacencyList[curTreelet].end()) {
                treeletsCombined = true;
                uint64_t mergeTreelet = bestEdgeIter->first;
                CHECK_NE(mergeTreelet, curTreelet);

                liveTreelets.erase(treeletLocs[mergeTreelet]);

                treeletSizes[curTreelet] += treeletSizes[mergeTreelet];

                // Merge in outgoing edges from mergeTreelet
                for (const auto &edge_pair : adjacencyList[mergeTreelet]) {
                    CHECK_NE(edge_pair.first, mergeTreelet); // Cycle

                    size_t erased = reverseAdjacencyList[edge_pair.first].erase(mergeTreelet);
                    CHECK_EQ(erased, 1);

                    if (edge_pair.first == curTreelet) continue;

                    auto res = adjacencyList[curTreelet].emplace(edge_pair.first, edge_pair.second);

                    if (!res.second) {
                        res.first->second += edge_pair.second;
                    } else {
                        reverseAdjacencyList[edge_pair.first].emplace(curTreelet, res.first);
                    }
                }
                treeletNodes[curTreelet].splice(treeletNodes[curTreelet].end(),
                                            move(treeletNodes[mergeTreelet]));

                bool found = false;
                // Update all links pointing to the treelet that was merged
                for (const auto &backLink : reverseAdjacencyList[mergeTreelet]) {
                    const auto &edge = *backLink.second;
                    CHECK_EQ(edge.first, mergeTreelet);
                    float weight = edge.second;

                    adjacencyList[backLink.first].erase(backLink.second);

                    if (backLink.first == curTreelet) {
                        CHECK_EQ(found, false);
                        found = true;
                        continue;
                    }

                    auto res = adjacencyList[backLink.first].emplace(curTreelet, weight);
                    if (!res.second) {
                        res.first->second += weight;
                    } else {
                        reverseAdjacencyList[curTreelet].emplace(backLink.first, res.first);
                    }
                }
                CHECK_EQ(found, true);
            }

            treeletIter = next(treeletIter);
        }

        if (!treeletsCombined) {
            break;
        }
    }

    uint32_t curTreeletID = 1;
    vector<uint32_t> assignment(nodeCount);

    uint64_t totalNodes = 0;

    for (const auto &curNodes : treeletNodes) {
        if (curNodes.empty()) continue;
        for (int node : curNodes) {
            assignment[node] = curTreeletID;
            totalNodes++;
        }

        curTreeletID++;
    }
    CHECK_EQ(totalNodes, nodeCount); // All treelets assigned?
    printf("Generated %u treelets\n", curTreeletID - 1);

    return assignment;
#endif
}

vector<uint32_t> TreeletDumpBVH::ComputeTreeletsTopological(
    const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    struct OutEdge {
        float weight;
        uint64_t dst;

        OutEdge(const Edge &edge) : weight(edge.weight), dst(edge.dst) {}
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
        InstanceMask includedInstances{};

        // Accounts for size of this node + the size of new instances that would
        // be pulled in
        auto getAdditionalSize = [this, &includedInstances](uint64_t nodeIdx) {
            const LinearBVHNode &node = nodes[nodeIdx];

            uint64_t totalSize = nodeSizes[nodeIdx];

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp =
                        dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> instance =
                        dynamic_pointer_cast<TreeletDumpBVH>(
                            tp->GetPrimitive());
                    if (!instance->copyable) continue;

                    if (!includedInstances.Get(instance->instanceID)) {
                        totalSize += instance->totalBytes;
                    }
                }
            }

            return totalSize;
        };

        uint64_t rootSize = getAdditionalSize(curNode);
        // If this is false the node is too big to fit in any treelet
        CHECK_LE(rootSize, maxTreeletBytes);

        uint64_t remainingBytes = maxTreeletBytes - rootSize;
        includedInstances |= nodeInstanceMasks[curNode];

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
            includedInstances |= nodeInstanceMasks[curNode];
        }

        curTreelet++;
    }

    return assignment;
}

vector<uint32_t> TreeletDumpBVH::ComputeTreeletsTopologicalHierarchical(
    const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment(nodeCount);
    vector<uint32_t> outgoingWeight(nodeCount);

    struct OutEdge {
        float weight;
        uint64_t dst;

        OutEdge(const Edge &edge) : weight(edge.weight), dst(edge.dst) {}
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

    for (auto curNodeIter = graph.depthFirst.rbegin();
         curNodeIter != graph.depthFirst.rend(); curNodeIter++) {
        uint64_t nodeIdx = *curNodeIter;
    }

    return assignment;
}

vector<uint32_t> TreeletDumpBVH::ComputeTreeletsGreedySize(
    const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    return vector<uint32_t>(nodeCount);

#if 0
    static const float ROOT_SIZE = subtreeSizes[0];

    struct OutEdge {
        float weight;
        uint64_t dst;
        uint64_t subtreeSize;

        OutEdge(const Edge &edge, uint64_t subtreeSize)
            : weight(edge.weight),
              dst(edge.dst),
              subtreeSize(subtreeSize)
        {}
    };

    struct EdgeCmp {
        bool operator()(const OutEdge &a, const OutEdge &b) const {
            float aEff = 15.f * a.weight - (float)a.subtreeSize / ROOT_SIZE;
            float bEff = 15.f * b.weight - (float)b.subtreeSize / ROOT_SIZE;
            if (aEff > bEff) {
                return true;
            }

            if (aEff < bEff) {
                return false;
            }

            return a.dst < b.dst;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    list<uint64_t> depthFirst;
    vector<decltype(depthFirst)::iterator> sortLocs(nodeCount);
    for (uint64_t vert : graph.depthFirst) {
        depthFirst.push_back(vert);
        sortLocs[vert] = --depthFirst.end();
    }

    uint32_t curTreelet = 1;
    while (!depthFirst.empty()) {
        uint64_t curNode = depthFirst.front();
        depthFirst.pop_front();
        assignment[curNode] = curTreelet;

        uint64_t remainingBytes = maxTreeletBytes - nodeSizes[curNode];
        set<OutEdge, EdgeCmp> cut;
        unordered_map<uint64_t, decltype(cut)::iterator> uniqueLookup;
        while (remainingBytes >= sizeof(CloudBVH::TreeletNode)) {
            // Add new edges leaving curNode
            auto outgoingBounds = graph.outgoing[curNode];
            for (int i = 0; i < outgoingBounds.second; i++) {
                const Edge *edge = outgoingBounds.first + i;
                if (nodeSizes[edge->dst] > remainingBytes) break;
                auto preexisting = uniqueLookup.find(edge->dst);
                if (preexisting == uniqueLookup.end()) {
                    auto res = cut.emplace(*edge, subtreeSizes[edge->dst]);
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
                uint64_t curBytes = nodeSizes[dst];
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
        }

        curTreelet++;
    }

    return assignment;
#endif
}

vector<uint32_t> TreeletDumpBVH::ComputeTreelets(
    const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment;
    switch (partitionAlgo) {
    case PartitionAlgorithm::OneByOne:
        assignment = ComputeTreeletsTopological(graph, maxTreeletBytes);
        break;
    case PartitionAlgorithm::TopologicalHierarchical:
        assignment =
            ComputeTreeletsTopologicalHierarchical(graph, maxTreeletBytes);
        break;
    case PartitionAlgorithm::GreedySize:
        assignment = ComputeTreeletsGreedySize(graph, maxTreeletBytes);
        break;
    case PartitionAlgorithm::PseudoAgglomerative:
        assignment = ComputeTreeletsAgglomerative(graph, maxTreeletBytes);
        break;
    case PartitionAlgorithm::Nvidia:
        assignment = OrigAssignTreelets(maxTreeletBytes);
        break;
    case PartitionAlgorithm::MergedGraph:
        assignment = ComputeTreeletsTopological(graph, maxTreeletBytes);
        break;
    }

    uint64_t totalBytesStats = 0;
    map<uint32_t, uint64_t> sizes;
    unordered_map<uint32_t, InstanceMask> instanceTracker;
    for (uint64_t nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t treelet = assignment[nodeIdx];
        CHECK_NE(treelet, 0);

        instanceTracker[treelet] |= nodeInstanceMasks[nodeIdx];

        uint64_t bytes = nodeSizes[nodeIdx];

        sizes[treelet] += bytes;
        totalBytesStats += bytes;
    }

    for (auto &kv : instanceTracker) {
        uint32_t treelet = kv.first;
        InstanceMask &mask = kv.second;
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            if (mask.Get(instanceIdx)) {
                sizes[treelet] += instanceSizes[instanceIdx];
                totalBytesStats += instanceSizes[instanceIdx];
            }
        }
    }

    printf("Generated %lu treelets: %lu total bytes from %ld nodes\n",
           sizes.size(), totalBytesStats, nodeCount);

    for (auto &sz : sizes) {
        CHECK_LE(sz.second, maxTreeletBytes);
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return assignment;
}

vector<uint32_t> TreeletDumpBVH::OrigAssignTreelets(
    const uint64_t maxTreeletBytes) const {
    vector<uint32_t> labels(nodeCount);

    /* pass one */
    std::unique_ptr<float[]> best_costs(new float[nodeCount]);

    float max_nodes = (float)maxTreeletBytes / sizeof(CloudBVH::TreeletNode);
    const float AREA_EPSILON =
        nodes[0].bounds.SurfaceArea() * max_nodes / (nodeCount * 10);

    for (uint64_t root_index = nodeCount; root_index-- > 0;) {
        const LinearBVHNode &root_node = nodes[root_index];

        std::list<uint64_t> cut;
        cut.push_back(root_index);
        best_costs[root_index] = std::numeric_limits<float>::max();
        InstanceMask included_instances;
        uint64_t cur_instance_size = 0;
        uint64_t remaining_size = maxTreeletBytes;

        while (true) {
            auto best_node_iter = cut.end();
            float best_score = std::numeric_limits<float>::lowest();
            uint64_t best_node_size = 0;
            uint64_t best_instance_size = 0;

            for (auto iter = cut.begin(); iter != cut.end(); iter++) {
                auto n = *iter;
                float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;

                InstanceMask node_instance_mask =
                    nodeInstanceMasks[n] | included_instances;
                uint64_t additional_instance_size =
                    GetInstancesBytes(node_instance_mask) - cur_instance_size;
                uint64_t additional_node_size =
                    nodeSizes[n] + additional_instance_size;

                if (additional_node_size > remaining_size) continue;

                InstanceMask subtree_instance_mask =
                    subtreeInstanceMasks[n] | included_instances;
                uint64_t additional_subtree_size =
                    subtreeSizes[n] + GetInstancesBytes(subtree_instance_mask) -
                    cur_instance_size;

                uint64_t price = min(additional_subtree_size, remaining_size);
                float score = gain / price;
                if (score > best_score) {
                    best_node_iter = iter;
                    best_score = score;
                    best_node_size = additional_node_size;
                    best_instance_size = additional_instance_size;
                }
            }

            if (best_node_iter == cut.end()) break;
            uint64_t best_node_index = *best_node_iter;

            const LinearBVHNode &best_node = nodes[best_node_index];
            cut.erase(best_node_iter);

            if (best_node.nPrimitives == 0) {
                cut.push_back(best_node_index + 1);
                cut.push_back(best_node.secondChildOffset);
            }

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }
            best_costs[root_index] =
                std::min(best_costs[root_index], this_cost);

            remaining_size -= best_node_size;

            included_instances |= nodeInstanceMasks[best_node_index];
            cur_instance_size += best_instance_size;
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

        const LinearBVHNode &root_node = nodes[root_index];
        list<uint64_t> cut;
        cut.push_back(root_index);

        uint64_t remaining_size = maxTreeletBytes;
        const float best_cost = best_costs[root_index];
        uint64_t cur_instance_size = 0;
        InstanceMask included_instances;

        while (true) {
            auto best_node_iter = cut.end();
            float best_score = std::numeric_limits<float>::lowest();
            uint64_t best_node_size = 0;
            uint64_t best_instance_size = 0;

            for (auto iter = cut.begin(); iter != cut.end(); iter++) {
                auto n = *iter;
                float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;

                InstanceMask node_instance_mask =
                    nodeInstanceMasks[n] | included_instances;
                uint64_t additional_instance_size =
                    GetInstancesBytes(node_instance_mask) - cur_instance_size;
                uint64_t additional_node_size =
                    nodeSizes[n] + additional_instance_size;

                if (additional_node_size > remaining_size) continue;

                InstanceMask subtree_instance_mask =
                    subtreeInstanceMasks[n] | included_instances;
                uint64_t additional_subtree_size =
                    subtreeSizes[n] + GetInstancesBytes(subtree_instance_mask) -
                    cur_instance_size;

                uint64_t price = min(additional_subtree_size, remaining_size);
                float score = gain / price;
                if (score > best_score) {
                    best_node_iter = iter;
                    best_score = score;
                    best_node_size = additional_node_size;
                    best_instance_size = additional_instance_size;
                }
            }

            if (best_node_iter == cut.end()) break;
            uint64_t best_node_index = *best_node_iter;
            const LinearBVHNode &best_node = nodes[best_node_index];
            cut.erase(best_node_iter);

            if (best_node.nPrimitives == 0) {
                cut.push_back(best_node_index + 1);
                cut.push_back(best_node.secondChildOffset);
            }

            labels[best_node_index] = current_treelet;

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }

            remaining_size -= best_node_size;

            included_instances |= nodeInstanceMasks[best_node_index];
            cur_instance_size += best_instance_size;

            if (float_equals(this_cost, best_cost)) {
                break;
            }
        }

        for (const auto n : cut) {
            q.push(n);
        }
    }

    return labels;
}

void UpdateRayCount(const TreeletDumpBVH::RayCountMap &rayCounts, uint64_t src,
                    uint64_t dst) {
    // atomic_uint64_t &rayCount =
    //    const_cast<atomic_uint64_t &>(rayCounts[src].find(dst)->second);
    // rayCount++;
}

bool TreeletDumpBVH::IntersectSendCheck(const Ray &ray,
                                        SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    uint64_t toVisitOffset = 0, currentNodeIndex = 0;
    uint64_t nodesToVisit[64];

    int dirIdx = treeletAllocations[7].empty() ? 0 : ComputeIdx(invDir);
    const auto &labels = treeletAllocations[dirIdx];
    uint32_t prevTreelet = labels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        uint64_t prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        bool instanceReturn = false;
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->nPrimitives; ++i) {
                    auto &prim = primitives[node->primitivesOffset + i];
                    if (prim->Intersect(ray, isect)) {
                        hit = true;
                    }

                    if (prim->GetType() == PrimitiveType::Transformed) {
                        auto tp =
                            dynamic_pointer_cast<TransformedPrimitive>(prim);
                        auto inst = dynamic_pointer_cast<TreeletDumpBVH>(
                            tp->GetPrimitive());
                        if (!inst->copyable) {
                            if (i == node->nPrimitives - 1) {
                                instanceReturn = true;
                            }
                            totalRayTransfers++;
                        }
                    }
                }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curTreelet = labels[currentNodeIndex];

        if (curTreelet != prevTreelet && !instanceReturn) {
            totalRayTransfers++;
        }

        prevTreelet = curTreelet;
    }

    return hit;
}

bool TreeletDumpBVH::IntersectPSendCheck(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    uint64_t nodesToVisit[64];
    uint64_t toVisitOffset = 0, currentNodeIndex = 0;

    int dirIdx = treeletAllocations[7].empty() ? 0 : ComputeIdx(invDir);
    const auto &labels = treeletAllocations[dirIdx];
    uint32_t prevTreelet = labels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        uint64_t prevNodeIndex = currentNodeIndex;
        bool instanceReturn = false;

        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            // Process BVH node _node_ for traversal
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i) {
                    auto &prim = primitives[node->primitivesOffset + i];
                    if (prim->IntersectP(ray)) {
                        return true;
                    }
                    if (prim->GetType() == PrimitiveType::Transformed) {
                        auto tp =
                            dynamic_pointer_cast<TransformedPrimitive>(prim);
                        auto inst = dynamic_pointer_cast<TreeletDumpBVH>(
                            tp->GetPrimitive());
                        if (!inst->copyable) {
                            if (i == node->nPrimitives - 1) {
                                instanceReturn = true;
                            }
                            totalRayTransfers++;
                        }
                    }
                }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                uint64_t prevNodeIndex = currentNodeIndex;
                if (dirIsNeg[node->axis]) {
                    /// second child first
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curTreelet = labels[currentNodeIndex];

        if (curTreelet != prevTreelet && !instanceReturn) {
            totalRayTransfers++;
        }

        prevTreelet = curTreelet;
    }

    return false;
}

bool TreeletDumpBVH::IntersectCheckSend(const Ray &ray,
                                        SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    uint64_t toVisitOffset = 0, currentNodeIndex = 0;
    uint64_t nodesToVisit[64];

    int dirIdx = treeletAllocations[7].empty() ? 0 : ComputeIdx(invDir);
    const auto &labels = treeletAllocations[dirIdx];

    uint32_t prevTreelet = labels[currentNodeIndex];

    // Missed the root node
    if (!nodes[currentNodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg))
        return false;

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        uint64_t prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        if (node->nPrimitives > 0) {
            // Intersect ray with primitives in leaf BVH node
            for (int i = 0; i < node->nPrimitives; ++i) {
                if (primitives[node->primitivesOffset + i]->Intersect(ray,
                                                                      isect)) {
                    hit = true;
                }

                auto &prim = primitives[node->primitivesOffset + i];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    totalRayTransfers++;
                }
            }
        } else {
            // Put far BVH node on _nodesToVisit_ stack, advance to near
            // node
            if (dirIsNeg[node->axis]) {
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
            } else {
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
            }
        }

        while (toVisitOffset > 0) {
            uint64_t nodeIndex = nodesToVisit[--toVisitOffset];
            if (nodes[nodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg)) {
                currentNodeIndex = nodeIndex;
                break;
            }
        }

        if (currentNodeIndex == prevNodeIndex) break;

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curTreelet = labels[currentNodeIndex];

        // No check for instance returning in Check-Send, because the rays
        // will likely need to return to the point of entry
        if (curTreelet != prevTreelet) {
            totalRayTransfers++;
        }

        prevTreelet = curTreelet;
    }

    return hit;
}

bool TreeletDumpBVH::IntersectPCheckSend(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    uint64_t nodesToVisit[64];
    uint64_t toVisitOffset = 0, currentNodeIndex = 0;

    int dirIdx = treeletAllocations[7].empty() ? 0 : ComputeIdx(invDir);
    const auto &labels = treeletAllocations[dirIdx];

    uint32_t prevTreelet = labels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        uint64_t prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        if (node->nPrimitives > 0) {
            // Intersect ray with primitives in leaf BVH node
            for (int i = 0; i < node->nPrimitives; ++i) {
                if (primitives[node->primitivesOffset + i]->IntersectP(ray)) {
                    return true;
                }
                auto &prim = primitives[node->primitivesOffset + i];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    totalRayTransfers++;
                }
            }
        } else {
            // Put far BVH node on _nodesToVisit_ stack, advance to near
            // node
            if (dirIsNeg[node->axis]) {
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
            } else {
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
            }
        }

        while (toVisitOffset > 0) {
            uint64_t nodeIndex = nodesToVisit[--toVisitOffset];
            if (nodes[nodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg)) {
                currentNodeIndex = nodeIndex;
                break;
            }
        }

        if (currentNodeIndex == prevNodeIndex) break;

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curTreelet = labels[currentNodeIndex];

        if (curTreelet != prevTreelet) {
            totalRayTransfers++;
        }

        prevTreelet = curTreelet;
    }

    return false;
}

bool TreeletDumpBVH::Intersect(const Ray &ray,
                               SurfaceInteraction *isect) const {
    if (!rootBVH && !copyable) {
        totalRayTransfers++;
    }

    if (rootBVH || !copyable) {
        switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            return IntersectSendCheck(ray, isect);
        case TraversalAlgorithm::CheckSend:
            return IntersectCheckSend(ray, isect);
        default:
            CHECK_EQ(true, false);
            return false;
        }
    } else {
        return BVHAccel::Intersect(ray, isect);
    }
}

bool TreeletDumpBVH::IntersectP(const Ray &ray) const {
    if (!rootBVH && !copyable) {
        totalRayTransfers++;
    }

    if (rootBVH || !copyable) {
        switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            return IntersectPSendCheck(ray);
        case TraversalAlgorithm::CheckSend:
            return IntersectPCheckSend(ray);
        default:
            CHECK_EQ(true, false);
            return false;
        }
    } else {
        return BVHAccel::IntersectP(ray);
    }
}

void TreeletDumpBVH::DumpSanityCheck(
    const vector<unordered_map<uint64_t, uint32_t>> &treeletNodeLocations)
    const {
    enum CHILD { LEFT = 0, RIGHT = 1 };

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

            uint32_t curLocation = treeletNodeLocations[treeletID].at(nodeIdx);
            uint64_t leftNodeIdx = nodeIdx + 1;
            uint64_t rightNodeIdx = node.secondChildOffset;

            uint32_t leftTreelet =
                treeletAllocations[treelet.dirIdx][leftNodeIdx];
            uint32_t rightTreelet =
                treeletAllocations[treelet.dirIdx][rightNodeIdx];

            if (node.nPrimitives == 0) {
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

void TreeletDumpBVH::DumpHeader() const {
    const string dir = _manager.getScenePath();
    ofstream header(dir + "/HEADER");
    Bounds3f root = nodes[0].bounds;
    header.write(reinterpret_cast<char *>(&root), sizeof(Bounds3f));
    uint64_t allTreeletsSize = 0;
    for (const TreeletInfo &treelet : allTreelets) {
        allTreeletsSize += treelet.noInstanceSize + treelet.instanceSize;
    }

    header.write(reinterpret_cast<char *>(&allTreeletsSize), sizeof(uint64_t));
    header.close();
}

map<int, int> cutPtexTexture(const string &srcPath, const string &dstPath,
                             const set<int> &usedFaces) {
    map<int, int> oldToNew;

    // now we have to cut this ptex
    Ptex::String error;
    PtexPtr<PtexTexture> src{PtexTexture::open(srcPath.c_str(), error, false)};

    if (!src) {
        Error("%s", error.c_str());
        throw runtime_error("failed to open ptex file for reading: " + srcPath);
    }

    PtexPtr<PtexWriter> dst{PtexWriter::open(
        dstPath.c_str(), src->meshType(), src->dataType(), src->numChannels(),
        src->alphaChannel(), src->numFaces(), error, src->hasMipMaps())};

    if (!dst) {
        Error("%s", error.c_str());
        throw runtime_error("failed to open ptex file for writing: " + dstPath);
    }

    dst->setBorderModes(src->uBorderMode(), src->vBorderMode());
    dst->setEdgeFilterMode(src->edgeFilterMode());
    dst->writeMeta(src->getMetaData());

    vector<char> facebuffer;

    size_t outFaceId = 0;
    for (int i = 0; i < src->numFaces(); i++) {
        /* do we need to write this face? */
        if (!usedFaces.count(i)) continue;

        oldToNew[i] = outFaceId;

        const Ptex::FaceInfo &face_info = src->getFaceInfo(i);
        size_t bufferLen = Ptex::DataSize(src->dataType()) *
                           src->numChannels() * face_info.res.size();

        if (facebuffer.size() < bufferLen) {
            facebuffer.resize(bufferLen);
        }

        src->getData(i, &facebuffer[0], 0);
        dst->writeFace(outFaceId++, face_info, &facebuffer[0], 0);
    }

    // writing out the new texture file
    CHECK(dst->close(error));

    return oldToNew;
}

uint32_t getMaterialForMesh(TriangleMesh *newMesh, TriangleMesh *oldMesh) {
    enum { FLOAT, SPECTRUM };

    const uint32_t mtlID = _manager.getMeshMaterialId(oldMesh);

    protobuf::Material mtl;
    _manager.GetReader(ObjectType::Material, mtlID)->read(&mtl);

    vector<tuple<int, string, uint32_t, protobuf::FloatTexture,
                 protobuf::SpectrumTexture>>
        textures;

    set<int> usedFaces;
    if (newMesh->faceIndices) {
        usedFaces.insert(newMesh->faceIndices,
                         newMesh->faceIndices + newMesh->nTriangles);
    }

    // (2) are there any ptex textures that we can cut?
    for (auto &tex : mtl.float_textures()) {
        const auto &name = tex.first;
        const auto id = tex.second;

        protobuf::FloatTexture ftex;
        _manager.GetReader(ObjectType::FloatTexture, id)->read(&ftex);

        if (ftex.name() == "imagemap") {
            throw runtime_error("cutting imagemap textures is not supported");
        } else if (ftex.name() == "ptex") {
            textures.emplace_back(FLOAT, name, id, ftex,
                                  protobuf::SpectrumTexture{});
        }
    }

    for (auto &tex : mtl.spectrum_textures()) {
        const auto &name = tex.first;
        const auto id = tex.second;

        protobuf::SpectrumTexture stex;
        _manager.GetReader(ObjectType::SpectrumTexture, id)->read(&stex);

        if (stex.name() == "imagemap") {
            throw runtime_error("cutting imagemap textures is not supported");
        } else if (stex.name() == "ptex") {
            textures.emplace_back(SPECTRUM, name, id, protobuf::FloatTexture{},
                                  stex);
        }
    }

    // our work is done here
    if (textures.empty()) {
        return mtlID;
    }

    auto newMtlId = _manager.getNextId(ObjectType::Material);

    map<int, int> oldToNewFaceMapping;

    // (3) let's cut them up and create a new material & texture for this mesh.
    for (auto &tex : textures) {
        const int type = get<0>(tex);
        const string &tname = get<1>(tex);
        const uint32_t tid = get<2>(tex);

        auto &ftexProto = get<3>(tex);
        auto &stexProto = get<4>(tex);

        ParamSet pset = from_protobuf(type == FLOAT ? ftexProto.params()
                                                    : stexProto.params());

        const string filename = pset.FindOneString("filename", "");
        if (filename.empty()) {
            throw runtime_error("ptex texture with no filename");
        }

        const uint32_t newtid = _manager.getNextId(ObjectType::Texture);
        const string srcPath = _manager.getScenePath() + "/" + filename;
        const string dstPath =
            _manager.getScenePath() + "/" +
            _manager.getFileName(ObjectType::Texture, newtid);

        const auto mapping = cutPtexTexture(srcPath, dstPath, usedFaces);

        oldToNewFaceMapping.insert(mapping.begin(), mapping.end());

        // update the path to texture in stex
        std::unique_ptr<std::string[]> filenameVal(new std::string[1]);
        filenameVal[0] = _manager.getFileName(ObjectType::Texture, newtid);
        pset.AddString("filename", move(filenameVal), 1);

        if (type == FLOAT) {
            ftexProto.mutable_params()->CopyFrom(to_protobuf(pset));
            const auto newId = _manager.getNextId(ObjectType::FloatTexture);
            _manager.GetWriter(ObjectType::FloatTexture, newId)
                ->write(ftexProto);
            mtl.mutable_float_textures()->operator[](tname) = newId;

            _manager.recordDependency({ObjectType::Material, newMtlId},
                                      {ObjectType::FloatTexture, newId});

            _manager.recordDependency({ObjectType::FloatTexture, newId},
                                      {ObjectType::Texture, newtid});
        } else {
            stexProto.mutable_params()->CopyFrom(to_protobuf(pset));
            const auto newId = _manager.getNextId(ObjectType::SpectrumTexture);
            _manager.GetWriter(ObjectType::SpectrumTexture, newId)
                ->write(stexProto);
            mtl.mutable_spectrum_textures()->operator[](tname) = newId;

            _manager.recordDependency({ObjectType::Material, newMtlId},
                                      {ObjectType::SpectrumTexture, newId});

            _manager.recordDependency({ObjectType::SpectrumTexture, newId},
                                      {ObjectType::Texture, newtid});
        }
    }

    // (4) let's update the face indices for the triangle mesh
    if (newMesh->faceIndices) {
        for (int i = 0; i < newMesh->nTriangles; i++) {
            newMesh->faceIndices[i] =
                oldToNewFaceMapping.at(newMesh->faceIndices[i]);
        }
    }

    // (5) write out the new material
    _manager.GetWriter(ObjectType::Material, newMtlId)->write(mtl);
    return newMtlId;
}

vector<uint32_t> TreeletDumpBVH::DumpTreelets(bool root) const {
    // Assign IDs to each treelet
    for (const TreeletInfo &treelet : allTreelets) {
        _manager.getNextId(ObjectType::Treelet, &treelet);
    }

    vector<unordered_map<uint64_t, uint32_t>> treeletNodeLocations(
        allTreelets.size());
    vector<unordered_map<TreeletDumpBVH *, uint32_t>> treeletInstanceStarts(
        allTreelets.size());
    for (uint32_t treeletID = 0; treeletID < allTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = allTreelets[treeletID];
        uint32_t listIdx = 0;
        for (uint64_t nodeIdx : treelet.nodes) {
            treeletNodeLocations[treeletID][nodeIdx] = listIdx;
            listIdx++;
        }

        uint32_t instIdx = treelet.nodes.size();
        for (TreeletDumpBVH *inst : treelet.instances) {
            treeletInstanceStarts[treeletID][inst] = instIdx;
            instIdx += inst->nodeCount;

            CHECK_EQ(inst->copyable, true);
        }
    }

    DumpSanityCheck(treeletNodeLocations);

    unordered_map<TreeletDumpBVH *, vector<uint32_t>>
        nonCopyableInstanceTreelets;

    for (uint32_t treeletID = 0; treeletID < allTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = allTreelets[treeletID];
        // Find which triangles / meshes are in treelet
        unordered_map<TriangleMesh *, vector<size_t>> trianglesInTreelet;
        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];
            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Geometric) {
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint64_t triNum = (tri->v - tri->mesh->vertexIndices) / 3;

                    CHECK_GE(triNum, 0);
                    trianglesInTreelet[mesh].push_back(triNum);
                }
            }
        }

        // Get meshes for instances
        unordered_set<TriangleMesh *> instanceMeshes;
        for (const TreeletDumpBVH *inst : treelet.instances) {
            for (uint64_t nodeIdx = 0; nodeIdx < inst->nodeCount; nodeIdx++) {
                const LinearBVHNode &node = inst->nodes[nodeIdx];
                for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                    auto &prim =
                        inst->primitives[node.primitivesOffset + primIdx];
                    if (prim->GetType() != PrimitiveType::Geometric) {
                        throw runtime_error("double nested instancing?");
                    }
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();
                    instanceMeshes.insert(mesh);
                }
            }
        }

        unsigned sTreeletID = _manager.getId(&treelet);
        auto writer = _manager.GetWriter(ObjectType::Treelet, sTreeletID);

        const uint32_t numTriMeshes =
            trianglesInTreelet.size() + instanceMeshes.size();
        writer->write(numTriMeshes);

        // FIXME add material support
        _manager.recordDependency(ObjectKey{ObjectType::Treelet, sTreeletID},
                                  ObjectKey{ObjectType::Material, 0});

        unordered_map<TriangleMesh *, unordered_map<size_t, size_t>>
            triNumRemap;
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
            vector<int> faceIdxs(numTris);

            for (size_t i = 0; i < numTris; i++) {
                size_t triNum = triNums[i];
                for (int j = 0; j < 3; j++) {
                    int origIdx = mesh->vertexIndices[triNum * 3 + j];
                    int newIdx = vertexRemap.at(origIdx);
                    vertIdxs[i * 3 + j] = newIdx;
                }
                if (mesh->faceIndices) {
                    faceIdxs[i] = mesh->faceIndices[triNum];
                }
            }

            for (auto &kv : vertexRemap) {
                int origIdx = kv.first;
                int newIdx = kv.second;
                P[newIdx] = mesh->p[origIdx];
                if (mesh->s) {
                    S[newIdx] = mesh->s[origIdx];
                }
                if (mesh->n) {
                    N[newIdx] = mesh->n[origIdx];
                }
                if (mesh->uv) {
                    uv[newIdx] = mesh->uv[origIdx];
                }
            }

            set<int> usedFaces{faceIdxs.begin(), faceIdxs.end()};

            shared_ptr<TriangleMesh> newMesh = make_shared<TriangleMesh>(
                Transform(), numTris, vertIdxs.data(), numVerts, P.data(),
                mesh->s ? S.data() : nullptr, mesh->n ? N.data() : nullptr,
                mesh->uv ? uv.data() : nullptr, mesh->alphaMask,
                mesh->shadowAlphaMask,
                mesh->faceIndices ? faceIdxs.data() : nullptr);

            // Give triangle mesh an ID
            uint32_t sMeshID = _manager.getNextId(ObjectType::TriangleMesh);
            triMeshIDs[mesh] = sMeshID;

            /*** Figuring out the material for the mesh ***/
            // (1) the material id for the original mesh... let's load its info

            const uint32_t mtlID = getMaterialForMesh(newMesh.get(), mesh);

            // writing the triangle mesh
            writer->write(static_cast<uint64_t>(sMeshID));
            writer->write(static_cast<uint64_t>(mtlID));
            writer->write(serdes::triangle_mesh::serialize(*newMesh));

            _manager.recordDependency(
                ObjectKey{ObjectType::Treelet, sTreeletID},
                ObjectKey{ObjectType::Material, mtlID});
        }

        // Write out the full triangle meshes for all the instances referenced
        // by this treelet
        for (TriangleMesh *instMesh : instanceMeshes) {
            uint32_t sMeshID = _manager.getNextId(ObjectType::TriangleMesh);

            triMeshIDs[instMesh] = sMeshID;

            // writing the triangle mesh
            writer->write(static_cast<uint64_t>(sMeshID));
            writer->write(static_cast<uint64_t>(0));
            writer->write(serdes::triangle_mesh::serialize(*instMesh));
        }

        // Write out nodes for treelet
        /* format:
            (1) node_count
            (2) primitive_count
            (3) all the nodes in the form of a CloudBVH::TreeletNode array
            (4) all the primitives */

        uint32_t node_count = treelet.nodes.size();
        for_each(treelet.instances.begin(), treelet.instances.end(),
                 [&](auto inst) { node_count += inst->nodeCount; });

        uint32_t prim_count = 0;

        for_each(
            treelet.nodes.begin(), treelet.nodes.end(),
            [&](const uint64_t idx) { prim_count += nodes[idx].nPrimitives; });

        for_each(treelet.instances.begin(), treelet.instances.end(),
                 [&](const TreeletDumpBVH *inst) {
                     for (int i = 0; i < inst->nodeCount; i++) {
                         prim_count += inst->nodes[i].nPrimitives;
                     }
                 });

        writer->write(static_cast<uint32_t>(node_count));
        writer->write(static_cast<uint32_t>(prim_count));

        size_t current_primitive_offset = 0;
        vector<CloudBVH::TreeletNode> output_nodes;
        output_nodes.reserve(node_count);
        memset(&output_nodes[0], 0, sizeof(CloudBVH::TreeletNode) * node_count);

        enum Child { LEFT = 0, RIGHT = 1 };

        stack<pair<uint32_t, Child>> q;

        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];
            output_nodes.emplace_back(node.bounds, node.axis);

            auto &out_node = output_nodes.back();

            if (not q.empty()) {
                auto parent = q.top();
                q.pop();

                output_nodes[parent.first].child_treelet[parent.second] =
                    sTreeletID;
                output_nodes[parent.first].child_node[parent.second] =
                    output_nodes.size() - 1;
            }

            if (node.nPrimitives == 0) {  // it's not a leaf
                uint32_t r_tid =
                    treeletAllocations[treelet.dirIdx][node.secondChildOffset];
                if (r_tid != treeletID) {
                    out_node.child_treelet[RIGHT] =
                        _manager.getId(&allTreelets[r_tid]);
                    out_node.child_node[RIGHT] =
                        treeletNodeLocations[r_tid].at(node.secondChildOffset);
                } else {
                    q.emplace(output_nodes.size() - 1, RIGHT);
                }

                uint32_t l_tid =
                    treeletAllocations[treelet.dirIdx][nodeIdx + 1];
                if (l_tid != treeletID) {
                    out_node.child_treelet[LEFT] =
                        _manager.getId(&allTreelets[l_tid]);
                    out_node.child_node[LEFT] =
                        treeletNodeLocations[l_tid].at(nodeIdx + 1);
                } else {
                    q.emplace(output_nodes.size() - 1, LEFT);
                }
            } else {  // it is a leaf
                out_node.leaf_tag = ~0;
                out_node.primitive_offset = current_primitive_offset;
                out_node.primitive_count = node.nPrimitives;

                current_primitive_offset += node.nPrimitives;
            }
        }

        CHECK(q.empty());

        for (TreeletDumpBVH *inst : treelet.instances) {
            for (uint64_t nodeIdx = 0; nodeIdx < inst->nodeCount; nodeIdx++) {
                const LinearBVHNode &instNode = inst->nodes[nodeIdx];
                output_nodes.emplace_back(instNode.bounds, instNode.axis);

                auto &out_node = output_nodes.back();

                out_node.leaf_tag = ~0;
                out_node.primitive_offset = current_primitive_offset;
                out_node.primitive_count = instNode.nPrimitives;

                current_primitive_offset += instNode.nPrimitives;
            }
        }

        writer->write(reinterpret_cast<const char *>(output_nodes.data()),
                      sizeof(CloudBVH::TreeletNode) * output_nodes.size());
        output_nodes.clear();

        // Writing out the primitives
        /* format:
            for each node:
                (1) transformed primtives count
                (2) triangles count
                (3) list of primitives */

        serdes::cloudbvh::TransformedPrimitive primitive;
        serdes::cloudbvh::Triangle triangle;

        for (uint64_t nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];

            uint32_t transformed_primitive_count = 0;
            uint32_t triangle_count = 0;

            for (int i = 0; i < node.nPrimitives; i++) {
                if (primitives[node.primitivesOffset + i]->GetType() ==
                    PrimitiveType::Transformed) {
                    transformed_primitive_count++;
                } else {
                    triangle_count++;
                }
            }

            writer->write(transformed_primitive_count);
            writer->write(triangle_count);

            // write all transformed primitives for the node
            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];

                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp =
                        dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> instance =
                        dynamic_pointer_cast<TreeletDumpBVH>(
                            tp->GetPrimitive());

                    CHECK_NOTNULL(instance.get());
                    uint64_t instanceRef;
                    if (instance->copyable) {
                        instanceRef = treeletID;
                        instanceRef <<= 32;
                        instanceRef |=
                            treeletInstanceStarts[treeletID].at(instance.get());
                    } else {
                        auto iter =
                            nonCopyableInstanceTreelets.find(instance.get());
                        if (iter == nonCopyableInstanceTreelets.end()) {
                            auto instanceIDs = instance->DumpTreelets(false);
                            auto res = nonCopyableInstanceTreelets.emplace(
                                instance.get(), move(instanceIDs));
                            CHECK_EQ(res.second, true);
                            iter = res.first;
                        }

                        instanceRef = iter->second[treelet.dirIdx];
                        instanceRef <<= 32;
                    }

                    auto &t = tp->GetTransform();

                    primitive.root_ref = instanceRef;
                    primitive.start_transform = t.StartTransform()->GetMatrix();
                    primitive.end_transform = t.EndTransform()->GetMatrix();
                    primitive.start_time = t.StartTime();
                    primitive.end_time = t.EndTime();

                    writer->write(reinterpret_cast<const char *>(&primitive),
                                  sizeof(primitive));
                }
            }

            // write all triangles for the node
            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];

                if (prim->GetType() != PrimitiveType::Transformed) {
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint32_t sMeshID = triMeshIDs.at(mesh);
                    int origTriNum = (tri->v - mesh->vertexIndices) / 3;
                    int newTriNum = triNumRemap.at(mesh).at(origTriNum);

                    triangle.mesh_id = sMeshID;
                    triangle.tri_number = newTriNum;

                    writer->write(reinterpret_cast<const char *>(&triangle),
                                  sizeof(triangle));
                }
            }
        }

        // Write out primitives for instances
        for (TreeletDumpBVH *inst : treelet.instances) {
            for (uint64_t nodeIdx = 0; nodeIdx < inst->nodeCount; nodeIdx++) {
                const LinearBVHNode &instNode = inst->nodes[nodeIdx];

                uint32_t transformed_primitive_count = 0;
                uint32_t triangle_count = instNode.nPrimitives;

                writer->write(transformed_primitive_count);
                writer->write(triangle_count);

                for (int primIdx = 0; primIdx < instNode.nPrimitives;
                     primIdx++) {
                    auto &prim =
                        inst->primitives[instNode.primitivesOffset + primIdx];
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    CHECK_NOTNULL(gp.get());
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint32_t sMeshID = triMeshIDs.at(mesh);
                    int triNum = (tri->v - mesh->vertexIndices) / 3;

                    triangle.mesh_id = sMeshID;
                    triangle.tri_number = triNum;

                    writer->write(reinterpret_cast<const char *>(&triangle),
                                  sizeof(triangle));
                }
            }
        }
    }

    if (root) {
        ofstream staticAllocOut(_manager.getScenePath() + "/STATIC0_pre");
        for (const TreeletInfo &treelet : allTreelets) {
            uint32_t sTreeletID = _manager.getId(&treelet);
            staticAllocOut << sTreeletID << " " << treelet.totalProb << endl;
        }

        for (auto &kv : nonCopyableInstanceTreelets) {
            const TreeletDumpBVH *inst = kv.first;
            for (const TreeletInfo &treelet : inst->allTreelets) {
                float instProb =
                    instanceProbabilities[treelet.dirIdx][inst->instanceID];

                uint32_t sTreeletID = _manager.getId(&treelet);
                staticAllocOut << sTreeletID << " "
                               << treelet.totalProb * instProb << endl;
            }
        }
    }

    bool multiDir = false;
    for (const TreeletInfo &info : allTreelets) {
        if (info.dirIdx != 0) {
            multiDir = true;
            break;
        }
    }

    int numRoots = multiDir ? 8 : 1;

    vector<uint32_t> rootTreelets;
    for (int i = 0; i < numRoots; i++) {
        rootTreelets.push_back(_manager.getId(&allTreelets[i]));
    }

    return rootTreelets;
}

}  // namespace pbrt
