
/*
    pbrt source code is Copyright(c) 1998-2016
                        Matt Pharr, Greg Humphreys, and Wenzel Jakob.

    This file is part of pbrt.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:

    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */


// accelerators/bvh.cpp*
#include "accelerators/bvh.h"
#include "interaction.h"
#include "paramset.h"
#include "stats.h"
#include "parallel.h"
#include <algorithm>
#include <vector>
#include <queue>
#include <stack>
#include <fstream>
#include <tuple>
#include <limits>
#include <set>
#include <memory>

#include "cloud/manager.h"
#include "messages/utils.h"
#include "pbrt.pb.h"

namespace pbrt {

STAT_MEMORY_COUNTER("Memory/BVH tree", treeBytes);
STAT_RATIO("BVH/Primitives per leaf node", totalPrimitives, totalLeafNodes);
STAT_COUNTER("BVH/Interior nodes", interiorNodes);
STAT_COUNTER("BVH/Leaf nodes", leafNodes);

void BVHBuildNode::InitLeaf(int first, int n, const Bounds3f &b) {
    firstPrimOffset = first;
    nPrimitives = n;
    bounds = b;
    children[0] = children[1] = nullptr;
    ++leafNodes;
    ++totalLeafNodes;
    totalPrimitives += n;
}

void BVHBuildNode::InitInterior(int axis, BVHBuildNode *c0, BVHBuildNode *c1) {
    children[0] = c0;
    children[1] = c1;
    bounds = Union(c0->bounds, c1->bounds);
    splitAxis = axis;
    nPrimitives = 0;
    ++interiorNodes;
}

// BVHAccel Utility Functions
inline uint32_t LeftShift3(uint32_t x) {
    CHECK_LE(x, (1 << 10));
    if (x == (1 << 10)) --x;
#ifdef PBRT_HAVE_BINARY_CONSTANTS
    x = (x | (x << 16)) & 0b00000011000000000000000011111111;
    // x = ---- --98 ---- ---- ---- ---- 7654 3210
    x = (x | (x << 8)) & 0b00000011000000001111000000001111;
    // x = ---- --98 ---- ---- 7654 ---- ---- 3210
    x = (x | (x << 4)) & 0b00000011000011000011000011000011;
    // x = ---- --98 ---- 76-- --54 ---- 32-- --10
    x = (x | (x << 2)) & 0b00001001001001001001001001001001;
    // x = ---- 9--8 --7- -6-- 5--4 --3- -2-- 1--0
#else
    x = (x | (x << 16)) & 0x30000ff;
    // x = ---- --98 ---- ---- ---- ---- 7654 3210
    x = (x | (x << 8)) & 0x300f00f;
    // x = ---- --98 ---- ---- 7654 ---- ---- 3210
    x = (x | (x << 4)) & 0x30c30c3;
    // x = ---- --98 ---- 76-- --54 ---- 32-- --10
    x = (x | (x << 2)) & 0x9249249;
    // x = ---- 9--8 --7- -6-- 5--4 --3- -2-- 1--0
#endif // PBRT_HAVE_BINARY_CONSTANTS
    return x;
}

inline uint32_t EncodeMorton3(const Vector3f &v) {
    CHECK_GE(v.x, 0);
    CHECK_GE(v.y, 0);
    CHECK_GE(v.z, 0);
    return (LeftShift3(v.z) << 2) | (LeftShift3(v.y) << 1) | LeftShift3(v.x);
}

static void RadixSort(std::vector<MortonPrimitive> *v) {
    std::vector<MortonPrimitive> tempVector(v->size());
    PBRT_CONSTEXPR int bitsPerPass = 6;
    PBRT_CONSTEXPR int nBits = 30;
    static_assert((nBits % bitsPerPass) == 0,
                  "Radix sort bitsPerPass must evenly divide nBits");
    PBRT_CONSTEXPR int nPasses = nBits / bitsPerPass;

    for (int pass = 0; pass < nPasses; ++pass) {
        // Perform one pass of radix sort, sorting _bitsPerPass_ bits
        int lowBit = pass * bitsPerPass;

        // Set in and out vector pointers for radix sort pass
        std::vector<MortonPrimitive> &in = (pass & 1) ? tempVector : *v;
        std::vector<MortonPrimitive> &out = (pass & 1) ? *v : tempVector;

        // Count number of zero bits in array for current radix sort bit
        PBRT_CONSTEXPR int nBuckets = 1 << bitsPerPass;
        int bucketCount[nBuckets] = {0};
        PBRT_CONSTEXPR int bitMask = (1 << bitsPerPass) - 1;
        for (const MortonPrimitive &mp : in) {
            int bucket = (mp.mortonCode >> lowBit) & bitMask;
            CHECK_GE(bucket, 0);
            CHECK_LT(bucket, nBuckets);
            ++bucketCount[bucket];
        }

        // Compute starting index in output array for each bucket
        int outIndex[nBuckets];
        outIndex[0] = 0;
        for (int i = 1; i < nBuckets; ++i)
            outIndex[i] = outIndex[i - 1] + bucketCount[i - 1];

        // Store sorted values in output array
        for (const MortonPrimitive &mp : in) {
            int bucket = (mp.mortonCode >> lowBit) & bitMask;
            out[outIndex[bucket]++] = mp;
        }
    }
    // Copy final result from _tempVector_, if needed
    if (nPasses & 1) std::swap(*v, tempVector);
}

// BVHAccel Method Definitions
BVHAccel::BVHAccel(std::vector<std::shared_ptr<Primitive>> p,
                   int maxPrimsInNode, SplitMethod splitMethod)
    : maxPrimsInNode(std::min(255, maxPrimsInNode)),
      splitMethod(splitMethod),
      primitives(std::move(p)) {
    ProfilePhase _(Prof::AccelConstruction);
    if (primitives.empty()) return;
    // Build BVH from _primitives_

    // Initialize _primitiveInfo_ array for primitives
    std::vector<BVHPrimitiveInfo> primitiveInfo(primitives.size());
    for (size_t i = 0; i < primitives.size(); ++i)
        primitiveInfo[i] = {i, primitives[i]->WorldBound()};

    // Build BVH tree for primitives using _primitiveInfo_
    int totalNodes = 0;
    MemoryArena arena(1024 * 1024);
    std::vector<std::shared_ptr<Primitive>> orderedPrims;
    orderedPrims.reserve(primitives.size());
    BVHBuildNode *root;
    if (splitMethod == SplitMethod::HLBVH)
        root = HLBVHBuild(arena, primitiveInfo, &totalNodes, orderedPrims);
    else
        root = recursiveBuild(arena, primitiveInfo, 0, primitives.size(),
                              &totalNodes, orderedPrims);
    primitives.swap(orderedPrims);
    primitiveInfo.resize(0);
    LOG(INFO) << StringPrintf("BVH created with %d nodes for %d "
                              "primitives (%.2f MB), arena allocated %.2f MB",
                              totalNodes, (int)primitives.size(),
                              float(totalNodes * sizeof(LinearBVHNode)) /
                              (1024.f * 1024.f),
                              float(arena.TotalAllocated()) /
                              (1024.f * 1024.f));

    // Compute representation of depth-first traversal of BVH tree
    treeBytes += totalNodes * sizeof(LinearBVHNode) + sizeof(*this) +
                 primitives.size() * sizeof(primitives[0]);
    nodes = AllocAligned<LinearBVHNode>(totalNodes);
    int offset = 0;
    nodeCount = totalNodes;
    flattenBVHTree(root, &offset);
    CHECK_EQ(totalNodes, offset);
}

Bounds3f BVHAccel::WorldBound() const {
    return nodes ? nodes[0].bounds : Bounds3f();
}

struct BucketInfo {
    int count = 0;
    Bounds3f bounds;
};

BVHBuildNode *BVHAccel::recursiveBuild(
    MemoryArena &arena, std::vector<BVHPrimitiveInfo> &primitiveInfo, int start,
    int end, int *totalNodes,
    std::vector<std::shared_ptr<Primitive>> &orderedPrims) {
    CHECK_NE(start, end);
    BVHBuildNode *node = arena.Alloc<BVHBuildNode>();
    (*totalNodes)++;
    // Compute bounds of all primitives in BVH node
    Bounds3f bounds;
    for (int i = start; i < end; ++i)
        bounds = Union(bounds, primitiveInfo[i].bounds);
    int nPrimitives = end - start;
    if (nPrimitives == 1) {
        // Create leaf _BVHBuildNode_
        int firstPrimOffset = orderedPrims.size();
        for (int i = start; i < end; ++i) {
            int primNum = primitiveInfo[i].primitiveNumber;
            orderedPrims.push_back(primitives[primNum]);
        }
        node->InitLeaf(firstPrimOffset, nPrimitives, bounds);
        return node;
    } else {
        // Compute bound of primitive centroids, choose split dimension _dim_
        Bounds3f centroidBounds;
        for (int i = start; i < end; ++i)
            centroidBounds = Union(centroidBounds, primitiveInfo[i].centroid);
        int dim = centroidBounds.MaximumExtent();

        // Partition primitives into two sets and build children
        int mid = (start + end) / 2;
        if (centroidBounds.pMax[dim] == centroidBounds.pMin[dim]) {
            // Create leaf _BVHBuildNode_
            int firstPrimOffset = orderedPrims.size();
            for (int i = start; i < end; ++i) {
                int primNum = primitiveInfo[i].primitiveNumber;
                orderedPrims.push_back(primitives[primNum]);
            }
            node->InitLeaf(firstPrimOffset, nPrimitives, bounds);
            return node;
        } else {
            // Partition primitives based on _splitMethod_
            switch (splitMethod) {
            case SplitMethod::Middle: {
                // Partition primitives through node's midpoint
                Float pmid =
                    (centroidBounds.pMin[dim] + centroidBounds.pMax[dim]) / 2;
                BVHPrimitiveInfo *midPtr = std::partition(
                    &primitiveInfo[start], &primitiveInfo[end - 1] + 1,
                    [dim, pmid](const BVHPrimitiveInfo &pi) {
                        return pi.centroid[dim] < pmid;
                    });
                mid = midPtr - &primitiveInfo[0];
                // For lots of prims with large overlapping bounding boxes, this
                // may fail to partition; in that case don't break and fall
                // through
                // to EqualCounts.
                if (mid != start && mid != end) break;
            }
            case SplitMethod::EqualCounts: {
                // Partition primitives into equally-sized subsets
                mid = (start + end) / 2;
                std::nth_element(&primitiveInfo[start], &primitiveInfo[mid],
                                 &primitiveInfo[end - 1] + 1,
                                 [dim](const BVHPrimitiveInfo &a,
                                       const BVHPrimitiveInfo &b) {
                                     return a.centroid[dim] < b.centroid[dim];
                                 });
                break;
            }
            case SplitMethod::SAH:
            default: {
                // Partition primitives using approximate SAH
                if (nPrimitives <= 2) {
                    // Partition primitives into equally-sized subsets
                    mid = (start + end) / 2;
                    std::nth_element(&primitiveInfo[start], &primitiveInfo[mid],
                                     &primitiveInfo[end - 1] + 1,
                                     [dim](const BVHPrimitiveInfo &a,
                                           const BVHPrimitiveInfo &b) {
                                         return a.centroid[dim] <
                                                b.centroid[dim];
                                     });
                } else {
                    // Allocate _BucketInfo_ for SAH partition buckets
                    PBRT_CONSTEXPR int nBuckets = 12;
                    BucketInfo buckets[nBuckets];

                    // Initialize _BucketInfo_ for SAH partition buckets
                    for (int i = start; i < end; ++i) {
                        int b = nBuckets *
                                centroidBounds.Offset(
                                    primitiveInfo[i].centroid)[dim];
                        if (b == nBuckets) b = nBuckets - 1;
                        CHECK_GE(b, 0);
                        CHECK_LT(b, nBuckets);
                        buckets[b].count++;
                        buckets[b].bounds =
                            Union(buckets[b].bounds, primitiveInfo[i].bounds);
                    }

                    // Compute costs for splitting after each bucket
                    Float cost[nBuckets - 1];
                    for (int i = 0; i < nBuckets - 1; ++i) {
                        Bounds3f b0, b1;
                        int count0 = 0, count1 = 0;
                        for (int j = 0; j <= i; ++j) {
                            b0 = Union(b0, buckets[j].bounds);
                            count0 += buckets[j].count;
                        }
                        for (int j = i + 1; j < nBuckets; ++j) {
                            b1 = Union(b1, buckets[j].bounds);
                            count1 += buckets[j].count;
                        }
                        cost[i] = 1 +
                                  (count0 * b0.SurfaceArea() +
                                   count1 * b1.SurfaceArea()) /
                                      bounds.SurfaceArea();
                    }

                    // Find bucket to split at that minimizes SAH metric
                    Float minCost = cost[0];
                    int minCostSplitBucket = 0;
                    for (int i = 1; i < nBuckets - 1; ++i) {
                        if (cost[i] < minCost) {
                            minCost = cost[i];
                            minCostSplitBucket = i;
                        }
                    }

                    // Either create leaf or split primitives at selected SAH
                    // bucket
                    Float leafCost = nPrimitives;
                    if (nPrimitives > maxPrimsInNode || minCost < leafCost) {
                        BVHPrimitiveInfo *pmid = std::partition(
                            &primitiveInfo[start], &primitiveInfo[end - 1] + 1,
                            [=](const BVHPrimitiveInfo &pi) {
                                int b = nBuckets *
                                        centroidBounds.Offset(pi.centroid)[dim];
                                if (b == nBuckets) b = nBuckets - 1;
                                CHECK_GE(b, 0);
                                CHECK_LT(b, nBuckets);
                                return b <= minCostSplitBucket;
                            });
                        mid = pmid - &primitiveInfo[0];
                    } else {
                        // Create leaf _BVHBuildNode_
                        int firstPrimOffset = orderedPrims.size();
                        for (int i = start; i < end; ++i) {
                            int primNum = primitiveInfo[i].primitiveNumber;
                            orderedPrims.push_back(primitives[primNum]);
                        }
                        node->InitLeaf(firstPrimOffset, nPrimitives, bounds);
                        return node;
                    }
                }
                break;
            }
            }
            node->InitInterior(dim,
                               recursiveBuild(arena, primitiveInfo, start, mid,
                                              totalNodes, orderedPrims),
                               recursiveBuild(arena, primitiveInfo, mid, end,
                                              totalNodes, orderedPrims));
        }
    }
    return node;
}

BVHBuildNode *BVHAccel::HLBVHBuild(
    MemoryArena &arena, const std::vector<BVHPrimitiveInfo> &primitiveInfo,
    int *totalNodes,
    std::vector<std::shared_ptr<Primitive>> &orderedPrims) const {
    // Compute bounding box of all primitive centroids
    Bounds3f bounds;
    for (const BVHPrimitiveInfo &pi : primitiveInfo)
        bounds = Union(bounds, pi.centroid);

    // Compute Morton indices of primitives
    std::vector<MortonPrimitive> mortonPrims(primitiveInfo.size());
    ParallelFor([&](int i) {
        // Initialize _mortonPrims[i]_ for _i_th primitive
        PBRT_CONSTEXPR int mortonBits = 10;
        PBRT_CONSTEXPR int mortonScale = 1 << mortonBits;
        mortonPrims[i].primitiveIndex = primitiveInfo[i].primitiveNumber;
        Vector3f centroidOffset = bounds.Offset(primitiveInfo[i].centroid);
        mortonPrims[i].mortonCode = EncodeMorton3(centroidOffset * mortonScale);
    }, primitiveInfo.size(), 512);

    // Radix sort primitive Morton indices
    RadixSort(&mortonPrims);

    // Create LBVH treelets at bottom of BVH

    // Find intervals of primitives for each treelet
    std::vector<LBVHTreelet> treeletsToBuild;
    for (int start = 0, end = 1; end <= (int)mortonPrims.size(); ++end) {
#ifdef PBRT_HAVE_BINARY_CONSTANTS
      uint32_t mask = 0b00111111111111000000000000000000;
#else
      uint32_t mask = 0x3ffc0000;
#endif
      if (end == (int)mortonPrims.size() ||
            ((mortonPrims[start].mortonCode & mask) !=
             (mortonPrims[end].mortonCode & mask))) {
            // Add entry to _treeletsToBuild_ for this treelet
            int nPrimitives = end - start;
            int maxBVHNodes = 2 * nPrimitives;
            BVHBuildNode *nodes = arena.Alloc<BVHBuildNode>(maxBVHNodes, false);
            treeletsToBuild.push_back({start, nPrimitives, nodes});
            start = end;
        }
    }

    // Create LBVHs for treelets in parallel
    std::atomic<int> atomicTotal(0), orderedPrimsOffset(0);
    orderedPrims.resize(primitives.size());
    ParallelFor([&](int i) {
        // Generate _i_th LBVH treelet
        int nodesCreated = 0;
        const int firstBitIndex = 29 - 12;
        LBVHTreelet &tr = treeletsToBuild[i];
        tr.buildNodes =
            emitLBVH(tr.buildNodes, primitiveInfo, &mortonPrims[tr.startIndex],
                     tr.nPrimitives, &nodesCreated, orderedPrims,
                     &orderedPrimsOffset, firstBitIndex);
        atomicTotal += nodesCreated;
    }, treeletsToBuild.size());
    *totalNodes = atomicTotal;

    // Create and return SAH BVH from LBVH treelets
    std::vector<BVHBuildNode *> finishedTreelets;
    finishedTreelets.reserve(treeletsToBuild.size());
    for (LBVHTreelet &treelet : treeletsToBuild)
        finishedTreelets.push_back(treelet.buildNodes);
    return buildUpperSAH(arena, finishedTreelets, 0, finishedTreelets.size(),
                         totalNodes);
}

BVHBuildNode *BVHAccel::emitLBVH(
    BVHBuildNode *&buildNodes,
    const std::vector<BVHPrimitiveInfo> &primitiveInfo,
    MortonPrimitive *mortonPrims, int nPrimitives, int *totalNodes,
    std::vector<std::shared_ptr<Primitive>> &orderedPrims,
    std::atomic<int> *orderedPrimsOffset, int bitIndex) const {
    CHECK_GT(nPrimitives, 0);
    if (bitIndex == -1 || nPrimitives < maxPrimsInNode) {
        // Create and return leaf node of LBVH treelet
        (*totalNodes)++;
        BVHBuildNode *node = buildNodes++;
        Bounds3f bounds;
        int firstPrimOffset = orderedPrimsOffset->fetch_add(nPrimitives);
        for (int i = 0; i < nPrimitives; ++i) {
            int primitiveIndex = mortonPrims[i].primitiveIndex;
            orderedPrims[firstPrimOffset + i] = primitives[primitiveIndex];
            bounds = Union(bounds, primitiveInfo[primitiveIndex].bounds);
        }
        node->InitLeaf(firstPrimOffset, nPrimitives, bounds);
        return node;
    } else {
        int mask = 1 << bitIndex;
        // Advance to next subtree level if there's no LBVH split for this bit
        if ((mortonPrims[0].mortonCode & mask) ==
            (mortonPrims[nPrimitives - 1].mortonCode & mask))
            return emitLBVH(buildNodes, primitiveInfo, mortonPrims, nPrimitives,
                            totalNodes, orderedPrims, orderedPrimsOffset,
                            bitIndex - 1);

        // Find LBVH split point for this dimension
        int searchStart = 0, searchEnd = nPrimitives - 1;
        while (searchStart + 1 != searchEnd) {
            CHECK_NE(searchStart, searchEnd);
            int mid = (searchStart + searchEnd) / 2;
            if ((mortonPrims[searchStart].mortonCode & mask) ==
                (mortonPrims[mid].mortonCode & mask))
                searchStart = mid;
            else {
                CHECK_EQ(mortonPrims[mid].mortonCode & mask,
                         mortonPrims[searchEnd].mortonCode & mask);
                searchEnd = mid;
            }
        }
        int splitOffset = searchEnd;
        CHECK_LE(splitOffset, nPrimitives - 1);
        CHECK_NE(mortonPrims[splitOffset - 1].mortonCode & mask,
                 mortonPrims[splitOffset].mortonCode & mask);

        // Create and return interior LBVH node
        (*totalNodes)++;
        BVHBuildNode *node = buildNodes++;
        BVHBuildNode *lbvh[2] = {
            emitLBVH(buildNodes, primitiveInfo, mortonPrims, splitOffset,
                     totalNodes, orderedPrims, orderedPrimsOffset,
                     bitIndex - 1),
            emitLBVH(buildNodes, primitiveInfo, &mortonPrims[splitOffset],
                     nPrimitives - splitOffset, totalNodes, orderedPrims,
                     orderedPrimsOffset, bitIndex - 1)};
        int axis = bitIndex % 3;
        node->InitInterior(axis, lbvh[0], lbvh[1]);
        return node;
    }
}

BVHBuildNode *BVHAccel::buildUpperSAH(MemoryArena &arena,
                                      std::vector<BVHBuildNode *> &treeletRoots,
                                      int start, int end,
                                      int *totalNodes) const {
    CHECK_LT(start, end);
    int nNodes = end - start;
    if (nNodes == 1) return treeletRoots[start];
    (*totalNodes)++;
    BVHBuildNode *node = arena.Alloc<BVHBuildNode>();

    // Compute bounds of all nodes under this HLBVH node
    Bounds3f bounds;
    for (int i = start; i < end; ++i)
        bounds = Union(bounds, treeletRoots[i]->bounds);

    // Compute bound of HLBVH node centroids, choose split dimension _dim_
    Bounds3f centroidBounds;
    for (int i = start; i < end; ++i) {
        Point3f centroid =
            (treeletRoots[i]->bounds.pMin + treeletRoots[i]->bounds.pMax) *
            0.5f;
        centroidBounds = Union(centroidBounds, centroid);
    }
    int dim = centroidBounds.MaximumExtent();
    // FIXME: if this hits, what do we need to do?
    // Make sure the SAH split below does something... ?
    CHECK_NE(centroidBounds.pMax[dim], centroidBounds.pMin[dim]);

    // Allocate _BucketInfo_ for SAH partition buckets
    PBRT_CONSTEXPR int nBuckets = 12;
    struct BucketInfo {
        int count = 0;
        Bounds3f bounds;
    };
    BucketInfo buckets[nBuckets];

    // Initialize _BucketInfo_ for HLBVH SAH partition buckets
    for (int i = start; i < end; ++i) {
        Float centroid = (treeletRoots[i]->bounds.pMin[dim] +
                          treeletRoots[i]->bounds.pMax[dim]) *
                         0.5f;
        int b =
            nBuckets * ((centroid - centroidBounds.pMin[dim]) /
                        (centroidBounds.pMax[dim] - centroidBounds.pMin[dim]));
        if (b == nBuckets) b = nBuckets - 1;
        CHECK_GE(b, 0);
        CHECK_LT(b, nBuckets);
        buckets[b].count++;
        buckets[b].bounds = Union(buckets[b].bounds, treeletRoots[i]->bounds);
    }

    // Compute costs for splitting after each bucket
    Float cost[nBuckets - 1];
    for (int i = 0; i < nBuckets - 1; ++i) {
        Bounds3f b0, b1;
        int count0 = 0, count1 = 0;
        for (int j = 0; j <= i; ++j) {
            b0 = Union(b0, buckets[j].bounds);
            count0 += buckets[j].count;
        }
        for (int j = i + 1; j < nBuckets; ++j) {
            b1 = Union(b1, buckets[j].bounds);
            count1 += buckets[j].count;
        }
        cost[i] = .125f +
                  (count0 * b0.SurfaceArea() + count1 * b1.SurfaceArea()) /
                      bounds.SurfaceArea();
    }

    // Find bucket to split at that minimizes SAH metric
    Float minCost = cost[0];
    int minCostSplitBucket = 0;
    for (int i = 1; i < nBuckets - 1; ++i) {
        if (cost[i] < minCost) {
            minCost = cost[i];
            minCostSplitBucket = i;
        }
    }

    // Split nodes and create interior HLBVH SAH node
    BVHBuildNode **pmid = std::partition(
        &treeletRoots[start], &treeletRoots[end - 1] + 1,
        [=](const BVHBuildNode *node) {
            Float centroid =
                (node->bounds.pMin[dim] + node->bounds.pMax[dim]) * 0.5f;
            int b = nBuckets *
                    ((centroid - centroidBounds.pMin[dim]) /
                     (centroidBounds.pMax[dim] - centroidBounds.pMin[dim]));
            if (b == nBuckets) b = nBuckets - 1;
            CHECK_GE(b, 0);
            CHECK_LT(b, nBuckets);
            return b <= minCostSplitBucket;
        });
    int mid = pmid - &treeletRoots[0];
    CHECK_GT(mid, start);
    CHECK_LT(mid, end);
    node->InitInterior(
        dim, this->buildUpperSAH(arena, treeletRoots, start, mid, totalNodes),
        this->buildUpperSAH(arena, treeletRoots, mid, end, totalNodes));
    return node;
}

int BVHAccel::flattenBVHTree(BVHBuildNode *node, int *offset) {
    LinearBVHNode *linearNode = &nodes[*offset];
    linearNode->bounds = node->bounds;
    int myOffset = (*offset)++;
    if (node->nPrimitives > 0) {
        CHECK(!node->children[0] && !node->children[1]);
        CHECK_LT(node->nPrimitives, 65536);
        linearNode->primitivesOffset = node->firstPrimOffset;
        linearNode->nPrimitives = node->nPrimitives;
    } else {
        // Create interior flattened BVH node
        linearNode->axis = node->splitAxis;
        linearNode->nPrimitives = 0;
        flattenBVHTree(node->children[0], offset);
        linearNode->secondChildOffset =
            flattenBVHTree(node->children[1], offset);
    }
    return myOffset;
}

BVHAccel::~BVHAccel() { FreeAligned(nodes); }

bool BVHAccel::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    int nodesToVisit[64];
    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->nPrimitives; ++i)
                    if (primitives[node->primitivesOffset + i]->Intersect(
                            ray, isect))
                        hit = true;
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
    }
    return hit;
}

bool BVHAccel::IntersectP(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int nodesToVisit[64];
    int toVisitOffset = 0, currentNodeIndex = 0;
    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            // Process BVH node _node_ for traversal
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i) {
                    if (primitives[node->primitivesOffset + i]->IntersectP(
                            ray)) {
                        return true;
                    }
                }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
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
    }
    return false;
}

std::shared_ptr<BVHAccel> CreateBVHAccelerator(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps) {
    std::string splitMethodName = ps.FindOneString("splitmethod", "sah");
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
    auto res = std::make_shared<BVHAccel>(std::move(prims), maxPrimsInNode, splitMethod);

    const bool root_bvh = ps.FindOneBool("sceneaccelerator", false);
    if (PbrtOptions.dumpScene and root_bvh) {
        const int dump_node_size = ps.FindOneInt("dumptreeletsize", 50000);
        res->Dump(dump_node_size);
    }

    return res;
}

void BVHAccel::assignTreelets(uint32_t * labels, const uint32_t max_nodes) const {
    /* pass one */
    const uint32_t INSTANCE_SIZE = 1;
    const uint32_t TRIANGLE_SIZE = 3;

    std::unique_ptr<uint32_t[]> subtree_footprint(new uint32_t[nodeCount]);
    std::unique_ptr<float []> best_costs(new float[nodeCount]);

    const float AREA_EPSILON = nodes[0].bounds.SurfaceArea() * max_nodes / (nodeCount * 10);

    for (int root_index = nodeCount - 1; root_index >= 0; root_index--) {
        const LinearBVHNode & root_node = nodes[root_index];

        if (root_node.nPrimitives) { /* leaf */
            /* determine the footprint of the node by adding up the size of all
             * primitives */
            uint32_t footprint = 0;
            for (int i = 0; i < root_node.nPrimitives; i++) {
                auto &primitive = primitives[root_node.primitivesOffset + i];
                if (primitive->GetType() == PrimitiveType::Transformed) {
                    /* it's an instance */
                    footprint += INSTANCE_SIZE;
                } else if (primitive->GetType() == PrimitiveType::Geometric) {
                    /* it's a shape+material */
                    std::shared_ptr<GeometricPrimitive> gp =
                        std::dynamic_pointer_cast<GeometricPrimitive>(
                            primitive);

                    const Shape *shape = gp->GetShape();
                    if (shape->GetType() != ShapeType::Triangle) {
                        throw std::runtime_error(
                            "only Triangles are supported");
                    }

                    footprint += TRIANGLE_SIZE;
                }
            }
            subtree_footprint[root_index] = footprint;
        } else {
            subtree_footprint[root_index] = 1 + subtree_footprint[root_index + 1]
                                              + subtree_footprint[root_node.secondChildOffset];
        }

        std::set<int> cut;
        cut.insert(root_index);
        uint32_t remaining_size = max_nodes;
        best_costs[root_index] = std::numeric_limits<float>::max();

        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint32_t price = std::min(subtree_footprint[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            remaining_size--;
            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }
            best_costs[root_index] = std::min(best_costs[root_index], this_cost);
        }
    }

    auto float_equals = [](const float a, const float b) {
        return fabs(a - b) < 1e-4;
    };

    uint32_t current_treelet = 0;

    std::stack<int> q;
    q.push(0);

    int node_count = 0;

    while (not q.empty()) {
        const int root_index = q.top();
        q.pop();

        current_treelet++;

        const LinearBVHNode & root_node = nodes[root_index];
        std::set<int> cut;
        cut.insert(root_index);

        uint32_t remaining_size = max_nodes;
        const float best_cost = best_costs[root_index];

        float cost = 0;
        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint32_t price = std::min(subtree_footprint[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            labels[best_node_index] = current_treelet;
            remaining_size--;

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }

            if (float_equals(this_cost, best_cost)) {
                break;
            }
        }

        for (const auto n : cut) {
            q.push(n);
        }
    }

    /* make sure all of the nodes have a treelet */
    /* for (int i = 0; i < nodeCount; i++) {
        if (not labels[i]) {
            throw std::runtime_error("unassigned node");
        }
    } */
}

uint32_t BVHAccel::dumpTreelets(uint32_t *labels,
                                const size_t max_treelet_nodes) const {
    std::map<BVHAccel *, size_t> bvh_instances;
    const uint32_t bvh_root_id =
        global::manager.getNextId(ObjectType::Treelet, &nodes[0]);

    for (int root_index = 0; root_index < nodeCount; root_index++) {
        if (not labels[root_index]) {
            continue; /* we've already written this node to disk */
        }

        const size_t treelet_id = global::manager.getId(&nodes[root_index]);

        // For each triangle mesh in each treelet, determine the set of triangle
        // idxs from that mesh which are in the treelet
        // triangle mesh -> vec of triangle ids
        std::map<TriangleMesh *, std::vector<size_t>> triangles_in_treelet;
        // Keep track of the materials for each triangle mesh
        std::map<TriangleMesh *, const Material *> triangle_mesh_material;

        // First pass through the treelet to determine which triangles we use
        // from which meshes so that we can split the meshes into chunks.
        {
            const uint32_t current_treelet = labels[root_index];
            std::stack<int> q;
            q.push(root_index);

            while (not q.empty()) {
                const int node_index = q.top();
                q.pop();

                if (node_index == -1) {
                    continue;
                }

                const LinearBVHNode &node = nodes[node_index];
                labels[node_index] = 0; /* it's dumped */

                if (node.nPrimitives > 0) {
                    /* sorry about the disgusting code */
                    for (int i = 0; i < node.nPrimitives; i++) {
                        auto &primitive = primitives[node.primitivesOffset + i];
                        if (primitive->GetType() ==
                            PrimitiveType::Transformed) {
                            /* it's an instance, ignore in first pass */
                        } else if (primitive->GetType() ==
                                   PrimitiveType::Geometric) {
                            /* it's a shape+material */
                            std::shared_ptr<GeometricPrimitive> gp =
                                std::dynamic_pointer_cast<GeometricPrimitive>(
                                    primitive);

                            const Shape *shape = gp->GetShape();
                            if (shape->GetType() != ShapeType::Triangle) {
                                throw std::runtime_error(
                                    "only Triangles are supported");
                            }

                            const Triangle *triangle =
                                dynamic_cast<const Triangle *>(shape);

                            TriangleMesh *mesh = triangle->mesh.get();

                            const int tri_num =
                                (triangle->v -
                                 triangle->mesh->vertexIndices.data()) /
                                3;

                            if (tri_num < 0 ||
                                (tri_num * 3) >=
                                    triangle->mesh->vertexIndices.size()) {
                                throw std::runtime_error(
                                    "invalid triangle number");
                            }

                            triangles_in_treelet[mesh].push_back(tri_num);

                            if (triangle_mesh_material.count(mesh) == 0) {
                                triangle_mesh_material[mesh] =
                                    gp->GetMaterial();
                            }
                        }
                    }
                    continue;
                }

                if (labels[node.secondChildOffset] != current_treelet) {
                    global::manager.getNextId(ObjectType::Treelet,
                                              &nodes[node.secondChildOffset]);
                    q.push(-1);
                } else {
                    q.push(node.secondChildOffset);
                }

                if (labels[node_index + 1] != current_treelet) {
                    global::manager.getNextId(ObjectType::Treelet,
                                              &nodes[node_index + 1]);
                    q.push(-1);
                } else {
                    q.push(node_index + 1);
                }
            }
        }

        /* get the material ids */
        std::map<TriangleMesh *, int> triangle_mesh_material_ids;
        for (auto& kv : triangle_mesh_material) {
          TriangleMesh* mesh = kv.first;
          const Material* material = kv.second;

          const uint32_t material_id =
              PbrtOptions.dumpMaterials ? global::manager.getId(material) : 0;

          triangle_mesh_material_ids[mesh] = material_id;
        }

        auto writer =
            global::manager.GetWriter(ObjectType::Treelet, treelet_id);

        uint32_t num_triangle_meshes = triangles_in_treelet.size();
        writer->write(num_triangle_meshes);

        /* Split up triangle meshes. Currently we only split the meshes so that
         all triangles in the treelet are included.
         TODO(apoms): Split triangle meshes up based on the footprint of the
         triangles + their textures */
        std::map<TriangleMesh *, std::map<size_t, uint32_t>> triangle_mesh_ids;
        std::map<TriangleMesh *, std::map<size_t, std::shared_ptr<TriangleMesh>>> triangle_to_sub_mesh;
        std::map<TriangleMesh *, std::map<size_t, size_t>> triangle_to_sub_mesh_triangle;
        for (auto& kv : triangles_in_treelet) {
          TriangleMesh* mesh = kv.first;
          std::vector<size_t> &triangle_nums = kv.second;

          const uint32_t material_id = triangle_mesh_material_ids[mesh];

          /* assign each mesh a unique id */
          const uint32_t tm_id =
              global::manager.getNextId(ObjectType::TriangleMesh);
          /* generate the sub-mesh */
          std::shared_ptr<TriangleMesh> sub_mesh;
          {
            size_t num_triangles = triangle_nums.size();
            /* Determine number of vertices and assign contiguous unique ids to each
               vertex to use as vertex indices for new sub mesh */
            size_t next_triangle_id = 0;
            size_t next_vertex_id = 0;
            std::map<int, size_t> vertices_used;
            for (size_t i = 0; i < num_triangles; ++i) {
              size_t triangle_num = triangle_nums[i];
              for (int j = 0; j < 3; ++j) {
                int vertex_offset = mesh->vertexIndices[triangle_num * 3 + j];
                if (vertices_used.count(vertex_offset) == 0) {
                  vertices_used[vertex_offset] = next_vertex_id++;
                }
              }
              triangle_to_sub_mesh_triangle[mesh][triangle_num] = next_triangle_id++;
            }
            size_t num_vertices = vertices_used.size();

            /* Fill in triangle mesh data for the sub mesh */
            std::vector<int> vertex_indices(num_triangles * 3);
            std::vector<Point3f> P(num_vertices);
            std::vector<Vector3f> S(num_vertices);
            std::vector<Normal3f> N(num_vertices);
            std::vector<Point2f> uv(num_vertices);
            std::vector<int> face_indices(num_triangles);

            for (size_t i = 0; i < num_triangles; ++i) {
              size_t triangle_num = triangle_nums[i];
              for (int j = 0; j < 3; ++j) {
                  int orig_vertex_offset =
                      mesh->vertexIndices[triangle_num * 3 + j];
                  int new_vertex_offset = vertices_used.at(orig_vertex_offset);
                  vertex_indices[i * 3 + j] = new_vertex_offset;
              }
              if (mesh->faceIndices.size() > 0) {
                  face_indices[i] = mesh->faceIndices[triangle_num];
              }
            }
            for (auto& kv : vertices_used) {
              int orig_vertex_offset = kv.first;
              int new_vertex_offset = kv.second;
              P[new_vertex_offset] = mesh->p[orig_vertex_offset];
              if (mesh->s.get()) {
                  S[new_vertex_offset] = mesh->s[orig_vertex_offset];
              }
              if (mesh->n.get()) {
                  N[new_vertex_offset] = mesh->n[orig_vertex_offset];
              }
              if (mesh->uv.get()) {
                  uv[new_vertex_offset] = mesh->uv[orig_vertex_offset];
              }
            }
            sub_mesh = std::make_shared<TriangleMesh>(
                Transform(), num_triangles, vertex_indices.data(), num_vertices,
                P.data(), mesh->s.get() ? S.data() : nullptr,
                mesh->n.get() ? N.data() : nullptr,
                mesh->uv.get() ? uv.data() : nullptr, mesh->alphaMask,
                mesh->shadowAlphaMask,
                mesh->faceIndices.size() > 0 ? face_indices.data() : nullptr);
          }

          for (size_t triangle_num : triangle_nums) {
              triangle_mesh_ids[mesh][triangle_num] = tm_id;
              triangle_to_sub_mesh[mesh][triangle_num] = sub_mesh;
          }

          /* write out the sub mesh */
          protobuf::TriangleMesh tm_proto = to_protobuf(*sub_mesh);
          tm_proto.set_id(tm_id);
          tm_proto.set_material_id(material_id);
          writer->write(tm_proto);

          /* track the dependency of the mesh on the material */
          global::manager.recordDependency(
              ObjectKey{ObjectType::Treelet, treelet_id},
              ObjectKey{ObjectType::Material, material_id});

          /* track the dependency of the treelet on the mesh */
          /* global::manager.recordDependency(
              ObjectKey{ObjectType::Treelet, treelet_id},
              ObjectKey{ObjectType::TriangleMesh, tm_id}); */
        }

        const uint32_t current_treelet = labels[root_index];
        std::stack<int> q;
        q.push(root_index);

        while (not q.empty()) {
            const int node_index = q.top();
            q.pop();

            if (node_index == -1) {
                writer->write_empty();
                continue;
            }

            const LinearBVHNode & node = nodes[node_index];
            labels[node_index] = 0; /* it's dumped */

            protobuf::BVHNode proto_node;
            *proto_node.mutable_bounds() = to_protobuf(node.bounds);
            proto_node.set_axis(node.axis);

            if (node.nPrimitives > 0) {
                /* sorry about the disgusting code */
                for (int i = 0; i < node.nPrimitives; i++) {
                    auto &primitive = primitives[node.primitivesOffset + i];
                    if (primitive->GetType() == PrimitiveType::Transformed) {
                        /* it's an instance */
                        std::shared_ptr<TransformedPrimitive> tp =
                            std::dynamic_pointer_cast<TransformedPrimitive>(primitive);

                        if (tp != nullptr and tp->GetBaseType() == PrimitiveType::Aggregate) {
                            std::shared_ptr<BVHAccel> bvh =
                                std::dynamic_pointer_cast<BVHAccel>(tp->GetPrimitive());

                            if (bvh == nullptr) {
                                throw std::runtime_error("primitive is not a BVH");
                            }

                            if (not bvh_instances.count(bvh.get())) {
                                bvh_instances[bvh.get()] = bvh->Dump(max_treelet_nodes);
                            }

                            protobuf::TransformedPrimitive proto_tp;
                            proto_tp.set_root_ref(bvh_instances[bvh.get()]);
                            *proto_tp.mutable_transform() = to_protobuf(tp->GetTransform());

                            *proto_node.add_transformed_primitives() = proto_tp;
                        }
                    }
                    else if (primitive->GetType() == PrimitiveType::Geometric) {
                        /* it's a shape+material */
                        std::shared_ptr<GeometricPrimitive> gp =
                            std::dynamic_pointer_cast<GeometricPrimitive>(primitive);

                        const Shape *shape = gp->GetShape();
                        if (shape->GetType() != ShapeType::Triangle) {
                            throw std::runtime_error("only Triangles are supported");
                        }

                        const Triangle *triangle = dynamic_cast<const Triangle *>(shape);

                        TriangleMesh* mesh = triangle->mesh.get();

                        const int tri_num = (triangle->v - mesh->vertexIndices.data()) / 3;
                        if (tri_num < 0 || (tri_num * 3) >= mesh->vertexIndices.size()) {
                            throw std::runtime_error("invalid triangle number");
                        }

                        const int tm_id = triangle_mesh_ids.at(mesh).at(tri_num);
                        TriangleMesh* sub_mesh = triangle_to_sub_mesh.at(mesh).at(tri_num).get();

                        const int sub_mesh_tri_num = triangle_to_sub_mesh_triangle.at(mesh).at(tri_num);

                        protobuf::Triangle triangle_proto;
                        triangle_proto.set_mesh_id(tm_id);
                        triangle_proto.set_tri_number(sub_mesh_tri_num);
                        *proto_node.add_triangles() = triangle_proto;
                    }
                }

                writer->write(proto_node);
                writer->write_empty();
                writer->write_empty();
                continue;
            }

            if (labels[node.secondChildOffset] != current_treelet) {
                proto_node.set_right_ref(global::manager.getId(&nodes[node.secondChildOffset]));
                q.push(-1);
            }
            else {
                q.push(node.secondChildOffset);
            }

            if (labels[node_index + 1] != current_treelet) {
                proto_node.set_left_ref(global::manager.getId(&nodes[node_index + 1]));
                q.push(-1);
            }
            else {
                q.push(node_index + 1);
            }

            writer->write(proto_node);
        }

    }

    return bvh_root_id;
}

uint32_t BVHAccel::Dump(const size_t max_treelet_nodes) const {
    std::cerr << "Dumping BVH...\n";

    /* (1) assign each node to a treelet */
    std::unique_ptr<uint32_t[]> treelet_labels {new uint32_t[nodeCount]};
    memset(treelet_labels.get(), 0, nodeCount * sizeof(uint32_t));
    assignTreelets(treelet_labels.get(), max_treelet_nodes);

    /* (2) dump the treelets to disk */
    return dumpTreelets(treelet_labels.get(), max_treelet_nodes);
}

}
