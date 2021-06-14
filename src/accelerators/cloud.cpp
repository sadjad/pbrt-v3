#include "cloud.h"

#include <fstream>
#include <memory>
#include <stack>
#include <thread>

#include "bvh.h"
#include "cloud/manager.h"
#include "core/parallel.h"
#include "core/paramset.h"
#include "core/primitive.h"
#include "materials/matte.h"
#include "messages/lite.h"
#include "messages/serdes.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "shapes/triangle.h"

using namespace std;

namespace pbrt {

STAT_COUNTER("BVH/Total nodes", nNodes);
STAT_COUNTER("BVH/Visited nodes", nNodesVisited);
STAT_COUNTER("BVH/Visited primitives", nPrimitivesVisited);

struct membuf : streambuf {
    membuf(char *begin, char *end) { this->setg(begin, begin, end); }
};

CloudBVH::CloudBVH(const uint32_t bvh_root, const bool preload_all)
    : bvh_root_(bvh_root) {
    ProfilePhase _(Prof::AccelConstruction);

    if (MaxThreadIndex() > 1 && !preload_all) {
        throw runtime_error(
            "Cannot use lazy-loading CloudBVH with multiple threads");
    }

    unique_ptr<Float[]> color(new Float[3]);
    color[0] = 0.f;
    color[1] = 0.5;
    color[2] = 0.f;

    ParamSet emptyParams;
    ParamSet params;
    params.AddRGBSpectrum("Kd", move(color), 3);

    map<string, shared_ptr<Texture<Float>>> fTex;
    map<string, shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams textureParams(params, emptyParams, fTex, sTex);
    default_material.reset(CreateMatteMaterial(textureParams));

    if (preload_all) {
        /* (1) load all the treelets in parallel */
        const auto treelet_count = global::manager.treeletCount();

        treelets_.resize(treelet_count + 1);

        ParallelFor([&](int64_t treelet_id) { loadTreeletBase(treelet_id); },
                    treelet_count);

        /* (2.A) load all the necessary materials */
        set<uint32_t> required_materials;

        for (size_t i = 0; i < treelet_count; i++) {
            required_materials.insert(treelets_[i]->required_materials.begin(),
                                      treelets_[i]->required_materials.end());
        }

        for (const auto mid : required_materials) {
            if (materials_.count(mid) == 0) {
                auto r = global::manager.GetReader(ObjectType::Material, mid);
                protobuf::Material material;
                r->read(&material);
                materials_[mid] = material::from_protobuf(material);
            }
        }

        /* (2.B) create all the necessary external instances */
        set<uint64_t> required_instances;

        for (size_t i = 0; i < treelet_count; i++) {
            required_instances.insert(treelets_[i]->required_instances.begin(),
                                      treelets_[i]->required_instances.end());
        }

        for (const auto rid : required_instances) {
            if (not bvh_instances_.count(rid)) {
                bvh_instances_[rid] =
                    make_shared<ExternalInstance>(*this, (uint16_t)(rid >> 32));
            }
        }

        /* (3) finish loading the treelets */
        ParallelFor(
            [&](int64_t treelet_id) { finializeTreeletLoad(treelet_id); },
            treelet_count);

        preloading_done_ = true;
    }
}

CloudBVH::~CloudBVH() {}

Bounds3f CloudBVH::WorldBound() const {
    // The correctness of this function is only guaranteed for the root treelet
    CHECK_EQ(bvh_root_, 0);

    LoadTreelet(bvh_root_);
    return treelets_[bvh_root_]->nodes[0].bounds;
}

// Sums the full surface area for each root. Does not account for overlap
// between roots
Float CloudBVH::RootSurfaceAreas(Transform txfm) const {
    LoadTreelet(bvh_root_);
    CHECK_EQ(treelets_.size(), 1);

    Float area = 0;

    vector<Bounds3f> roots;

    for (const TreeletNode &node : treelets_[bvh_root_]->nodes) {
        auto cur = txfm(node.bounds);

        bool newRoot = true;
        for (const Bounds3f &root : roots) {
            auto u = Union(root, cur);
            if (u == root) {
                newRoot = false;
                break;
            }
        }

        if (newRoot) {
            roots.push_back(cur);
            area += cur.SurfaceArea();
        }
    }

    return area;
}

Float CloudBVH::SurfaceAreaUnion() const {
    LoadTreelet(bvh_root_);
    CHECK_EQ(treelets_.size(), 1);

    Bounds3f boundUnion;
    for (const TreeletNode &node : treelets_[bvh_root_]->nodes) {
        boundUnion = Union(boundUnion, node.bounds);
    }

    return boundUnion.SurfaceArea();
}

void CloudBVH::LoadTreelet(const uint32_t root_id, istream *stream) const {
    if (preloading_done_ or
        (treelets_.size() > root_id && treelets_[root_id] != nullptr)) {
        return; /* this tree is already loaded */
    }

    if (treelets_.size() <= root_id) {
        treelets_.resize(root_id + 1);
    }

    loadTreeletBase(root_id, stream);

    auto &treelet = *treelets_[root_id];

    /* load the materials */
    for (const auto mid : treelet.required_materials) {
        if (materials_.count(mid) == 0) {
            auto reader = global::manager.GetReader(ObjectType::Material, mid);
            protobuf::Material material;
            reader->read(&material);
            materials_[mid] = material::from_protobuf(material);
        }
    }

    /* create the instances */
    for (const auto rid : treelet.required_instances) {
        if (not bvh_instances_.count(rid)) {
            bvh_instances_[rid] =
                make_shared<ExternalInstance>(*this, (uint16_t)(rid >> 32));
        }
    }

    finializeTreeletLoad(root_id);
}

void CloudBVH::finializeTreeletLoad(const uint32_t root_id) const {
    auto &treelet = *treelets_[root_id];

    /* fill in unfinished primitives */
    for (auto &u : treelet.unfinished_transformed) {
        treelet.primitives[u.primitive_index] =
            make_unique<TransformedPrimitive>(bvh_instances_[u.instance_ref],
                                              move(u.primitive_to_world));
    }

    MediumInterface medium_interface{};

    for (auto &u : treelet.unfinished_geometric) {
        treelet.primitives[u.primitive_index] = make_unique<GeometricPrimitive>(
            move(u.shape), materials_[u.material_id], nullptr,
            medium_interface);
    }

    treelet.required_instances.clear();
    treelet.required_materials.clear();
    treelet.unfinished_geometric.clear();
    treelet.unfinished_transformed.clear();
}

void CloudBVH::loadTreeletBase(const uint32_t root_id, istream *stream) const {
    ProfilePhase _(Prof::LoadTreelet);

    vector<TreeletNode> nodes;

    if (stream != nullptr) {
        throw runtime_error("not implemented");
    }

    vector<char> treelet_buffer;
    {
        const string treelet_path =
            global::manager.getScenePath() + "/" +
            global::manager.getFileName(ObjectType::Treelet, root_id);

        ifstream fin{treelet_path, ios::binary | ios::ate};
        streamsize size = fin.tellg();
        fin.seekg(0, ios::beg);

        treelet_buffer.resize(size);
        fin.read(treelet_buffer.data(), size);
    }

    LiteRecordReader reader{treelet_buffer.data(), treelet_buffer.size()};

    treelets_[root_id] = make_unique<Treelet>();

    auto &treelet = *treelets_[root_id];
    auto &tree_meshes = treelet.meshes;
    auto &tree_primitives = treelet.primitives;
    auto &tree_transforms = treelet.transforms;
    auto &tree_instances = treelet.instances;

    map<uint32_t, uint32_t> mesh_material_ids;

    /* read in the triangle meshes for this treelet first */
    uint32_t num_triangle_meshes = 0;
    reader.read(&num_triangle_meshes);

    for (int i = 0; i < num_triangle_meshes; ++i) {
        /* load the TriangleMesh if necessary */
        uint64_t tm_id;
        uint64_t material_id;

        reader.read(&tm_id);
        reader.read(&material_id);

        const char *tm_buffer;
        size_t tm_buffer_len;
        reader.read(&tm_buffer, &tm_buffer_len);

        auto p = tree_meshes.emplace(
            tm_id,
            make_shared<TriangleMesh>(move(
                serdes::triangle_mesh::deserialize(tm_buffer, tm_buffer_len))));

        CHECK_EQ(p.second, true);
        mesh_material_ids[tm_id] = material_id;
    }

    uint32_t node_count;
    uint32_t primitive_count;

    reader.read(&node_count);
    reader.read(&primitive_count);

    nodes.reserve(node_count);
    tree_primitives.reserve(primitive_count);

    stack<pair<uint32_t, Child>> q;

    while (not reader.eof()) {
        const serdes::cloudbvh::Node *serdes_node;
        bool success =
            reader.read(reinterpret_cast<const char **>(&serdes_node), nullptr);
        CHECK_EQ(success, true);

        TreeletNode node(serdes_node->bounds, serdes_node->axis);
        const uint32_t index = nodes.size();

        if (not q.empty()) {
            auto parent = q.top();
            q.pop();

            nodes[parent.first].child_treelet[parent.second] = root_id;
            nodes[parent.first].child_node[parent.second] = index;
        }

        bool is_leaf = serdes_node->transformed_primitives_count ||
                       serdes_node->triangles_count;

        if (serdes_node->right_ref) {
            uint64_t right_ref = serdes_node->right_ref;
            uint16_t treelet_id = (uint16_t)(right_ref >> 32);
            node.child_treelet[RIGHT] = treelet_id;
            node.child_node[RIGHT] = (uint32_t)right_ref;
        } else if (!is_leaf) {
            q.emplace(index, RIGHT);
        }

        if (serdes_node->left_ref) {
            uint64_t left_ref = serdes_node->left_ref;
            uint16_t treelet_id = (uint16_t)(left_ref >> 32);
            node.child_treelet[LEFT] = treelet_id;
            node.child_node[LEFT] = (uint32_t)left_ref;
        } else if (!is_leaf) {
            q.emplace(index, LEFT);
        }

        if (is_leaf) {
            node.leaf_tag = ~0;
            node.primitive_offset = tree_primitives.size();
            node.primitive_count = serdes_node->transformed_primitives_count +
                                   serdes_node->triangles_count;
        }

        const serdes::cloudbvh::TransformedPrimitive *serdes_primitive;
        const serdes::cloudbvh::Triangle *serdes_triangle;

        for (int i = 0; i < serdes_node->transformed_primitives_count; i++) {
            reader.read(reinterpret_cast<const char **>(&serdes_primitive),
                        nullptr);

            tree_transforms.push_back(move(
                make_unique<Transform>(serdes_primitive->start_transform)));
            const Transform *start = tree_transforms.back().get();

            const Transform *end;
            if (start->GetMatrix() != serdes_primitive->end_transform) {
                tree_transforms.push_back(move(
                    make_unique<Transform>(serdes_primitive->end_transform)));
                end = tree_transforms.back().get();
            } else {
                end = start;
            }

            AnimatedTransform primitive_to_world{
                start, serdes_primitive->start_time, end,
                serdes_primitive->end_time};

            uint64_t instance_ref = serdes_primitive->root_ref;

            uint16_t instance_group = (uint16_t)(instance_ref >> 32);
            uint32_t instance_node = (uint32_t)instance_ref;

            if (instance_group == root_id) {
                if (not tree_instances.count(instance_ref)) {
                    tree_instances[instance_ref] =
                        make_shared<IncludedInstance>(&treelet, instance_node);
                }

                tree_primitives.push_back(make_unique<TransformedPrimitive>(
                    tree_instances[instance_ref], primitive_to_world));
            } else {
                treelet.required_instances.insert(instance_ref);

                treelet.unfinished_transformed.emplace_back(
                    tree_primitives.size(), instance_ref,
                    move(primitive_to_world));

                tree_primitives.push_back(nullptr);
            }
        }

        for (int i = 0; i < serdes_node->triangles_count; i++) {
            reader.read(reinterpret_cast<const char **>(&serdes_triangle),
                        nullptr);

            const auto mesh_id = serdes_triangle->mesh_id;
            const auto tri_number = serdes_triangle->tri_number;
            const auto material_id = mesh_material_ids[mesh_id];
            treelet.required_materials.insert(material_id);

            auto shape = make_unique<Triangle>(
                &identity_transform_, &identity_transform_, false,
                tree_meshes.at(mesh_id), tri_number);

            treelet.unfinished_geometric.emplace_back(tree_primitives.size(),
                                                      material_id, move(shape));

            tree_primitives.push_back(nullptr);
        }

        nodes.emplace_back(move(node));
        nNodes++;
    }

    treelet.nodes = move(nodes);
}

void CloudBVH::Trace(RayState &rayState) const {
    SurfaceInteraction isect;

    RayDifferential ray = rayState.ray;
    Vector3f invDir{1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z};
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    const uint32_t currentTreelet = rayState.toVisitTop().treelet;
    LoadTreelet(currentTreelet); /* we don't load any other treelets */

    bool hasTransform = false;
    bool transformChanged = false;

    while (true) {
        auto &top = rayState.toVisitTop();
        if (currentTreelet != top.treelet) {
            break;
        }

        RayState::TreeletNode current = move(top);
        rayState.toVisitPop();
        nNodesVisited++;

        auto &treelet = *treelets_[current.treelet];
        auto &node = treelet.nodes[current.node];

        /* prepare the ray */
        if (current.transformed != hasTransform || transformChanged) {
            transformChanged = false;

            ray = current.transformed
                      ? Inverse(rayState.rayTransform)(rayState.ray)
                      : rayState.ray;

            invDir = Vector3f{1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z};
            dirIsNeg[0] = invDir.x < 0;
            dirIsNeg[1] = invDir.y < 0;
            dirIsNeg[2] = invDir.z < 0;
        }

        hasTransform = current.transformed;

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;

                for (int i = node.primitive_offset + current.primitive;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    nPrimitivesVisited++;

                    if (primitives[i]->GetType() ==
                        PrimitiveType::Transformed) {
                        TransformedPrimitive *tp =
                            dynamic_cast<TransformedPrimitive *>(
                                primitives[i].get());

                        shared_ptr<ExternalInstance> cbvh =
                            dynamic_pointer_cast<ExternalInstance>(
                                tp->GetPrimitive());

                        if (cbvh) {
                            if (current.primitive + 1 < node.primitive_count) {
                                RayState::TreeletNode next_primitive = current;
                                next_primitive.primitive++;
                                rayState.toVisitPush(move(next_primitive));
                            }

                            Transform txfm;
                            tp->GetTransform().Interpolate(ray.time, &txfm);

                            RayState::TreeletNode next;
                            next.treelet = cbvh->RootID();
                            next.node = 0;

                            if (txfm.IsIdentity()) {
                                next.transformed = false;
                            } else {
                                rayState.rayTransform = txfm;
                                next.transformed = true;
                            }
                            rayState.toVisitPush(move(next));
                            break;
                        }

                        shared_ptr<IncludedInstance> included =
                            dynamic_pointer_cast<IncludedInstance>(
                                tp->GetPrimitive());
                        if (included) {
                            if (tp->Intersect(ray, &isect)) {
                                rayState.ray.tMax = ray.tMax;
                                rayState.SetHit(current);
                            }
                        }
                    } else if (primitives[i]->Intersect(ray, &isect)) {
                        rayState.ray.tMax = ray.tMax;
                        rayState.SetHit(current);
                    }

                    current.primitive++;
                }

                if (rayState.toVisitEmpty()) break;
            } else {
                RayState::TreeletNode children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].treelet = node.child_treelet[i];
                    children[i].node = node.child_node[i];
                    children[i].transformed = current.transformed;
                }

                if (dirIsNeg[node.axis]) {
                    rayState.toVisitPush(move(children[LEFT]));
                    rayState.toVisitPush(move(children[RIGHT]));
                } else {
                    rayState.toVisitPush(move(children[RIGHT]));
                    rayState.toVisitPush(move(children[LEFT]));
                }
            }
        } else {
            if (rayState.toVisitEmpty()) break;
        }
    }
}

bool CloudBVH::Intersect(RayState &rayState, SurfaceInteraction *isect) const {
    if (!rayState.hit) {
        return false;
    }

    auto &hit = rayState.hitNode;
    LoadTreelet(hit.treelet);

    auto &treelet = *treelets_[hit.treelet];
    auto &node = treelet.nodes[hit.node];
    auto &primitives = treelet.primitives;

    if (!node.is_leaf()) {
        return false;
    }

    Ray ray = rayState.ray;

    if (hit.transformed) {
        ray = Inverse(rayState.hitTransform)(ray);
    }

    primitives[node.primitive_offset + hit.primitive]->Intersect(ray, isect);
    rayState.ray.tMax = ray.tMax;

    if (hit.transformed && !rayState.hitTransform.IsIdentity()) {
        *isect = (rayState.hitTransform)(*isect);
    }

    return true;
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    return Intersect(ray, isect, bvh_root_);
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect,
                         const uint32_t bvh_root) const {
    ProfilePhase _(Prof::AccelIntersect);

    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    pair<uint32_t, uint32_t> toVisit[64];
    uint8_t toVisitOffset = 0;

    uint32_t startTreelet = bvh_root;
    if (bvh_root == 0) {
        startTreelet = ComputeIdx(ray.d);
    }

    pair<uint32_t, uint32_t> current(startTreelet, 0);

    uint32_t prevTreelet = startTreelet;
    while (true) {
        LoadTreelet(current.first);
        auto &treelet = *treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    if (primitives[i]->Intersect(ray, isect)) hit = true;
                }

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first = node.child_treelet[i];
                    children[i].second = node.child_node[i];
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                } else {
                    toVisit[toVisitOffset++] = children[RIGHT];
                    current = children[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = toVisit[--toVisitOffset];
        }

        prevTreelet = current.first;
    }

    return hit;
}

bool CloudBVH::IntersectP(const Ray &ray) const {
    return IntersectP(ray, bvh_root_);
}

bool CloudBVH::IntersectP(const Ray &ray, const uint32_t bvh_root) const {
    ProfilePhase _(Prof::AccelIntersectP);

    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    uint8_t toVisitOffset = 0;
    pair<uint32_t, uint32_t> toVisit[64];

    uint32_t startTreelet = bvh_root;
    if (bvh_root == 0) {
        startTreelet = ComputeIdx(ray.d);
    }

    pair<uint32_t, uint32_t> current(startTreelet, 0);

    uint32_t prevTreelet = startTreelet;
    while (true) {
        LoadTreelet(current.first);
        auto &treelet = *treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    if (primitives[i]->IntersectP(ray)) return true;
                }

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first = node.child_treelet[i];
                    children[i].second = node.child_node[i];
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                } else {
                    toVisit[toVisitOffset++] = children[RIGHT];
                    current = children[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = toVisit[--toVisitOffset];
        }

        prevTreelet = current.first;
    }

    return false;
}

vector<Bounds3f> CloudBVH::getTreeletNodeBounds(
    const uint32_t treelet_id, const int recursionLimit) const {
    return vector<Bounds3f>();
#if 0
    LoadTreelet(treelet_id);

    vector<Bounds3f> treeletBounds;

    const int depth = 0;
    const int idx = 1;

    // load base node bounds
    auto &currTreelet = *treelets_.at(treelet_id);
    auto &currNode = currTreelet.nodes[0];

    // size reflects indexing starting at 1
    const size_t size = pow(2, recursionLimit);
    treeletBounds.resize(size);
    recurseBVHNodes(depth, recursionLimit, idx, currTreelet, currNode,
                    treeletBounds);

    return treeletBounds;
#endif
}

void CloudBVH::recurseBVHNodes(const int depth, const int recursionLimit,
                               const int idx, const Treelet &currTreelet,
                               const TreeletNode &currNode,
                               vector<Bounds3f> &treeletBounds) const {
// FIXME Update for multi root treelets
#if 0
    if (depth == recursionLimit) {
        return;
    }

    // save the current node
    treeletBounds[idx] = currNode.bounds;

    // save left value, if there's one
    if (currNode.has[0]) {
        const uint32_t left = currNode.child[0];
        recurseBVHNodes(depth + 1, recursionLimit, 2 * idx, currTreelet,
                        currTreelet.nodes[left], treeletBounds);
    }

    // save right value, if there's one
    if (currNode.has[1]) {
        const uint32_t right = currNode.child[1];
        recurseBVHNodes(depth + 1, recursionLimit, 2 * idx + 1, currTreelet,
                        currTreelet.nodes[right], treeletBounds);
    }
#endif
}

void CloudBVH::clear() const {
    treelets_.clear();
    bvh_instances_.clear();
    materials_.clear();
}

shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps) {
    const bool preload = ps.FindOneBool("preload", false);
    return make_shared<CloudBVH>(0, preload);
}

Bounds3f CloudBVH::IncludedInstance::WorldBound() const {
    return treelet_->nodes[nodeIdx_].bounds;
}

bool CloudBVH::IncludedInstance::Intersect(const Ray &ray,
                                           SurfaceInteraction *isect) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = nodeIdx_;
    int nodesToVisit[64];
    while (true) {
        const CloudBVH::TreeletNode *node = &treelet_->nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->is_leaf()) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->primitive_count; ++i)
                    if (treelet_->primitives[node->primitive_offset + i]
                            ->Intersect(ray, isect))
                        hit = true;
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = node->child_node[LEFT];
                    currentNodeIndex = node->child_node[RIGHT];
                } else {
                    nodesToVisit[toVisitOffset++] = node->child_node[RIGHT];
                    currentNodeIndex = node->child_node[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }
    }
    return hit;
}

bool CloudBVH::IncludedInstance::IntersectP(const Ray &ray) const {
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int toVisitOffset = 0, currentNodeIndex = nodeIdx_;
    int nodesToVisit[64];

    while (true) {
        const CloudBVH::TreeletNode *node = &treelet_->nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->is_leaf()) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->primitive_count; ++i)
                    if (treelet_->primitives[node->primitive_offset + i]
                            ->IntersectP(ray)) {
                        return true;
                    }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = node->child_node[LEFT];
                    currentNodeIndex = node->child_node[RIGHT];
                } else {
                    nodesToVisit[toVisitOffset++] = node->child_node[RIGHT];
                    currentNodeIndex = node->child_node[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }
    }

    return false;
}

Vector3f ComputeRayDir(unsigned idx) {
    unsigned x = idx & (1 << 0);
    unsigned y = idx & (1 << 1);
    unsigned z = idx & (1 << 2);

    return Vector3f(x ? 1 : -1, y ? 1 : -1, z ? 1 : -1);
}

unsigned ComputeIdx(const Vector3f &dir) {
    if (PbrtOptions.directionalTreelets) {
        return (dir.x >= 0 ? 1 : 0) + ((dir.y >= 0 ? 1 : 0) << 1) +
               ((dir.z >= 0 ? 1 : 0) << 2);
    } else {
        return 0;
    }
}

}  // namespace pbrt
