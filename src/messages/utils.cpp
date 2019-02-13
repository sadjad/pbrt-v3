#include "utils.h"

#include <chrono>
#include <memory>
#include <stdexcept>

#include "cameras/environment.h"
#include "cameras/orthographic.h"
#include "cameras/perspective.h"
#include "cameras/realistic.h"
#include "core/api.h"
#include "core/api_makefns.h"
#include "core/api.h"
#include "filters/box.h"
#include "filters/gaussian.h"
#include "filters/mitchell.h"
#include "filters/sinc.h"
#include "filters/triangle.h"
#include "lights/distant.h"
#include "lights/goniometric.h"
#include "lights/infinite.h"
#include "lights/point.h"
#include "lights/projection.h"
#include "lights/spot.h"
#include "samplers/halton.h"
#include "samplers/maxmin.h"
#include "samplers/random.h"
#include "samplers/sobol.h"
#include "samplers/stratified.h"
#include "samplers/zerotwosequence.h"
#include "shapes/fake.h"
#include "cloud/manager.h"

using namespace std;
using namespace std::chrono;

namespace pbrt {

protobuf::Point2i to_protobuf(const Point2i& point) {
    protobuf::Point2i proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    return proto_point;
}

protobuf::Point2f to_protobuf(const Point2f& point) {
    protobuf::Point2f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    return proto_point;
}

protobuf::Point3f to_protobuf(const Point3f& point) {
    protobuf::Point3f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    proto_point.set_z(point.z);
    return proto_point;
}

protobuf::Vector2f to_protobuf(const Vector2f& vector) {
    protobuf::Vector2f proto_vector;
    proto_vector.set_x(vector.x);
    proto_vector.set_y(vector.y);
    return proto_vector;
}

protobuf::Vector3f to_protobuf(const Vector3f& vector) {
    protobuf::Vector3f proto_vector;
    proto_vector.set_x(vector.x);
    proto_vector.set_y(vector.y);
    proto_vector.set_z(vector.z);
    return proto_vector;
}

protobuf::Normal3f to_protobuf(const Normal3f& normal) {
    protobuf::Normal3f proto_normal;
    proto_normal.set_x(normal.x);
    proto_normal.set_y(normal.y);
    proto_normal.set_z(normal.z);
    return proto_normal;
}

protobuf::Bounds2i to_protobuf(const Bounds2i& bounds) {
    protobuf::Bounds2i proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Bounds2f to_protobuf(const Bounds2f& bounds) {
    protobuf::Bounds2f proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Bounds3f to_protobuf(const Bounds3f& bounds) {
    protobuf::Bounds3f proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Matrix to_protobuf(const Matrix4x4& matrix) {
    protobuf::Matrix proto_matrix;
    for (size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4; j++) {
            proto_matrix.add_m(matrix.m[i][j]);
        }
    }
    return proto_matrix;
}

protobuf::RGBSpectrum to_protobuf(const RGBSpectrum& spectrum) {
    protobuf::RGBSpectrum proto_spectrum;
    proto_spectrum.add_c(spectrum[0]);
    proto_spectrum.add_c(spectrum[1]);
    proto_spectrum.add_c(spectrum[2]);
    return proto_spectrum;
}

protobuf::RayDifferential to_protobuf(const RayDifferential& ray) {
    protobuf::RayDifferential proto_ray;

    *proto_ray.mutable_o() = to_protobuf(ray.o);
    *proto_ray.mutable_d() = to_protobuf(ray.d);
    proto_ray.set_t_max(ray.tMax);
    proto_ray.set_time(ray.time);

    proto_ray.set_has_differentials(ray.hasDifferentials);
    if (ray.hasDifferentials) {
        *proto_ray.mutable_rx_origin() = to_protobuf(ray.rxOrigin);
        *proto_ray.mutable_ry_origin() = to_protobuf(ray.ryOrigin);
        *proto_ray.mutable_rx_direction() = to_protobuf(ray.rxDirection);
        *proto_ray.mutable_ry_direction() = to_protobuf(ray.ryDirection);
    }

    return proto_ray;
}

protobuf::AnimatedTransform to_protobuf(const AnimatedTransform& transform) {
    protobuf::AnimatedTransform proto_transform;
    *proto_transform.mutable_start_transform() =
        to_protobuf(transform.startTransform->GetMatrix());
    *proto_transform.mutable_end_transform() =
        to_protobuf(transform.endTransform->GetMatrix());
    proto_transform.set_start_time(transform.startTime);
    proto_transform.set_end_time(transform.endTime);
    return proto_transform;
}

protobuf::TriangleMesh to_protobuf(const TriangleMesh& tm) {
    protobuf::TriangleMesh proto_tm;
    proto_tm.set_n_triangles(tm.nTriangles);
    proto_tm.set_n_vertices(tm.nVertices);

    for (size_t i = 0; i < tm.nTriangles; i++) {
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i]);
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i + 1]);
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i + 2]);
    }

    for (size_t i = 0; i < tm.nVertices; i++) {
        *proto_tm.add_p() = to_protobuf(tm.p[i]);
    }

    if (tm.uv) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_uv() = to_protobuf(tm.uv[i]);
        }
    }

    if (tm.n) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_n() = to_protobuf(tm.n[i]);
        }
    }

    if (tm.s) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_s() = to_protobuf(tm.s[i]);
        }
    }

    return proto_tm;
}

protobuf::VisitNode to_protobuf(const RayState::TreeletNode& node) {
    protobuf::VisitNode proto_visit;
    proto_visit.set_treelet(node.treelet);
    proto_visit.set_node(node.node);
    if (node.transform) {
        *proto_visit.mutable_transform() =
            to_protobuf(node.transform->GetMatrix());
    }
    return proto_visit;
}

protobuf::RayState to_protobuf(const RayState& state) {
    protobuf::RayState proto_state;
    proto_state.set_sample_id(state.sample.id);
    proto_state.set_sample_num(state.sample.num);
    *proto_state.mutable_sample_pixel() = to_protobuf(state.sample.pixel);
    *proto_state.mutable_sample_p_film() = to_protobuf(state.sample.pFilm);
    proto_state.set_sample_weight(state.sample.weight);
    *proto_state.mutable_ray() = to_protobuf(state.ray);

    for (auto& tv : state.toVisit) {
        *proto_state.add_to_visit() = to_protobuf(tv);
    }

    if (state.hit.initialized()) {
        *proto_state.mutable_hit() = to_protobuf(*state.hit);
    }

    *proto_state.mutable_beta() = to_protobuf(state.beta);
    *proto_state.mutable_ld() = to_protobuf(state.Ld);
    proto_state.set_bounces(state.bounces);
    proto_state.set_remaining_bounces(state.remainingBounces);
    proto_state.set_is_shadow_ray(state.isShadowRay);

    return proto_state;
}

protobuf::SampleData to_protobuf(const CloudIntegrator::SampleData& sample) {
    protobuf::SampleData proto_sample;
    *proto_sample.mutable_p_film() = to_protobuf(sample.sample.pFilm);
    proto_sample.set_weight(sample.weight);
    return proto_sample;
}

protobuf::ParamSet to_protobuf(const ParamSet& ps) {
    protobuf::ParamSet proto_params;

    auto ap = [](auto& proto_item, const auto& item) {
        proto_item.set_name(item->name);
        for (size_t i = 0; i < item->nValues; i++) {
            proto_item.add_values(item->values[i]);
        }
    };

    auto ao = [](auto& proto_item, const auto& item) {
        proto_item.set_name(item->name);
        for (size_t i = 0; i < item->nValues; i++) {
            *proto_item.add_values() = to_protobuf(item->values[i]);
        }
    };

    // clang-format off
    for (const auto& i : ps.bools)     { ap(*proto_params.add_bools(), i); }
    for (const auto& i : ps.ints)      { ap(*proto_params.add_ints(), i); }
    for (const auto& i : ps.floats)    { ap(*proto_params.add_floats(), i); }
    for (const auto& i : ps.point2fs)  { ao(*proto_params.add_point2fs(), i); }
    for (const auto& i : ps.vector2fs) { ao(*proto_params.add_vector2fs(), i); }
    for (const auto& i : ps.point3fs)  { ao(*proto_params.add_point3fs(), i); }
    for (const auto& i : ps.vector3fs) { ao(*proto_params.add_vector3fs(), i); }
    for (const auto& i : ps.point2fs)  { ao(*proto_params.add_point2fs(), i); }
    for (const auto& i : ps.normals)   { ao(*proto_params.add_normals(), i); }
    for (const auto& i : ps.spectra)   { ao(*proto_params.add_spectra(), i); }
    for (const auto& i : ps.strings)   { ap(*proto_params.add_strings(), i); }
    for (const auto& i : ps.textures)  { ap(*proto_params.add_textures(), i); }
    // clang-format on

    return proto_params;
}

protobuf::Scene to_protobuf(const Scene& scene) {
    protobuf::Scene proto_scene;
    *proto_scene.mutable_world_bound() = to_protobuf(scene.WorldBound());
    return proto_scene;
}

protobuf::TextureParams to_protobuf(const TextureParams& texture_params) {
  protobuf::TextureParams params;
  *params.mutable_geom_params() = to_protobuf(texture_params.GetGeomParams());
  *params.mutable_material_params() = to_protobuf(texture_params.GetMaterialParams());
  for (auto& kv : texture_params.GetFloatTextures()) {
    int id = global::manager.getId(kv.second.get());
    (*params.mutable_float_textures())[kv.first] = id;
  }
  for (auto& kv : texture_params.GetSpectrumTextures()) {
    int id = global::manager.getId(kv.second.get());
    (*params.mutable_spectrum_textures())[kv.first] = id;
  }
  return params;
}

protobuf::ObjectKey to_protobuf(
    const SceneManager::ObjectKey& ObjectKey) {
    protobuf::ObjectKey proto;
    proto.set_type(to_underlying(ObjectKey.type));
    proto.set_id(ObjectKey.id);
    return proto;
}

protobuf::RayStats to_protobuf(const RayStats& stats) {
  protobuf::RayStats proto;
  proto.set_sent_rays(stats.sentRays);
  proto.set_received_rays(stats.receivedRays);
  proto.set_waiting_rays(stats.waitingRays);
  proto.set_processed_rays(stats.processedRays);
  proto.set_demanded_rays(stats.demandedRays);
  for (double d : stats.traceDurationPercentiles) {
    proto.add_trace_duration_percentiles(d);
  }
  for (double d : stats.rayDurations) {
    proto.add_ray_durations(d);
  }
  return proto;
}

protobuf::QueueStats to_protobuf(const QueueStats& stats) {
    protobuf::QueueStats proto;
    proto.set_ray(stats.ray);
    proto.set_finished(stats.finished);
    proto.set_pending(stats.pending);
    proto.set_out(stats.out);
    proto.set_connecting(stats.connecting);
    proto.set_connected(stats.connected);
    return proto;
}

protobuf::WorkerStats to_protobuf(const WorkerStats& stats) {
    protobuf::WorkerStats proto;
    proto.set_finished_paths(stats._finishedPaths);
    (*proto.mutable_aggregate_stats()) = to_protobuf(stats.aggregateStats);
    (*proto.mutable_queue_stats()) = to_protobuf(stats.queueStats);

    for (const auto& kv : stats.objectStats) {
        protobuf::WorkerStats::ObjectRayStats* ray_stats =
            proto.add_object_stats();
        (*ray_stats->mutable_id()) = to_protobuf(kv.first);
        (*ray_stats->mutable_stats()) = to_protobuf(kv.second);
    }

    proto.set_bytes_sent(stats.bytesSent);
    proto.set_bytes_received(stats.bytesReceived);
    proto.set_interval_ms(
        duration_cast<milliseconds>(now() - stats.intervalStart).count());

    return proto;
}

template <class ValueType, class ProtoItem>
unique_ptr<ValueType[]> p2v(const ProtoItem& item) {
    auto values = make_unique<ValueType[]>(item.values_size());
    for (size_t i = 0; i < item.values_size(); i++) {
        values[i] = item.values(i);
    }
    return values;
}

template <class ValueType, class ProtoItem>
unique_ptr<ValueType[]> p2vo(const ProtoItem& item) {
    auto values = make_unique<ValueType[]>(item.values_size());
    for (size_t i = 0; i < item.values_size(); i++) {
        values[i] = move(from_protobuf(item.values(i)));
    }
    return values;
}

Point2i from_protobuf(const protobuf::Point2i& point) {
    return {point.x(), point.y()};
}

Point2f from_protobuf(const protobuf::Point2f& point) {
    return {point.x(), point.y()};
}

Point3f from_protobuf(const protobuf::Point3f& point) {
    return {point.x(), point.y(), point.z()};
}

Normal3f from_protobuf(const protobuf::Normal3f& normal) {
    return {normal.x(), normal.y(), normal.z()};
}

Vector2f from_protobuf(const protobuf::Vector2f& vector) {
    return {vector.x(), vector.y()};
}

Vector3f from_protobuf(const protobuf::Vector3f& vector) {
    return {vector.x(), vector.y(), vector.z()};
}

Bounds2i from_protobuf(const protobuf::Bounds2i& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Bounds2f from_protobuf(const protobuf::Bounds2f& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Bounds3f from_protobuf(const protobuf::Bounds3f& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Matrix4x4 from_protobuf(const protobuf::Matrix& proto_matrix) {
    Matrix4x4 matrix;
    for (size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4 and (4 * i + j < proto_matrix.m_size()); j++) {
            matrix.m[i][j] = proto_matrix.m(4 * i + j);
        }
    }
    return matrix;
}

RGBSpectrum from_protobuf(const protobuf::RGBSpectrum& proto_spectrum) {
    return RGBSpectrum::FromRGB(proto_spectrum.c().data());
}

RayDifferential from_protobuf(const protobuf::RayDifferential& proto_ray) {
    RayDifferential ray;

    ray.o = from_protobuf(proto_ray.o());
    ray.d = from_protobuf(proto_ray.d());
    ray.tMax = proto_ray.t_max();
    ray.time = proto_ray.time();
    ray.hasDifferentials = proto_ray.has_differentials();

    if (ray.hasDifferentials) {
        ray.rxOrigin = from_protobuf(proto_ray.rx_origin());
        ray.ryOrigin = from_protobuf(proto_ray.ry_origin());
        ray.rxDirection = from_protobuf(proto_ray.rx_direction());
        ray.ryDirection = from_protobuf(proto_ray.ry_direction());
    }

    return ray;
}

TriangleMesh from_protobuf(const protobuf::TriangleMesh& proto_tm) {
    Transform identity;
    vector<int> vertexIndices;
    vector<Point3f> p;
    vector<Vector3f> s;
    vector<Normal3f> n;
    vector<Point2f> uv;

    vertexIndices.reserve(proto_tm.n_triangles() * 3);
    p.reserve(proto_tm.n_vertices());

    for (size_t i = 0; i < proto_tm.vertex_indices_size(); i++) {
        vertexIndices.push_back(proto_tm.vertex_indices(i));
    }

    for (size_t i = 0; i < proto_tm.n_vertices(); i++) {
        p.push_back(from_protobuf(proto_tm.p(i)));
    }

    for (size_t i = 0; i < proto_tm.uv_size(); i++) {
        uv.push_back(from_protobuf(proto_tm.uv(i)));
    }

    for (size_t i = 0; i < proto_tm.s_size(); i++) {
        s.push_back(from_protobuf(proto_tm.s(i)));
    }

    for (size_t i = 0; i < proto_tm.n_size(); i++) {
        n.push_back(from_protobuf(proto_tm.n(i)));
    }

    return {identity,
            proto_tm.n_triangles(),
            vertexIndices.data(),
            proto_tm.n_vertices(),
            p.data(),
            s.data(),
            n.data(),
            uv.data(),
            nullptr,
            nullptr,
            nullptr};
}

RayState::TreeletNode from_protobuf(const protobuf::VisitNode& proto_node) {
    RayState::TreeletNode node;
    node.treelet = proto_node.treelet();
    node.node = proto_node.node();

    if (proto_node.has_transform()) {
        node.transform =
            make_shared<Transform>(from_protobuf(proto_node.transform()));
    }

    return node;
}

RayState from_protobuf(const protobuf::RayState& proto_state) {
    RayState state;

    state.sample.id = proto_state.sample_id();
    state.sample.num = proto_state.sample_num();
    state.sample.pixel = from_protobuf(proto_state.sample_pixel());
    state.sample.pFilm = from_protobuf(proto_state.sample_p_film());
    state.sample.weight = proto_state.sample_weight();
    state.ray = from_protobuf(proto_state.ray());

    for (size_t i = 0; i < proto_state.to_visit_size(); i++) {
        state.toVisit.push_back(from_protobuf(proto_state.to_visit(i)));
    }

    if (proto_state.has_hit()) {
        state.hit.reset(from_protobuf(proto_state.hit()));
    }

    state.beta = from_protobuf(proto_state.beta());
    state.Ld = from_protobuf(proto_state.ld());
    state.bounces = proto_state.bounces();
    state.remainingBounces = proto_state.remaining_bounces();
    state.isShadowRay = proto_state.is_shadow_ray();

    return state;
}

CloudIntegrator::SampleData from_protobuf(const protobuf::SampleData& proto_s) {
    CloudIntegrator::SampleData sample;
    sample.sample.pFilm = from_protobuf(proto_s.p_film());
    sample.weight = proto_s.weight();
    return sample;
}

ParamSet from_protobuf(const protobuf::ParamSet& pp) {
    ParamSet ps;

    for (const auto& item : pp.bools()) {
        ps.AddBool(item.name(), p2v<bool>(item), item.values_size());
    }

    for (const auto& item : pp.ints()) {
        ps.AddInt(item.name(), p2v<int>(item), item.values_size());
    }

    for (const auto& item : pp.floats()) {
        ps.AddFloat(item.name(), p2v<Float>(item), item.values_size());
    }

    for (const auto& item : pp.point2fs()) {
        ps.AddPoint2f(item.name(), p2vo<Point2f>(item), item.values_size());
    }

    for (const auto& item : pp.vector2fs()) {
        ps.AddVector2f(item.name(), p2vo<Vector2f>(item), item.values_size());
    }

    for (const auto& item : pp.point3fs()) {
        ps.AddPoint3f(item.name(), p2vo<Point3f>(item), item.values_size());
    }

    for (const auto& item : pp.vector3fs()) {
        ps.AddVector3f(item.name(), p2vo<Vector3f>(item), item.values_size());
    }

    for (const auto& item : pp.normals()) {
        ps.AddNormal3f(item.name(), p2vo<Normal3f>(item), item.values_size());
    }

    for (const auto& item : pp.spectra()) {
        ps.AddSpectrum(item.name(), p2vo<Spectrum>(item), item.values_size());
    }

    for (const auto& item : pp.strings()) {
        ps.AddString(item.name(), p2v<string>(item), item.values_size());
    }

    for (const auto& item : pp.textures()) {
        for (const auto& val : item.values()) {
            ps.AddTexture(item.name(), val);
            break; /* only one value for texture */
        }
    }

    return ps;
}

Scene from_protobuf(const protobuf::Scene& proto_scene) {
    /* we have to create a fake scene here */
    auto worldBound = from_protobuf(proto_scene.world_bound());

    /* create a shape */
    shared_ptr<Shape> fakeShape = make_shared<FakeShape>(worldBound);
    shared_ptr<Primitive> fakePrimitive = make_shared<GeometricPrimitive>(
        fakeShape, nullptr, nullptr, MediumInterface{});

    /* create the fake scene */
    return {fakePrimitive, {}};
}

TextureParams from_protobuf(
    const protobuf::TextureParams& texture_params, ParamSet& geom_params,
    ParamSet& material_params,
    std::map<std::string, std::shared_ptr<Texture<Float>>>& fTex,
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>>& sTex) {

    for (auto& kv : texture_params.float_textures()) {
        auto texture_reader = global::manager.GetReader(
            ObjectType::FloatTexture, kv.second);
        protobuf::FloatTexture texture;
        texture_reader->read(&texture);
        fTex[kv.first] = float_texture::from_protobuf(texture);
    }
    for (auto& kv : texture_params.spectrum_textures()) {
        // Load the texture
        auto texture_reader = global::manager.GetReader(
            ObjectType::SpectrumTexture, kv.second);
        protobuf::SpectrumTexture texture;
        texture_reader->read(&texture);
        sTex[kv.first] = spectrum_texture::from_protobuf(texture);
    }
    geom_params = from_protobuf(texture_params.geom_params());
    material_params = from_protobuf(texture_params.material_params());
    return TextureParams(geom_params, material_params, fTex, sTex);
}

SceneManager::ObjectKey from_protobuf(
    const protobuf::ObjectKey& ObjectKey) {
    return SceneManager::ObjectKey{
        static_cast<ObjectType>(ObjectKey.type()),
        ObjectKey.id()};
}

protobuf::Light light::to_protobuf(const string& name, const ParamSet& params,
                                   const Transform& light2world) {
    protobuf::Light proto_light;
    proto_light.set_name(name);
    *proto_light.mutable_paramset() = pbrt::to_protobuf(params);
    *proto_light.mutable_light_to_world() =
        pbrt::to_protobuf(light2world.GetMatrix());
    return proto_light;
}

shared_ptr<Light> light::from_protobuf(const protobuf::Light& proto_light) {
    shared_ptr<Light> light;

    const string& name = proto_light.name();
    const Transform light2world =
        pbrt::from_protobuf(proto_light.light_to_world());
    const ParamSet paramSet = pbrt::from_protobuf(proto_light.paramset());

    if (name == "point") {
        light = CreatePointLight(light2world, nullptr, paramSet);
    } else if (name == "spot") {
        light = CreateSpotLight(light2world, nullptr, paramSet);
    } else if (name == "goniometric") {
        light = CreateGoniometricLight(light2world, nullptr, paramSet);
    } else if (name == "projection") {
        light = CreateProjectionLight(light2world, nullptr, paramSet);
    } else if (name == "distant") {
        light = CreateDistantLight(light2world, paramSet);
    } else if (name == "infinite" || name == "exinfinite") {
        light = CreateInfiniteLight(light2world, paramSet);
    } else {
        throw runtime_error("unknown light name");
    }

    return light;
}

protobuf::Sampler sampler::to_protobuf(const string& name,
                                       const ParamSet& params,
                                       const Bounds2i& sampleBounds) {
    protobuf::Sampler proto_sampler;
    proto_sampler.set_name(name);
    *proto_sampler.mutable_paramset() = pbrt::to_protobuf(params);
    *proto_sampler.mutable_sample_bounds() = pbrt::to_protobuf(sampleBounds);
    return proto_sampler;
}

shared_ptr<Sampler> sampler::from_protobuf(const protobuf::Sampler& ps) {
    Sampler* sampler;

    const string& name = ps.name();
    const ParamSet paramSet = pbrt::from_protobuf(ps.paramset());
    const Bounds2i sampleBounds = pbrt::from_protobuf(ps.sample_bounds());

    if (name == "lowdiscrepancy" || name == "02sequence") {
        sampler = CreateZeroTwoSequenceSampler(paramSet);
    } else if (name == "maxmindist") {
        sampler = CreateMaxMinDistSampler(paramSet);
    } else if (name == "halton") {
        sampler = CreateHaltonSampler(paramSet, sampleBounds);
    } else if (name == "sobol") {
        sampler = CreateSobolSampler(paramSet, sampleBounds);
    } else if (name == "random") {
        sampler = CreateRandomSampler(paramSet);
    } else if (name == "stratified") {
        sampler = CreateStratifiedSampler(paramSet);
    } else {
        throw runtime_error("unknown sampler name");
    }

    return shared_ptr<Sampler>(sampler);
}

protobuf::Camera camera::to_protobuf(const string& name, const ParamSet& params,
                                     const AnimatedTransform& cam2world,
                                     const string& filmName,
                                     const ParamSet& filmParams,
                                     const string& filterName,
                                     const ParamSet& filterParams) {
    protobuf::Camera_Film_Filter proto_filter;
    protobuf::Camera_Film proto_film;
    protobuf::Camera proto_camera;

    /* (1) the filter */
    proto_filter.set_name(filterName);
    *proto_filter.mutable_paramset() = pbrt::to_protobuf(filterParams);

    /* (2) the film */
    proto_film.set_name(filmName);
    *proto_film.mutable_paramset() = pbrt::to_protobuf(filmParams);
    *proto_film.mutable_filter() = proto_filter;

    /* (3) the camera */
    proto_camera.set_name(name);
    *proto_camera.mutable_paramset() = pbrt::to_protobuf(params);
    *proto_camera.mutable_camera_to_world() = pbrt::to_protobuf(cam2world);
    *proto_camera.mutable_film() = proto_film;

    return proto_camera;
}

shared_ptr<Camera> camera::from_protobuf(
    const protobuf::Camera& proto_camera,
    vector<unique_ptr<Transform>>& transformCache) {
    /* (1) create the filter */
    Filter* filterPtr = nullptr;

    const auto& proto_filter = proto_camera.film().filter();
    const auto& filter_name = proto_filter.name();
    const auto filter_paramset = pbrt::from_protobuf(proto_filter.paramset());

    if (filter_name == "box") {
        filterPtr = CreateBoxFilter(filter_paramset);
    } else if (filter_name == "gaussian") {
        filterPtr = CreateGaussianFilter(filter_paramset);
    } else if (filter_name == "mitchell") {
        filterPtr = CreateMitchellFilter(filter_paramset);
    } else if (filter_name == "sinc") {
        filterPtr = CreateSincFilter(filter_paramset);
    } else if (filter_name == "triangle") {
        filterPtr = CreateTriangleFilter(filter_paramset);
    } else {
        throw runtime_error("unknown filter name: " + filter_name);
    }

    auto filter = unique_ptr<Filter>(filterPtr);

    /* (2) create the film */
    Film* film = nullptr;

    const auto& proto_film = proto_camera.film();
    const auto& film_name = proto_film.name();
    const auto film_paramset = pbrt::from_protobuf(proto_film.paramset());

    if (film_name == "image") {
        film = CreateFilm(film_paramset, move(filter));
    } else {
        throw runtime_error("unknown film name");
    }

    /* (3) create the camera */
    Camera* camera = nullptr;
    MediumInterface mi;
    Transform* cam2world[2];

    const auto& name = proto_camera.name();
    const auto paramset = pbrt::from_protobuf(proto_camera.paramset());
    const auto& transform = proto_camera.camera_to_world();

    transformCache.push_back(make_unique<Transform>(
        pbrt::from_protobuf(transform.start_transform())));
    cam2world[0] = transformCache.back().get();

    transformCache.push_back(
        make_unique<Transform>(pbrt::from_protobuf(transform.end_transform())));
    cam2world[1] = transformCache.back().get();

    AnimatedTransform ac2w(cam2world[0], transform.start_time(), cam2world[1],
                           transform.end_time());

    if (name == "perspective") {
        camera = CreatePerspectiveCamera(paramset, ac2w, film, mi.outside);
    } else if (name == "orthographic") {
        camera = CreateOrthographicCamera(paramset, ac2w, film, mi.outside);
    } else if (name == "realistic") {
        camera = CreateRealisticCamera(paramset, ac2w, film, mi.outside);
    } else if (name == "environment") {
        camera = CreateEnvironmentCamera(paramset, ac2w, film, mi.outside);
    } else {
        throw runtime_error("unknown camera name");
    }

    return shared_ptr<Camera>(camera);
}

std::shared_ptr<Material> material::from_protobuf(
    const protobuf::Material& material) {
    ParamSet geom_params;
    ParamSet material_params;
    std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams tp =
        pbrt::from_protobuf(material.texture_params(), geom_params,
                            material_params, fTex, sTex);
    return pbrt::MakeMaterial(material.name(), tp);
}

protobuf::Material material::to_protobuf(const std::string& name,
                                         const TextureParams& tp) {
  protobuf::Material material;
  material.set_name(name);
  material.mutable_texture_params()->CopyFrom(pbrt::to_protobuf(tp));
  return material;
}

std::shared_ptr<Texture<Float>> float_texture::from_protobuf(
    const protobuf::FloatTexture& texture) {
    ParamSet geom_params;
    ParamSet material_params;
    std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams tp =
        pbrt::from_protobuf(texture.texture_params(), geom_params,
                            material_params, fTex, sTex);

    return MakeFloatTexture(texture.name(),
                            Transform(pbrt::from_protobuf(texture.tex2world())),
                            tp);
}

protobuf::FloatTexture float_texture::to_protobuf(const std::string& name,
                                                  const Transform& tex2world,
                                                  const TextureParams& tp) {
    protobuf::FloatTexture texture;
    texture.set_name(name);
    texture.mutable_tex2world()->CopyFrom(pbrt::to_protobuf(tex2world.GetMatrix()));
    texture.mutable_texture_params()->CopyFrom(pbrt::to_protobuf(tp));
    return texture;
}

std::shared_ptr<Texture<Spectrum>> spectrum_texture::from_protobuf(
    const protobuf::SpectrumTexture& texture) {
    ParamSet geom_params;
    ParamSet material_params;
    std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams tp =
        pbrt::from_protobuf(texture.texture_params(), geom_params,
                            material_params, fTex, sTex);

    return MakeSpectrumTexture(
        texture.name(), Transform(pbrt::from_protobuf(texture.tex2world())),
        tp);
}

protobuf::SpectrumTexture spectrum_texture::to_protobuf(
    const std::string& name, const Transform& tex2world,
    const TextureParams& tp) {
    protobuf::SpectrumTexture texture;
    texture.set_name(name);
    texture.mutable_tex2world()->CopyFrom(pbrt::to_protobuf(tex2world.GetMatrix()));
    texture.mutable_texture_params()->CopyFrom(pbrt::to_protobuf(tp));
    return texture;
}

RayStats from_protobuf(const protobuf::RayStats& proto) {
    RayStats stats;
    stats.sentRays = proto.sent_rays();
    stats.receivedRays = proto.received_rays();
    stats.waitingRays = proto.waiting_rays();
    stats.processedRays = proto.processed_rays();
    stats.demandedRays = proto.demanded_rays();

    for (int i = 0; i < NUM_PERCENTILES; ++i) {
      double d = proto.trace_duration_percentiles(i);
      stats.traceDurationPercentiles[i] = d;
    }
    stats.rayDurations.reserve(proto.ray_durations_size());
    for (double d : proto.ray_durations()) {
      stats.rayDurations.push_back(d);
    }
    return stats;
}

QueueStats from_protobuf(const protobuf::QueueStats& proto) {
    QueueStats stats;
    stats.ray = proto.ray();
    stats.finished = proto.finished();
    stats.pending = proto.pending();
    stats.out = proto.out();
    stats.connecting = proto.connecting();
    stats.connected = proto.connected();
    return stats;
}

WorkerStats from_protobuf(const protobuf::WorkerStats& proto) {
    WorkerStats stats;
    stats._finishedPaths = proto.finished_paths();
    stats.aggregateStats = from_protobuf(proto.aggregate_stats());
    stats.queueStats = from_protobuf(proto.queue_stats());

    for (const protobuf::WorkerStats::ObjectRayStats& object_stats :
         proto.object_stats()) {
        auto id = from_protobuf(object_stats.id());
        stats.objectStats[id] = from_protobuf(object_stats.stats());
    }

    for (const auto& kv : proto.time_per_action()) {
        stats.timePerAction[kv.first] = kv.second;
    }

    stats.bytesSent = proto.bytes_sent();
    stats.bytesReceived = proto.bytes_received();
    stats.interval = milliseconds{proto.interval_ms()};

    return stats;
}

}  // namespace pbrt
