#include "accelerators/cloud.h"
#include "cloud/manager.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/sampler.h"
#include "core/stats.h"
#include "integrators/cloud.h"
#include "messages/utils.h"
#include "pbrt/main.h"
#include "pbrt/raystate.h"

using namespace std;

namespace pbrt {

STAT_COUNTER("Integrator/Camera rays generated", nCameraRays);
STAT_COUNTER("Integrator/Total rays traced", totalRays);
STAT_COUNTER("Intersections/Regular ray intersection tests",
             nIntersectionTests);

void AccumulatedStats::Merge(const AccumulatedStats &other) {
    for (const auto &item : other.counters) {
        counters[item.first] += item.second;
    }

    for (const auto &item : other.memoryCounters) {
        memoryCounters[item.first] += item.second;
    }

    for (const auto &item : other.intDistributionCounts) {
        intDistributionCounts[item.first] += item.second;
        intDistributionSums[item.first] +=
            other.intDistributionSums.at(item.first);

        if (intDistributionMins.count(item.first) == 0) {
            intDistributionMins[item.first] =
                other.intDistributionMins.at(item.first);
        } else {
            intDistributionMins[item.first] =
                min(intDistributionMins[item.first],
                    other.intDistributionMins.at(item.first));
        }

        if (intDistributionMaxs.count(item.first) == 0) {
            intDistributionMaxs[item.first] =
                other.intDistributionMaxs.at(item.first);
        } else {
            intDistributionMaxs[item.first] =
                max(intDistributionMaxs[item.first],
                    other.intDistributionMaxs.at(item.first));
        }
    }

    for (const auto &item : other.floatDistributionCounts) {
        floatDistributionCounts[item.first] += item.second;
        floatDistributionSums[item.first] +=
            other.floatDistributionSums.at(item.first);

        if (!floatDistributionMins.count(item.first)) {
            floatDistributionMins[item.first] =
                other.floatDistributionMins.at(item.first);
        } else {
            floatDistributionMins[item.first] =
                min(floatDistributionMins.at(item.first),
                    other.floatDistributionMins.at(item.first));
        }

        if (!floatDistributionMaxs.count(item.first)) {
            floatDistributionMaxs[item.first] =
                other.floatDistributionMaxs.at(item.first);
        } else {
            floatDistributionMaxs[item.first] =
                max(floatDistributionMaxs[item.first],
                    other.floatDistributionMaxs.at(item.first));
        }
    }

    for (const auto &item : other.percentages) {
        percentages[item.first].first += item.second.first;
        percentages[item.first].second += item.second.second;
    }

    for (const auto &item : other.ratios) {
        ratios[item.first].first += item.second.first;
        ratios[item.first].second += item.second.second;
    }
}

namespace scene {

string GetObjectName(const ObjectType type, const uint32_t id) {
    return SceneManager::getFileName(type, id);
}

Base::Base() {}

Base::~Base() {}

Base::Base(Base &&) = default;
Base &Base::operator=(Base &&) = default;

Base::Base(const std::string &path, const int samplesPerPixel) {
    using namespace pbrt::global;

    PbrtOptions.nThreads = 1;

    manager.init(path);

    auto reader = manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);

    reader = manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler, samplesPerPixel);

    reader = manager.GetReader(ObjectType::Lights);
    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }

    reader = manager.GetReader(ObjectType::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    fakeScene = make_unique<Scene>(from_protobuf(proto_scene));

    for (auto &light : lights) {
        light->Preprocess(*fakeScene);
    }

    const auto treeletCount = manager.treeletCount();
    treeletDependencies.resize(treeletCount);

    for (TreeletId i = 0; i < treeletCount; i++) {
        treeletDependencies[i] = manager.getTreeletDependencies(i);
    }

    this->samplesPerPixel = sampler->samplesPerPixel;
    sampleBounds = camera->film->GetSampleBounds();
    sampleExtent = sampleBounds.Diagonal();
    totalPaths = sampleBounds.Area() * sampler->samplesPerPixel;
}

Base LoadBase(const std::string &path, const int samplesPerPixel) {
    return {path, samplesPerPixel};
}

shared_ptr<CloudBVH> LoadTreelet(const string &path, const TreeletId treeletId,
                                 istream *stream) {
    using namespace pbrt::global;
    manager.init(path);
    shared_ptr<CloudBVH> treelet = make_shared<CloudBVH>(treeletId);
    treelet->LoadTreelet(treeletId, stream);
    return treelet;
}

}  // namespace scene

namespace graphics {

RayStatePtr TraceRay(RayStatePtr &&rayState, const CloudBVH &treelet) {
    return CloudIntegrator::Trace(move(rayState), treelet);
}

pair<RayStatePtr, RayStatePtr> ShadeRay(RayStatePtr &&rayState,
                                        const CloudBVH &treelet,
                                        const vector<shared_ptr<Light>> &lights,
                                        const Vector2i &sampleExtent,
                                        shared_ptr<GlobalSampler> &sampler,
                                        int maxPathDepth, MemoryArena &arena) {
    return CloudIntegrator::Shade(move(rayState), treelet, lights, sampleExtent,
                                  sampler, maxPathDepth, arena);
}

RayStatePtr GenerateCameraRay(const shared_ptr<Camera> &camera,
                              const Point2i &pixel, const uint32_t sample,
                              const uint8_t maxDepth,
                              const Vector2i &sampleExtent,
                              shared_ptr<GlobalSampler> &sampler) {
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    sampler->StartPixel(pixel);
    sampler->SetSampleNumber(sample);

    CameraSample cameraSample = sampler->GetCameraSample(pixel);

    RayStatePtr statePtr = RayState::Create();
    RayState &state = *statePtr;

    state.sample.id =
        (pixel.x + pixel.y * sampleExtent.x) * samplesPerPixel + sample;
    state.sample.dim = sampler->GetCurrentDimension();
    state.sample.pFilm = cameraSample.pFilm;
    state.sample.weight =
        camera->GenerateRayDifferential(cameraSample, &state.ray);
    state.ray.ScaleDifferentials(rayScale);
    state.remainingBounces = maxDepth - 1;
    state.StartTrace();

    ++nCameraRays;
    ++nIntersectionTests;
    ++totalRays;

    return statePtr;
}

void AccumulateImage(const shared_ptr<Camera> &camera,
                     const vector<Sample> &rays) {
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

    for (const auto &ray : rays) {
        filmTile->AddSample(ray.pFilm, ray.L, ray.weight, true);
    }

    camera->film->MergeFilmTile(move(filmTile));
}

void WriteImage(const shared_ptr<Camera> &camera, const string &filename) {
    if (not filename.empty()) {
        camera->film->setFilename(filename);
    }

    camera->film->WriteImage();
}

}  // namespace graphics

}  // namespace pbrt
