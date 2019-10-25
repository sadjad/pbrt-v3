#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cloud/manager.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA OUTPUT OUTPUT-SAMPLES" << endl;
}

shared_ptr<Camera> loadCamera(const string &scenePath,
                              vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

shared_ptr<GlobalSampler> loadSampler(const string &scenePath) {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const string scenePath{argv[1]};
        const string outputPath{argv[2]};
        const string samplesPath{argv[3]};

        global::manager.init(scenePath);

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<GlobalSampler> sampler = loadSampler(scenePath);
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);

        const Bounds2i sampleBounds = camera->film->GetSampleBounds();
        const Vector2i sampleExtent = sampleBounds.Diagonal();
        const auto samplesPerPixel = sampler->samplesPerPixel;
        const uint8_t maxDepth = 5;
        const float rayScale = 1 / sqrt((Float)sampler->samplesPerPixel);

        protobuf::RecordWriter rayWriter{outputPath};
        protobuf::RecordWriter sampleWriter{samplesPath};

        /* Generate all the samples */
        size_t i = 0;

        for (size_t sample = 0; sample < sampler->samplesPerPixel; sample++) {
            for (Point2i pixel : sampleBounds) {
                sampler->StartPixel(pixel);
                if (!InsideExclusive(pixel, sampleBounds)) continue;
                sampler->SetSampleNumber(sample);

                CloudIntegrator::SampleData sampleData;
                sampleData.sample = sampler->GetCameraSample(pixel);

                RayStatePtr statePtr = make_unique<RayState>();
                RayState &state = *statePtr;

                state.sample.id =
                    (pixel.x + pixel.y * sampleExtent.x) * samplesPerPixel +
                    sample;
                state.sample.dim = sampler->GetCurrentDimension();
                state.remainingBounces = maxDepth;
                sampleData.weight = camera->GenerateRayDifferential(
                    sampleData.sample, &state.ray);
                state.ray.ScaleDifferentials(rayScale);
                state.StartTrace();

                const auto len = statePtr->Serialize();
                rayWriter.write(statePtr->serialized + 4, len - 4);
                sampleWriter.write(to_protobuf(sampleData));
            }
        }

        cerr << i << " sample(s) were generated." << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
