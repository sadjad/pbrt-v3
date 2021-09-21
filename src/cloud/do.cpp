#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "accelerators/cloud.h"
#include "cloud/manager.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "pbrt/main.h"
#include "pbrt/raystate.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA CAMERA-RAYS" << endl;
}

enum class Operation { Trace, Shade };

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        /* CloudBVH requires this */
        PbrtOptions.nThreads = 1;

        const string scenePath{argv[1]};
        const string raysPath{argv[2]};

        pbrt::scene::Base sceneBase = pbrt::scene::LoadBase(scenePath, 0);

        queue<RayStatePtr> rayList;
        vector<Sample> samples;

        /* loading all the rays */
        {
            protobuf::RecordReader reader{raysPath};
            while (!reader.eof()) {
                string rayStr;

                if (reader.read(&rayStr)) {
                    auto rayStatePtr = RayState::Create();
                    rayStatePtr->Deserialize(rayStr.data(), rayStr.length());
                    rayList.push(move(rayStatePtr));
                }
            }
        }

        cerr << rayList.size() << " RayState(s) loaded." << endl;

        if (!rayList.size()) {
            return EXIT_SUCCESS;
        }

        /* prepare the scene */
        MemoryArena arena;
        auto &camera = sceneBase.camera;
        auto &sampler = sceneBase.sampler;
        auto &lights = sceneBase.lights;
        auto &fakeScene = sceneBase.fakeScene;

        vector<unique_ptr<CloudBVH>> treelets;
        treelets.resize(sceneBase.GetTreeletCount());

        /* let's load all the treelets */
        for (size_t i = 0; i < treelets.size(); i++) {
            treelets[i] = make_unique<CloudBVH>(i, false, false);
        }

        for (auto &light : lights) {
            light->Preprocess(*fakeScene);
        }

        const auto sampleExtent = camera->film->GetSampleBounds().Diagonal();
        const int maxDepth = 5;

        while (!rayList.empty()) {
            RayStatePtr theRayPtr = move(rayList.front());
            RayState &theRay = *theRayPtr;
            rayList.pop();

            const TreeletId rayTreeletId = theRay.CurrentTreelet();

            if (!theRay.toVisitEmpty()) {
                auto newRayPtr = graphics::TraceRay(move(theRayPtr),
                                                    *treelets[rayTreeletId]);
                auto &newRay = *newRayPtr;

                const bool hit = newRay.HasHit();
                const bool emptyVisit = newRay.toVisitEmpty();

                if (newRay.IsShadowRay()) {
                    if (hit || emptyVisit) {
                        newRay.Ld = hit ? 0.f : newRay.Ld;
                        samples.emplace_back(*newRayPtr);
                    } else {
                        rayList.push(move(newRayPtr));
                    }
                } else if (!emptyVisit || hit) {
                    rayList.push(move(newRayPtr));
                } else if (emptyVisit) {
                    newRay.Ld = 0.f;
                    if (newRay.remainingBounces == maxDepth - 1) {
                        for (const auto &light : sceneBase.infiniteLights) {
                            newRay.Ld += light->Le(newRay.ray);
                        }
                    }
                    samples.emplace_back(*newRayPtr);
                }
            } else if (theRay.HasHit()) {
                RayStatePtr bounceRay, shadowRay;
                tie(bounceRay, shadowRay) = graphics::ShadeRay(
                    move(theRayPtr), *treelets[rayTreeletId], lights,
                    sampleExtent, sampler, maxDepth, arena);

                if (bounceRay != nullptr) {
                    rayList.push(move(bounceRay));
                }

                if (shadowRay != nullptr) {
                    rayList.push(move(shadowRay));
                }
            }
        }

        graphics::AccumulateImage(camera, samples);
        camera->film->WriteImage();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
