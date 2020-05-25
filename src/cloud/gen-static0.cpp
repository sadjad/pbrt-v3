#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <set>
#include <vector>
#include <unordered_map>

#include "geometry.h"
#include "accelerators/cloud.h"
#include <pbrt/main.h>
#include <dirent.h>
#include "messages/serialization.h"
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "rng.h"

using namespace std;
using namespace pbrt;

void surfaceArea(const string &path, ofstream &static0) {
  pbrt::PbrtOptions.nThreads = 1;
  pbrt::scene::Base base(path, 1);

  size_t numTreelets = base.GetTreeletCount();
  static0 << numTreelets << endl;

  vector<uint64_t> treeletAreas;
  vector<uint64_t> unionAreas;
  vector<set<uint32_t>> treeletChildren;
  vector<uint64_t> instanceAreas(numTreelets);
  vector<uint64_t> finalAreas(numTreelets);

  for (uint32_t treeletIdx = 0; treeletIdx < numTreelets; treeletIdx++) {
      auto treelet = pbrt::scene::LoadTreelet(path, treeletIdx);
      uint64_t area = (uint64_t)treelet->RootSurfaceAreas();
      uint64_t unionArea = (uint64_t)treelet->SurfaceAreaUnion();
      treeletAreas.push_back(area);
      unionAreas.push_back(unionArea);

      auto info = treelet->GetInfo(treeletIdx);

      for (auto &kv : info.instances) {
          instanceAreas[kv.first] += kv.second;
      }

      treeletChildren.push_back(info.children);
  }

  for (uint32_t treeletIdx = 0; treeletIdx < numTreelets; treeletIdx++) {
      uint64_t instanceArea = instanceAreas[treeletIdx];
      uint64_t treeletArea = treeletAreas[treeletIdx];
      if (instanceArea == 0) {
          finalAreas[treeletIdx] = treeletArea;
          continue;
      }

      vector<uint32_t> areaStack {treeletIdx};
      while (!areaStack.empty()) {
          uint32_t subInst = areaStack.back();
          areaStack.pop_back();

          uint32_t subInstArea = treeletAreas[subInst];
          double areaRatio = subInstArea / treeletArea;

          finalAreas[subInst] = areaRatio * instanceArea;

          for (uint32_t child : treeletChildren[subInst]) {
              areaStack.push_back(child);
          }
      }
  }

  for (uint32_t treeletIdx = 0; treeletIdx < numTreelets; treeletIdx++) {
      uint64_t areaHeuristic = (uint64_t)ceil(finalAreas[treeletIdx]);

      cout << treeletIdx << " " << treeletAreas[treeletIdx] << " " <<
              unionAreas[treeletIdx] << " " <<
              areaHeuristic << endl;

      static0 << areaHeuristic << " " << 1 << " " << treeletIdx << endl;
  }
}

Vector3f computeRayDir(unsigned idx) {
    unsigned x = idx & (1 << 0);
    unsigned y = idx & (1 << 1);
    unsigned z = idx & (1 << 2);

    return Vector3f(x ? 1 : -1, y ? 1 : -1, z ? 1 : -1);
}

void randomRays(const string &path, ofstream &static0, const int spp) {
    DIR *dir = opendir(path.c_str());
    if (!dir) {
        cerr << "Can't open " << path << endl;
        exit(EXIT_FAILURE);
    }

    dirent *entry = nullptr;
    unordered_map<uint32_t, Bounds3f> bBoxesMap;
    while ((entry = readdir(dir))) {
        if (entry->d_name[0] != 'B') continue;
        uint32_t treeletId = stoul(entry->d_name + 1);

        protobuf::RecordReader boundsReader(path + "/" + entry->d_name);
        protobuf::Bounds3f protoBounds;
        bool success = boundsReader.read(&protoBounds);
        CHECK_EQ(success, true);
        Bounds3f bounds = from_protobuf(protoBounds);
        cout << bounds << endl;
        bBoxesMap.emplace(treeletId, bounds);
    }
    closedir(dir);

    const uint32_t numTreelets = bBoxesMap.size();

    vector<Bounds3f> bBoxes(numTreelets);
    for (const auto &kv : bBoxesMap) {
        bBoxes[kv.first] = kv.second;
    }

    RNG rng;
    Bounds3f root = bBoxes[0];
    Float rootArea = root.SurfaceArea();

    vector<uint64_t> rayCounts(numTreelets);
    for (uint32_t treeletId = 0; treeletId < numTreelets; treeletId++) {
        Bounds3f bbox = bBoxes[treeletId];

        int reducedSpp = spp * (bbox.SurfaceArea() / rootArea);
        reducedSpp = max(reducedSpp, 10);

        for (int i = 0; i < reducedSpp; i++) {
            Point3f u(rng.UniformFloat(), rng.UniformFloat(), rng.UniformFloat());
            Point3f origin = bbox.Lerp(u);

            for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
                Vector3f dir = computeRayDir(dirIdx);
                Ray r(origin, dir);

                for (uint32_t oTreeletId = 0; oTreeletId < numTreelets; oTreeletId++) {
                    if (oTreeletId == treeletId) continue;
                    Bounds3f oBbox = bBoxes[oTreeletId];
                    if (oBbox.IntersectP(r)) {
                        rayCounts[oTreeletId]++;
                    }
                }
            }
        }
    }

    static0 << numTreelets << endl;

    for (uint32_t treeletId = 0; treeletId < numTreelets; treeletId++) {
        uint64_t count = max(rayCounts[treeletId], 1UL);
        static0 << count << " " << 1 << " " << treeletId << endl;
    }
}

int main(int argc, char* argv[])
{
  if (argc < 4) {
      cerr << "Usage: gen-static0 CMD SCENE STATIC0" << endl;
      return EXIT_FAILURE;
  }

  const string cmd { argv[1] };
  const string path { argv[2] };
  ofstream static0(argv[3]);

  if (cmd == "SAH") {
      surfaceArea(path, static0);
  } else {
      if (argc < 5) {
        cerr << "Usage: gen-static0 RAYS SCENE STATIC0 SPP" << endl;
        return EXIT_FAILURE;
      }
      int spp = stoi(argv[4]);
      randomRays(path, static0, spp);
  }

  return EXIT_SUCCESS;
}
