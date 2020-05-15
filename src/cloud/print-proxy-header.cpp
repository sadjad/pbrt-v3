#include "core/geometry.h"
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstdlib>

using namespace pbrt;
using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << argv[0] << " PROXYDIR" << endl;
        exit(1);
    }

    ifstream proxyHdr(string(argv[1]) + "/HEADER");

    Bounds3f root;
    proxyHdr.read(reinterpret_cast<char *>(&root), sizeof(Bounds3f));
    uint64_t treeletSize;
    proxyHdr.read(reinterpret_cast<char *>(&treeletSize), sizeof(uint64_t));

    uint64_t nodeCount;
    proxyHdr.read(reinterpret_cast<char *>(&nodeCount), sizeof(uint64_t));

    uint64_t numDependencies;
    proxyHdr.read(reinterpret_cast<char *>(&numDependencies), sizeof(uint64_t));
    vector<string> deps;
    deps.reserve(numDependencies);
    for (int i = 0; i < numDependencies; i++) {
        uint64_t numChars;
        proxyHdr.read(reinterpret_cast<char *>(&numChars), sizeof(uint64_t));
        vector<char> depBuf(numChars + 1, '\0');
        proxyHdr.read(depBuf.data(), numChars);

        deps.emplace_back(depBuf.data());
    }

    cout << root << endl << "Treelet bytes: " << treeletSize << endl << "Node count: " << nodeCount << endl;
    for (const auto &dep_name : deps) {
        cout << dep_name << endl;
    }

    return 0;
}
