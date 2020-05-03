#include <cstdlib>
#include <memory>
#include <iostream>
#include <fstream>

#include "pbrt.h"
#include "memory.h"
#include "fileutil.h"
#include "parser.h"
#include "api.h"
#include "spectrum.h"
#include <sys/stat.h>

using namespace pbrt;
using namespace std;

PBRT_CONSTEXPR int TokenOptional = 0;
PBRT_CONSTEXPR int TokenRequired = 1;

static void customParse(unique_ptr<Tokenizer> t,
                        ofstream &outFile,
                        const string &dirName) {
    vector<unique_ptr<Tokenizer>> fileStack;
    fileStack.push_back(move(t));
    parserLoc = &fileStack.back()->loc;

    bool ungetTokenSet = false;
    string ungetTokenValue;

    ofstream instanceFile;
    ofstream *curFile = &outFile;

    auto writeToken = [&curFile](const string_view &text) {
        curFile->write(text.data(), text.size());
        (*curFile) << " ";
    };

    auto writeLine = [&curFile]() {
        (*curFile) << "\n";
    };

    auto writeWorldBegin = [&curFile]() {
        (*curFile) << "WorldBegin\n";
    };

    auto writeWorldEnd = [&curFile]() {
        (*curFile) << "WorldEnd\n";
    };

    auto writeProxy = [&curFile](string_view &name) {
        (*curFile) << "ProxyBVH ";
        curFile->write(name.data(), name.size());
    };

    // nextToken is a little helper function that handles the file stack,
    // returning the next token from the current file until reaching EOF,
    // at which point it switches to the next file (if any).
    function<string_view(int)> nextToken = [&](int flags) -> string_view {
        if (ungetTokenSet) {
            ungetTokenSet = false;
            return string_view(ungetTokenValue.data(), ungetTokenValue.size());
        }

        if (fileStack.empty()) {
            if (flags & TokenRequired) {
                Error("premature EOF");
                exit(1);
            }
            parserLoc = nullptr;
            return {};
        }

        string_view tok = fileStack.back()->Next();

        if (tok.empty()) {
            // We've reached EOF in the current file. Anything more to parse?
            fileStack.pop_back();
            if (!fileStack.empty()) parserLoc = &fileStack.back()->loc;
            return nextToken(flags);
        } else if (tok[0] == '#') {
            // Swallow comments
            return nextToken(flags);
        } else
            // Regular token; success.
            return tok;
    };

    auto ungetToken = [&](string_view s) {
        CHECK(!ungetTokenSet);
        ungetTokenValue = string(s.data(), s.size());
        ungetTokenSet = true;
    };

    auto customParseParams = [&] (bool writeOut=true) {
        while (true) {
            string_view decl = nextToken(TokenOptional);
            if (decl.empty()) return;

            if (!isQuotedString(decl)) {
                ungetToken(decl);
                return;
            }

            if (writeOut) writeToken(decl);

            string_view val = nextToken(TokenRequired);
            if (writeOut) writeToken(val);

            if (val == "[") {
                while (true) {
                    val = nextToken(TokenRequired);
                    if (writeOut) writeToken(val);
                    if (val == "]") break;
                }
            }
        }
    };

    // Helper function for pbrt API entrypoints that take a single string
    // parameter and a ParamSet (e.g. pbrtShape()).
    auto basicParamListEntrypoint = [&](bool writeOut=true) {
        string_view name = nextToken(TokenRequired);
        if (writeOut) writeToken(name);
        customParseParams(writeOut);
        if (writeOut) writeLine();
    };

    auto syntaxError = [&](string_view tok) {
        cerr << "Unexpected token: ";
        cerr.write(tok.data(), tok.size());
        cerr << endl;
        exit(1);
    };

    while (true) {
        string_view tok = nextToken(TokenOptional);
        if (tok.empty()) break;

        switch (tok[0]) {
        case 'A':
            if (tok == "AttributeBegin") {
                pbrtAttributeBegin();

                writeToken(tok);
                writeLine();
            } else if (tok == "AttributeEnd") {
                pbrtAttributeEnd();

                writeToken(tok);
                writeLine();
            } else if (tok == "ActiveTransform") {
                writeToken(tok);
                string_view a = nextToken(TokenRequired);
                writeToken(a);

                if (a == "All")
                    pbrtActiveTransformAll();
                else if (a == "EndTime")
                    pbrtActiveTransformEndTime();
                else if (a == "StartTime")
                    pbrtActiveTransformStartTime();
                else
                    syntaxError(tok);

                writeLine();
            } else if (tok == "AreaLightSource") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else if (tok == "Accelerator") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else {
                syntaxError(tok);
            }
            break;

        case 'C':
            if (tok == "ConcatTransform") {
                writeToken(tok);

                auto braceToken = nextToken(TokenRequired);
                if (braceToken != "[") syntaxError(tok);
                writeToken(braceToken);

                Float m[16];
                for (int i = 0; i < 16; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    m[i] = parseNumber(numTok);
                }

                braceToken = nextToken(TokenRequired);
                if (braceToken != "]") syntaxError(tok);
                writeToken(braceToken);
                writeLine();

                pbrtConcatTransform(m);
            } else if (tok == "CoordinateSystem") {
                writeToken(tok);
                auto quoteName = nextToken(TokenRequired);
                writeToken(quoteName);
                writeLine();

                string_view n = dequoteString(quoteName);
                pbrtCoordinateSystem(toString(n));
            } else if (tok == "CoordSysTransform") {
                writeToken(tok);
                auto quoteName = nextToken(TokenRequired);
                writeToken(quoteName);
                writeLine();

                string_view n = dequoteString(quoteName);
                pbrtCoordSysTransform(toString(n));
            } else if (tok == "Camera") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else {
                syntaxError(tok);
            }
            break;

        case 'F':
            if (tok == "Film") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else {
                syntaxError(tok);
            }
            break;

        case 'I':
            if (tok == "Integrator") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else if (tok == "Include") {
                // Switch to the given file.
                string filename =
                    toString(dequoteString(nextToken(TokenRequired)));

                filename = AbsolutePath(ResolveFilename(filename));
                auto tokError = [](const char *msg) { Error("%s", msg); };
                unique_ptr<Tokenizer> tinc =
                    Tokenizer::CreateFromFile(filename, tokError);
                if (tinc) {
                    fileStack.push_back(move(tinc));
                    parserLoc = &fileStack.back()->loc;
                }
            } else if (tok == "Identity") {
                writeToken(tok);
                writeLine();

                pbrtIdentity();
            } else {
                syntaxError(tok);
            }
            break;

        case 'L':
            if (tok == "LightSource") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else if (tok == "LookAt") {
                writeToken(tok);

                Float v[9];
                for (int i = 0; i < 9; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    v[i] = parseNumber(numTok);
                }
                writeLine();

                pbrtLookAt(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                           v[8]);
            } else {
                syntaxError(tok);
            }
            break;

        case 'M':
            if (tok == "MakeNamedMaterial") {
                basicParamListEntrypoint(false);
            } else if (tok == "MakeNamedMedium") {
                basicParamListEntrypoint(false);
            } else if (tok == "Material") {
                basicParamListEntrypoint(false);
            } else if (tok == "MediumInterface") {
                nextToken(TokenRequired);
                string_view second = nextToken(TokenOptional);
                if (!second.empty()) {
                    if (!isQuotedString(second)) {
                        ungetToken(second);
                    }
                }
            } else {
                syntaxError(tok);
            }
            break;

        case 'N':
            if (tok == "NamedMaterial") {
                nextToken(TokenRequired);
            } else {
                syntaxError(tok);
            }
            break;

        case 'O':
            if (tok == "ObjectBegin") {
                auto quoteName = nextToken(TokenRequired);
                string_view n = dequoteString(quoteName);
                instanceFile.open(dirName + "/" + toString(n) + ".pbrt");
                curFile = &instanceFile;

                // FIXME write out current transforms and graphics state
                writeWorldBegin();
                writeToken(tok);
                writeToken(quoteName);
                writeLine();

                pbrtAttributeBegin();
            } else if (tok == "ObjectEnd") {
                writeToken(tok);
                writeLine();
                writeWorldEnd();
                curFile->close();
                curFile = &outFile;

                pbrtAttributeEnd();
            } else if (tok == "ObjectInstance") {
                string_view n = nextToken(TokenRequired);
                writeProxy(n);
                writeLine();
            } else {
                syntaxError(tok);
            }
            break;

        case 'P':
            if (tok == "PixelFilter") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else {
                syntaxError(tok);
            } break;

        case 'R':
            if (tok == "ReverseOrientation") {
                writeToken(tok);
                writeLine();
                pbrtReverseOrientation();
            } else if (tok == "Rotate") {
                writeToken(tok);

                Float v[4];
                for (int i = 0; i < 4; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    v[i] = parseNumber(numTok);
                }

                writeLine();

                pbrtRotate(v[0], v[1], v[2], v[3]);
            } else {
                syntaxError(tok);
            }
            break;

        case 'S':
            if (tok == "Shape") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else if (tok == "Sampler") {
                writeToken(tok);
                basicParamListEntrypoint();
            } else if (tok == "Scale") {
                writeToken(tok);

                Float v[3];
                for (int i = 0; i < 3; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    v[i] = parseNumber(numTok);
                }
                writeLine();

                pbrtScale(v[0], v[1], v[2]);
            } else {
                syntaxError(tok);
            }
            break;

        case 'T':
            if (tok == "TransformBegin") {
                writeToken(tok);
                writeLine();

                pbrtTransformBegin();
            } else if (tok == "TransformEnd") {
                writeToken(tok);
                writeLine();

                pbrtTransformEnd();
            } else if (tok == "Transform") {
                writeToken(tok);

                auto braceTok = nextToken(TokenRequired);
                if (braceTok != "[") syntaxError(tok);
                writeToken(braceTok);

                Float m[16];
                for (int i = 0; i < 16; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    m[i] = parseNumber(numTok);
                }

                braceTok = nextToken(TokenRequired);
                if (braceTok != "]") syntaxError(tok);
                writeToken(braceTok);
                writeLine();

                pbrtTransform(m);
            } else if (tok == "Translate") {
                writeToken(tok);

                Float v[3];
                for (int i = 0; i < 3; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);
                    v[i] = parseNumber(numTok);
                }
                writeLine();

                pbrtTranslate(v[0], v[1], v[2]);
            } else if (tok == "TransformTimes") {
                writeToken(tok);

                Float v[2];
                for (int i = 0; i < 2; ++i) {
                    auto numTok = nextToken(TokenRequired);
                    writeToken(numTok);

                    v[i] = parseNumber(numTok);
                }
                writeLine();

                pbrtTransformTimes(v[0], v[1]);
            } else if (tok == "Texture") {
                nextToken(TokenRequired);
                nextToken(TokenRequired);

                basicParamListEntrypoint(false);
            } else {
                syntaxError(tok);
            }
            break;

        case 'W':
            if (tok == "WorldBegin") {
                writeToken(tok);
                writeLine();

                pbrtWorldBegin();
            } else if (tok == "WorldEnd") {
                writeToken(tok);
                writeLine();
            } else {
                syntaxError(tok);
            }
            break;

        default:
            syntaxError(tok);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        cerr << argv[0] << " IN_SCENE OUT_SCENE INSTANCES_DIR" << endl;
        exit(EXIT_FAILURE);
    }
    const string inName(argv[1]);
    const string outName(argv[2]);
    const string instancesDir(argv[3]);

    mkdir(instancesDir.c_str(), 0700);

    ofstream outFile(outName);

    Options options;
    options.nThreads = 1;
    pbrtInit(options);

    SetSearchDirectory(DirectoryContaining(inName));
    auto tokError = [](const char *msg) { Error("%s", msg); exit(EXIT_FAILURE); };
    auto t = Tokenizer::CreateFromFile(inName, tokError);
    if (!t) {
        cerr << "Tokenizer failed" << endl;
    }

    customParse(move(t), outFile, instancesDir);
}
