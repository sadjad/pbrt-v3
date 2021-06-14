#ifndef PBRT_MESSAGES_LITE_H
#define PBRT_MESSAGES_LITE_H

#include <fstream>
#include <stdexcept>
#include <string>

class LiteRecordReader {
  public:
    LiteRecordReader() = default;
    LiteRecordReader(const char* buffer, const size_t len);

    bool read(const char** buf, size_t* len);

    template <class T>
    bool read(T* t);

    bool eof() const { return buffer_ == end_; }

  private:
    const char* buffer_{nullptr};
    size_t len_{0};
    const char* end_{nullptr};
};

template <class T>
bool LiteRecordReader::read(T* t) {
    size_t out_len;
    const char* ptr;
    bool result = read(&ptr, &out_len);

    if (out_len != sizeof(T)) {
        throw std::runtime_error(std::string("unexpected size: expected ") +
                                 std::to_string(sizeof(T)) + ", got " +
                                 std::to_string(out_len));
    }

    *t = *reinterpret_cast<const T*>(ptr);

    return result;
}

#endif /* PBRT_MESSAGES_LITE_H */
