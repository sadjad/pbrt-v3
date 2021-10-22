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

    const char* cur() const { return buffer_; }
    bool eof() const { return buffer_ == end_; }

  private:
    const char* buffer_{nullptr};
    size_t len_{0};
    const char* end_{nullptr};
};

class LiteRecordWriter {
  public:
    LiteRecordWriter(const std::string& filename)
        : fout_(filename, std::ios::binary | std::ios::trunc) {}

    void write(const char* buf, const uint32_t len);
    void write(const std::string& str) { write(str.data(), str.length()); }

    void write_raw(const std::string &str);

    template <class T>
    void write(const T& t);

    template <class T>
    void write_at(const size_t offset, const T& t);

  private:
    std::ofstream fout_{};
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

template <class T>
void LiteRecordWriter::write(const T& t) {
    const uint32_t len = sizeof(T);
    write(reinterpret_cast<const char*>(&t), len);
}

template <class T>
void LiteRecordWriter::write_at(const size_t offset, const T& t) {
    const auto cur = fout_.tellp();
    fout_.seekp(offset);
    write(t);
    fout_.seekp(max(cur, fout_.tellp()));
}

#endif /* PBRT_MESSAGES_LITE_H */
