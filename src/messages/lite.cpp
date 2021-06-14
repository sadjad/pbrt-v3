#include "lite.h"

using namespace std;

LiteRecordReader::LiteRecordReader(const char* buffer, const size_t len)
    : buffer_(buffer), len_(len), end_(buffer_ + len_) {}

bool LiteRecordReader::read(const char** out_buffer, size_t* out_len) {
    // first 4 bytes are the record length
    if (buffer_ + 4 > end_) {
        return false;
    }

    const uint32_t* record_len = reinterpret_cast<const uint32_t*>(buffer_);

    if (out_len != nullptr) {
        *out_len = *record_len;
    }

    *out_buffer = buffer_ + 4;
    buffer_ += (*record_len) + 4;

    return buffer_ <= end_;
}

void LiteRecordWriter::write(const char* buf, const uint32_t len) {
    fout_.write(reinterpret_cast<const char*>(&len), sizeof(len));
    fout_.write(buf, len);
}
