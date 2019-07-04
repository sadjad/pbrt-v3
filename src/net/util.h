#ifndef PBRT_NET_UTIL_H
#define PBRT_NET_UTIL_H

#include <string>
#include <cstring>
std::string put_field(const bool n);
std::string put_field(const uint64_t n);
std::string put_field(const uint32_t n);
std::string put_field(const uint16_t n);

void put_field(std::string& message, const bool n, size_t loc);
void put_field(std::string& message, uint64_t n, size_t loc);
void put_field(std::string& message, uint32_t n, size_t loc);
void put_field(std::string& message, uint16_t n, size_t loc);



/* avoid implicit conversions */
template <class T>
std::string put_field(T n) = delete;

#endif /* PBRT_NET_UTIL_H */
