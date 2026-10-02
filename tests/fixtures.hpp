#pragma once
#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
using Bytes = std::vector<uint8_t>;
inline void require(bool condition, const char* text) { if (!condition) throw std::runtime_error(text); }
inline void put(Bytes& b, uint64_t n, unsigned width) {
    for (unsigned i = width; i > 0; --i) b.push_back(static_cast<uint8_t>(n >> ((i - 1) * 8)));
}
inline Bytes join(Bytes a, const Bytes& b) { a.insert(a.end(), b.begin(), b.end()); return a; }
inline Bytes set(uint16_t id, Bytes body) {
    Bytes b; put(b, id, 2); put(b, body.size() + 4, 2); return join(b, body);
}
inline Bytes nf(uint32_t domain, Bytes body) {
    Bytes b; put(b, 9, 2); put(b, 1, 2); put(b, 1000, 4); put(b, 2000, 4);
    put(b, 3, 4); put(b, domain, 4); return join(b, body);
}
using Fields = std::vector<std::pair<uint16_t, uint16_t>>;
inline Bytes schema(uint16_t id, const Fields& fields) {
    Bytes b; put(b, id, 2); put(b, fields.size(), 2);
    for (auto [type, width] : fields) { put(b, type, 2); put(b, width, 2); }
    return b;
}
inline Bytes option_schema(uint16_t id, const Fields& scopes, const Fields& fields) {
    Bytes b; put(b, id, 2); put(b, scopes.size() * 4, 2); put(b, fields.size() * 4, 2);
    for (const auto& group : {scopes, fields}) for (auto [type, width] : group) {
        put(b, type, 2); put(b, width, 2);
    }
    return b;
}
