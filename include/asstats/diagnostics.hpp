#pragma once
#include <cstdint>

#define ASSTATS_DECODER_COUNTERS(X) \
    X(datagrams) \
    X(wrong_version) \
    X(malformed_datagrams) \
    X(flowsets) \
    X(data_template_flowsets) \
    X(options_template_flowsets) \
    X(data_templates) \
    X(options_templates) \
    X(template_refreshes) \
    X(template_replacements) \
    X(malformed_template_flowsets) \
    X(cache_limit_rejections) \
    X(unsupported_short_records) \
    X(unknown_template_flowsets) \
    X(unknown_template_bytes) \
    X(malformed_data_flowsets) \
    X(data_records) \
    X(options_records) \
    X(ipv4_records) \
    X(ipv6_records) \
    X(incomplete_records) \
    X(invalid_family_records) \
    X(reserved_flowsets) \
    X(unaligned_flowsets) \
    X(zero_length_system_scopes) \
    X(template_expiries)

namespace asstats {
struct Diagnostics {
#define FIELD(name) uint64_t name{};
    ASSTATS_DECODER_COUNTERS(FIELD)
#undef FIELD
};
inline void add_difference(Diagnostics& out, const Diagnostics& after, const Diagnostics& before) {
#define DELTA(name) out.name += after.name - before.name;
    ASSTATS_DECODER_COUNTERS(DELTA)
#undef DELTA
}
} // namespace asstats
