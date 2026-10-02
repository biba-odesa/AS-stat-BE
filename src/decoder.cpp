#include "asstats/decoder.hpp"
#include <algorithm>
#include <chrono>
#include <set>
#include <stdexcept>

namespace asstats {
namespace {
uint16_t u16(std::span<const uint8_t> b, size_t p) {
    return static_cast<uint16_t>((uint16_t(b[p]) << 8) | b[p + 1]);
}
uint64_t number(std::span<const uint8_t> b) {
    uint64_t n = 0;
    for (uint8_t v : b) n = (n << 8) | v;
    return n;
}
bool zeros(std::span<const uint8_t> b) {
    return std::all_of(b.begin(), b.end(), [](uint8_t v) { return v == 0; });
}
bool known_data(uint16_t id) {
    switch (id) {
    case 1: case 2: case 8: case 12: case 10: case 14: case 16: case 17:
    case 21: case 22: case 27: case 28: case 60: return true;
    default: return false;
    }
}
bool known_option(uint16_t id) {
    switch (id) {
    case 34: case 35: case 36: case 37: case 41: case 42:
    case 48: case 49: case 50: return true;
    default: return false;
    }
}
bool valid_length(uint16_t id, uint16_t length, bool options) {
    if (options) {
        switch (id) {
        case 34: case 50: return length == 4;
        case 35: case 48: case 49: return length == 1;
        case 36: case 37: return length == 2;
        case 41: case 42: return length >= 1 && length <= 8;
        default: return length > 0;
        }
    }
    switch (id) {
    case 1: case 2: return length >= 1 && length <= 8;
    case 10: case 14: case 16: case 17: return length == 2 || length == 4;
    case 8: case 12: case 21: case 22: return length == 4;
    case 27: case 28: return length == 16;
    case 60: return length == 1;
    default: return length > 0;
    }
}
}
Decoder::Decoder(DecoderLimits limits, uint64_t ttl, Clock clock)
    : limits_(limits), ttl_ns_(ttl), clock_(std::move(clock)) {
    if (!clock_) clock_ = [] {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    };
    if (!limits.max_templates || !limits.max_fields || limits.max_fields > 16383 ||
        !limits.max_record_bytes || limits.max_record_bytes > 65535)
        throw std::invalid_argument("Invalid decoder limits");
}
void Decoder::expire_templates() {
    if (!ttl_ns_) return; // Offline replay retains its existing no-TTL behavior.
    const uint64_t now = clock_();
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (now >= it->second.refreshed_ns && now - it->second.refreshed_ns >= ttl_ns_) {
            it = cache_.erase(it);
            ++stats_.template_expiries;
        } else ++it;
    }
}
void Decoder::decode(std::span<const uint8_t> p, const SourceMetadata& source,
                     const FlowSink& flow_sink, const OptionsSink& options_sink) {
    expire_templates();
    ++stats_.datagrams;
    if (p.size() < 2 || p.size() > 65535 || source.configured_source.empty() ||
        source.configured_source.size() > 256 || source.exporter_ip.empty() || source.exporter_ip.size() > 45) {
        ++stats_.malformed_datagrams;
        return;
    }
    if (u16(p, 0) != 9) { ++stats_.wrong_version; return; }
    if (p.size() < 20) { ++stats_.malformed_datagrams; return; }
    // Preflight all FlowSet boundaries before allowing any cache mutation.
    for (size_t pos = 20; pos < p.size();) {
        if (p.size() - pos < 4) { ++stats_.malformed_datagrams; return; }
        const size_t length = u16(p, pos + 2);
        if (length < 4 || length > p.size() - pos) { ++stats_.malformed_datagrams; return; }
        pos += length;
    }
    const auto domain = static_cast<uint32_t>(number(p.subspan(16, 4)));
    for (size_t pos = 20; pos < p.size();) {
        const auto id = u16(p, pos);
        const size_t length = u16(p, pos + 2);
        auto body = p.subspan(pos + 4, length - 4);
        pos += length;
        ++stats_.flowsets;
        // Some exporters omit alignment padding. Accept exact records, never arbitrary tails.
        if (length % 4) ++stats_.unaligned_flowsets;
        if (id < 2) {
            if (id == 0) ++stats_.data_template_flowsets;
            else ++stats_.options_template_flowsets;
            templates(body, id == 1, source, domain);
        } else if (id >= 256) {
            auto found = cache_.find(Key{source.configured_source, source.exporter_ip, domain, id});
            if (found == cache_.end()) {
                ++stats_.unknown_template_flowsets;
                stats_.unknown_template_bytes += body.size();
                continue;
            }
            records(body, found->second.schema, {source, domain, id}, flow_sink, options_sink);
        } else ++stats_.reserved_flowsets;
    }
}
void Decoder::templates(std::span<const uint8_t> body, bool options,
                        const SourceMetadata& source, uint32_t domain) {
    // Stage the entire FlowSet. A malformed suffix must not install or replace any template.
    std::map<uint16_t, Template> staged;
    size_t pos = 0, zero_scopes = 0;
    const auto bad = [this]() { ++stats_.malformed_template_flowsets; };
    while (pos < body.size()) {
        const size_t remaining = body.size() - pos;
        if (remaining <= 3 && zeros(body.subspan(pos)) && (body.size() + 4) % 4 == 0) {
            pos = body.size();
            break;
        }
        const size_t header = options ? 6 : 4;
        if (remaining < header) { bad(); return; }
        uint16_t id = u16(body, pos);
        size_t fields = u16(body, pos + 2), scopes = 0;
        if (options) {
            const size_t option_bytes = u16(body, pos + 4);
            if (fields == 0 || fields % 4 || option_bytes == 0 || option_bytes % 4) { bad(); return; }
            scopes = fields / 4;
            fields = scopes + option_bytes / 4;
        }
        pos += header;
        if (id < 256 || fields == 0 || fields > limits_.max_fields || fields > (body.size() - pos) / 4 ||
            staged.contains(id)) { bad(); return; }
        Template t{options, scopes, 0, {}};
        std::set<uint16_t> seen;
        for (size_t i = 0; i < fields; ++i) {
            Field field{u16(body, pos), u16(body, pos + 2)};
            pos += 4;
            if (i < scopes) {
                if (field.length == 0) {
                    if (field.type != 1 || !limits_.allow_zero_length_system_scope) { bad(); return; }
                    ++zero_scopes;
                }
            } else {
                if (!valid_length(field.type, field.length, options)) { bad(); return; }
                if ((options ? known_option(field.type) : known_data(field.type)) && !seen.insert(field.type).second) {
                    bad(); return;
                }
            }
            if (field.length > limits_.max_record_bytes - t.width) { bad(); return; }
            t.width += field.length;
            t.fields.push_back(field);
        }
        if (t.width == 0) { bad(); return; }
        staged.emplace(id, std::move(t));
    }
    if (staged.empty()) { bad(); return; }
    size_t additions = 0;
    for (const auto& [id, t] : staged) {
        (void)t;
        if (!cache_.contains(Key{source.configured_source, source.exporter_ip, domain, id})) ++additions;
    }
    if (additions > limits_.max_templates - cache_.size()) {
        ++stats_.cache_limit_rejections;
        return;
    }
    stats_.zero_length_system_scopes += zero_scopes;
    for (auto& [id, t] : staged) {
        if (options) ++stats_.options_templates;
        else ++stats_.data_templates;
        Key key{source.configured_source, source.exporter_ip, domain, id};
        auto old = cache_.find(key);
        if (old != cache_.end()) {
            if (old->second.schema == t) ++stats_.template_refreshes;
            else ++stats_.template_replacements;
        }
        cache_.insert_or_assign(std::move(key), CacheEntry{std::move(t), ttl_ns_ ? clock_() : 0});
    }
}
void Decoder::records(std::span<const uint8_t> body, const Template& t, const RecordIdentity& identity,
                      const FlowSink& flow_sink, const OptionsSink& options_sink) {
    // For widths <= 3, zero-valued records and alignment bytes cannot be distinguished safely.
    if (t.width <= 3) { ++stats_.unsupported_short_records; return; }
    const size_t count = body.size() / t.width, tail = body.size() % t.width;
    if (!count || tail > 3 || (tail && ((body.size() + 4) % 4 || !zeros(body.last(tail))))) {
        ++stats_.malformed_data_flowsets;
        return;
    }
    for (size_t r = 0; r < count; ++r) {
        const auto raw = body.subspan(r * t.width, t.width);
        size_t pos = 0;
        if (t.options) {
            OptionsRecord record{identity, {}, {}};
            for (size_t i = 0; i < t.fields.size(); ++i) {
                const auto f = t.fields[i];
                auto value = raw.subspan(pos, f.length);
                pos += f.length;
                if (i < t.scope_count) record.scopes.push_back({f.type, {value.begin(), value.end()}});
                else if (known_option(f.type)) record.values.push_back({f.type, number(value)});
            }
            ++stats_.options_records;
            if (options_sink) options_sink(record);
            continue;
        }
        FlowRecord record;
        record.identity = identity;
        bool v4 = false, v6 = false;
        for (const auto f : t.fields) {
            auto value = raw.subspan(pos, f.length);
            pos += f.length;
            // Unknown fields (including wide address-like vendor fields) are skipped by length.
            if (!known_data(f.type)) continue;
            switch (f.type) {
            case 8: case 12: v4 = true; continue;
            case 27: case 28: v6 = true; continue;
            default: break;
            }
            const uint64_t n = number(value);
            switch (f.type) {
            case 1: record.bytes = n; break;
            case 2: record.packets = n; break;
            case 16: record.source_asn = static_cast<uint32_t>(n); break;
            case 17: record.destination_asn = static_cast<uint32_t>(n); break;
            case 10: record.input_ifindex = static_cast<uint32_t>(n); break;
            case 14: record.output_ifindex = static_cast<uint32_t>(n); break;
            case 21: record.last_switched = static_cast<uint32_t>(n); break;
            case 22: record.first_switched = static_cast<uint32_t>(n); break;
            case 60: record.ip_version = static_cast<uint8_t>(n); break;
            default: break;
            }
        }
        if ((v4 && v6) || (record.ip_version && (*record.ip_version != 4 && *record.ip_version != 6)) ||
            (record.ip_version && ((v4 && *record.ip_version != 4) || (v6 && *record.ip_version != 6)))) {
            ++stats_.invalid_family_records;
            continue;
        }
        if (!record.ip_version) {
            if (v4) record.ip_version = 4;
            if (v6) record.ip_version = 6;
        }
        ++stats_.data_records;
        if (record.ip_version == 4) ++stats_.ipv4_records;
        if (record.ip_version == 6) ++stats_.ipv6_records;
        if (!record.complete()) ++stats_.incomplete_records;
        if (flow_sink) flow_sink(record);
    }
}
} // namespace asstats
