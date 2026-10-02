#pragma once
#include "record.hpp"
#include "diagnostics.hpp"
#include <functional>
#include <map>
#include <span>
#include <tuple>

namespace asstats {
struct DecoderLimits {
    size_t max_templates = 1024;
    size_t max_fields = 256;
    size_t max_record_bytes = 65535;
    bool allow_zero_length_system_scope = true;
};
class Decoder {
public:
    using Clock = std::function<uint64_t()>;
    explicit Decoder(DecoderLimits limits = {}, uint64_t template_ttl_ns = 0, Clock clock = {});
    void expire_templates();
    using FlowSink = std::function<void(const FlowRecord&)>;
    using OptionsSink = std::function<void(const OptionsRecord&)>;
    void decode(std::span<const uint8_t> payload, const SourceMetadata& source,
                const FlowSink& flow_sink, const OptionsSink& options_sink);
    const Diagnostics& diagnostics() const { return stats_; }
    size_t cache_size() const { return cache_.size(); }
    void clear_cache() { cache_.clear(); }
private:
    struct Field {
        uint16_t type{}, length{};
        bool operator==(const Field&) const = default;
    };
    struct Template {
        bool options{};
        size_t scope_count{}, width{};
        std::vector<Field> fields;
        bool operator==(const Template&) const = default;
    };
    using Key = std::tuple<std::string, std::string, uint32_t, uint16_t>;
    DecoderLimits limits_;
    Diagnostics stats_;
    struct CacheEntry { Template schema; uint64_t refreshed_ns; };
    std::map<Key, CacheEntry> cache_;
    uint64_t ttl_ns_;
    Clock clock_;
    void templates(std::span<const uint8_t> body, bool options, const SourceMetadata&, uint32_t);
    void records(std::span<const uint8_t> body, const Template&, const RecordIdentity&,
                 const FlowSink&, const OptionsSink&);
};
} // namespace asstats
