#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace asstats {
struct SourceMetadata {
    std::string configured_source;
    std::string exporter_ip;
    std::string destination_ip;
    uint16_t source_port{};
    uint16_t destination_port{};
    uint64_t received_ns{};
};
struct RecordIdentity {
    SourceMetadata source;
    uint32_t source_id{};
    uint16_t template_id{};
};
struct FlowRecord {
    RecordIdentity identity;
    std::optional<uint8_t> ip_version;
    std::optional<uint32_t> source_asn, destination_asn, input_ifindex, output_ifindex;
    std::optional<uint64_t> bytes, packets;
    std::optional<uint32_t> first_switched, last_switched;
    bool complete() const {
        return ip_version && source_asn && destination_asn && input_ifindex &&
               output_ifindex && bytes && packets;
    }
};
// Scope IDs have their own namespace and must never be interpreted as data field IDs.
struct ScopeValue {
    uint16_t type{};
    std::vector<uint8_t> raw;
};
struct OptionValue {
    uint16_t field{};
    uint64_t value{};
};
struct OptionsRecord {
    RecordIdentity identity;
    std::vector<ScopeValue> scopes;
    std::vector<OptionValue> values;
};
} // namespace asstats
