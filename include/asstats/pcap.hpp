#pragma once
#include "record.hpp"
#include <functional>
#include <istream>
#include <span>

namespace asstats {
struct PcapDiagnostics {
    uint64_t packets{}, udp_datagrams{}, truncated_packets{}, malformed_packets{};
    uint64_t unsupported_packets{}, fragments{}, filtered_datagrams{};
};
// Owns no payload after the callback returns. Classic PCAP, Ethernet, IPv4/IPv6 UDP.
class PcapReader {
public:
    using UdpSink = std::function<void(std::span<const uint8_t>, const SourceMetadata&)>;
    void read(std::istream&, const std::string& configured_source, uint16_t destination_port,
              const UdpSink&);
    const PcapDiagnostics& diagnostics() const { return stats_; }
private:
    PcapDiagnostics stats_;
    void ethernet(std::span<const uint8_t>, SourceMetadata, uint16_t, const UdpSink&);
};
} // namespace asstats
