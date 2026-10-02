#include "asstats/pcap.hpp"
#include <arpa/inet.h>
#include <array>
#include <stdexcept>
#include <vector>

namespace asstats {
namespace {
uint16_t be16(std::span<const uint8_t> b, size_t p) {
    return static_cast<uint16_t>((uint16_t(b[p]) << 8) | b[p + 1]);
}
uint32_t integer(std::span<const uint8_t> b, bool little) {
    uint32_t n = 0;
    if (little) for (auto i = b.rbegin(); i != b.rend(); ++i) n = (n << 8) | *i;
    else for (uint8_t v : b) n = (n << 8) | v;
    return n;
}
void exact(std::istream& in, std::span<uint8_t> b) {
    if (!in.read(reinterpret_cast<char*>(b.data()), static_cast<std::streamsize>(b.size())))
        throw std::runtime_error("Truncated PCAP or read error");
}
std::string address(std::span<const uint8_t> b, int af) {
    std::array<char, INET6_ADDRSTRLEN> text{};
    if (!inet_ntop(af, b.data(), text.data(), static_cast<socklen_t>(text.size())))
        throw std::runtime_error("Cannot format IP address");
    return text.data();
}
}
void PcapReader::read(std::istream& in, const std::string& configured_source, uint16_t port,
                      const UdpSink& sink) {
    std::array<uint8_t, 24> h{};
    exact(in, h);
    const auto magic = integer(std::span(h).first(4), false);
    const bool little = magic == 0xd4c3b2a1 || magic == 0x4d3cb2a1;
    const bool nano = magic == 0xa1b23c4d || magic == 0x4d3cb2a1;
    if (!little && magic != 0xa1b2c3d4 && magic != 0xa1b23c4d)
        throw std::runtime_error("Only classic PCAP is supported (not PCAPNG)");
    const auto read = [&](size_t pos, size_t n) { return integer(std::span(h).subspan(pos, n), little); };
    const auto snaplen = read(16, 4);
    constexpr uint32_t max_packet = 1024 * 1024;
    if (read(4, 2) != 2 || read(6, 2) != 4 || read(20, 4) != 1 || !snaplen || snaplen > max_packet)
        throw std::runtime_error("Expected PCAP 2.4 Ethernet, snaplen <= 1 MiB");
    while (true) {
        const auto next = in.peek();
        if (next == std::char_traits<char>::eof()) {
            if (in.bad() || !in.eof()) throw std::runtime_error("PCAP read error");
            break;
        }
        exact(in, std::span(h).first(16));
        const uint32_t seconds = read(0, 4), fraction = read(4, 4);
        const uint32_t captured = read(8, 4), original = read(12, 4);
        if (captured > snaplen || captured > original || fraction >= (nano ? 1000000000U : 1000000U))
            throw std::runtime_error("Invalid PCAP packet header");
        std::vector<uint8_t> packet(captured);
        exact(in, packet);
        ++stats_.packets;
        if (captured < original) { ++stats_.truncated_packets; continue; }
        SourceMetadata metadata;
        metadata.configured_source = configured_source;
        metadata.received_ns = uint64_t(seconds) * 1000000000ULL + uint64_t(fraction) * (nano ? 1 : 1000);
        ethernet(packet, std::move(metadata), port, sink);
    }
}
void PcapReader::ethernet(std::span<const uint8_t> p, SourceMetadata source, uint16_t port,
                          const UdpSink& sink) {
    const auto bad = [this]() { ++stats_.malformed_packets; };
    if (p.size() < 14) { bad(); return; }
    size_t pos = 14;
    auto type = be16(p, 12);
    unsigned tags = 0;
    while (type == 0x8100 || type == 0x88a8) {
        if (++tags > 4) { ++stats_.unsupported_packets; return; }
        if (p.size() - pos < 4) { bad(); return; }
        type = be16(p, pos + 2);
        pos += 4;
    }
    size_t end = 0;
    uint8_t protocol = 0;
    if (type == 0x0800) {
        if (p.size() - pos < 20 || (p[pos] >> 4) != 4) { bad(); return; }
        const size_t ihl = (p[pos] & 15) * 4U;
        const size_t total = be16(p, pos + 2);
        if (ihl < 20 || total < ihl || total > p.size() - pos) { bad(); return; }
        if (be16(p, pos + 6) & 0x3fff) { ++stats_.fragments; return; }
        source.exporter_ip = address(p.subspan(pos + 12, 4), AF_INET);
        source.destination_ip = address(p.subspan(pos + 16, 4), AF_INET);
        protocol = p[pos + 9];
        end = pos + total;
        pos += ihl;
    } else if (type == 0x86dd) {
        if (p.size() - pos < 40 || (p[pos] >> 4) != 6) { bad(); return; }
        const size_t length = be16(p, pos + 4);
        if (!length) { ++stats_.unsupported_packets; return; } // No jumbograms.
        if (length > p.size() - pos - 40) { bad(); return; }
        source.exporter_ip = address(p.subspan(pos + 8, 16), AF_INET6);
        source.destination_ip = address(p.subspan(pos + 24, 16), AF_INET6);
        protocol = p[pos + 6];
        pos += 40;
        end = pos + length;
        unsigned extensions = 0;
        while (protocol == 0 || protocol == 43 || protocol == 60 || protocol == 51) {
            if (++extensions > 8) { ++stats_.unsupported_packets; return; }
            if (end - pos < 2) { bad(); return; }
            const size_t width = protocol == 51 ? (size_t(p[pos + 1]) + 2) * 4 : (size_t(p[pos + 1]) + 1) * 8;
            if (width > end - pos) { bad(); return; }
            protocol = p[pos];
            pos += width;
        }
        if (protocol == 44) { ++stats_.fragments; return; }
    } else { ++stats_.unsupported_packets; return; }
    if (protocol != 17) { ++stats_.unsupported_packets; return; }
    if (end - pos < 8) { bad(); return; }
    const size_t length = be16(p, pos + 4);
    if (length < 8 || length != end - pos) { bad(); return; }
    source.source_port = be16(p, pos);
    source.destination_port = be16(p, pos + 2);
    if (port && source.destination_port != port) { ++stats_.filtered_datagrams; return; }
    ++stats_.udp_datagrams;
    sink(p.subspan(pos + 8, length - 8), source);
}
} // namespace asstats
