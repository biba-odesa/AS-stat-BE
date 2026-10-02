#pragma once
#include <cstdint>
#include <istream>
#include <string>
#include <vector>
namespace asstats {
struct SourceConfig {
    std::string name, protocol, bind_ip;
    uint16_t port{};
    uint32_t bind_address{};
    std::vector<uint32_t> exporters;
    uint32_t template_ttl_seconds{};
    size_t queue_datagrams{}, queue_memory_bytes{};
    uint32_t socket_receive_buffer_bytes{};
    bool allows(uint32_t address) const;
};
std::string ipv4_text(uint32_t network_order);
} // namespace asstats
