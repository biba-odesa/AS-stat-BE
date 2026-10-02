#pragma once
#include "config.hpp"
#include "queue.hpp"
#include <optional>
namespace asstats {
uint64_t monotonic_ns();
uint64_t realtime_ns();
struct ReceivedDatagram {
    DatagramInfo info;
    size_t original_size{};
    bool truncated{}, control_truncated{}, kernel_timestamp{}, destination_fallback{};
    std::optional<uint32_t> socket_drop_counter;
};
class UdpSocket {
public:
    explicit UdpSocket(const SourceConfig&, bool request_kernel_timestamp=true);
    ~UdpSocket();
    UdpSocket(const UdpSocket&)=delete;
    UdpSocket& operator=(const UdpSocket&)=delete;
    bool receive(std::span<uint8_t>, ReceivedDatagram&);
    int fd() const {return fd_;}
    uint16_t port() const {return port_;}
    bool timestamp_enabled() const {return timestamp_enabled_;}
    bool overflow_enabled() const {return overflow_enabled_;}
    int receive_buffer_bytes() const {return receive_buffer_bytes_;}
    void close();
private:
    int fd_=-1, receive_buffer_bytes_{};
    uint16_t port_{};
    uint32_t destination_{};
    bool timestamp_enabled_{}, overflow_enabled_{};
};
} // namespace asstats
