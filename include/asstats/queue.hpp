#pragma once
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
namespace asstats {
struct DatagramInfo {
    uint64_t received_ns{}, received_monotonic_ns{};
    uint32_t exporter{}, destination{};
    uint16_t source_port{}, destination_port{};
};
struct QueuedDatagram {
    DatagramInfo info;
    size_t size{};
    std::array<uint8_t,65535> payload;
};
struct QueueSnapshot {
    size_t count{}, payload_bytes{}, peak_count{}, peak_payload_bytes{};
    size_t allocated_bytes{}, count_limit{}, payload_capacity{};
    uint64_t accepted{}, drops{}, closed_rejections{};
};
class BoundedQueue {
public:
    BoundedQueue(size_t datagrams, size_t memory_bytes);
    static size_t descriptor_bytes(size_t count);
    bool push(const DatagramInfo&, std::span<const uint8_t>);
    bool pop(QueuedDatagram&, std::chrono::milliseconds wait);
    void close();
    bool drained() const;
    QueueSnapshot snapshot() const;
private:
    struct Slot { DatagramInfo info; size_t length; };
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::unique_ptr<Slot[]> slots_;
    std::unique_ptr<uint8_t[]> bytes_;
    QueueSnapshot stats_;
    size_t read_slot_{}, write_slot_{}, read_byte_{}, write_byte_{};
    bool closed_{};
};
} // namespace asstats
