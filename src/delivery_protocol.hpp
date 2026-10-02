#pragma once
#include <cstdint>
#include <chrono>
#include <string>
namespace asstats {
constexpr size_t delivery_payload_limit=131072;
struct DeliveryRequest {uint64_t rows{},first{},last{};uint32_t size{},stop{};};
#define DELIVERY_FIELDS(X) X(pending_batches) X(pending_bytes) X(oldest_timestamp) X(peak_batches) X(peak_bytes) \
 X(saved_batches) X(saved_rows) X(sent_batches) X(sent_rows) X(retries) X(http_errors) X(permanent_errors) \
 X(disk_errors) X(corrupt_files) X(unfinished_files) X(expired_batches) X(lost_rows) X(overflow_batches) X(free_bytes)
struct DeliveryReply {
#define FIELD(n) uint64_t n{};
 DELIVERY_FIELDS(FIELD)
#undef FIELD
 char error[192]{};
};
inline std::string delivery_reply_json(const DeliveryReply& r) {
    std::string out="{";
#define FIELD(n) out+="\"" #n "\":"+std::to_string(r.n)+",";
    DELIVERY_FIELDS(FIELD)
#undef FIELD
    const auto now=static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    out+="\"oldest_age_seconds\":"+std::to_string(r.oldest_timestamp&&now>r.oldest_timestamp?now-r.oldest_timestamp:0)+",\"retention_seconds\":604800}";return out;
}
}
