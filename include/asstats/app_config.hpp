#pragma once
#include "config.hpp"
#include "aggregation.hpp"
#include "delivery.hpp"
namespace asstats {
struct ArchiveConfig {
    std::string name,url;
    uint64_t interval_seconds{},retention_days{};
    size_t line{};
};
struct AppConfig {
    std::vector<SourceConfig> sources;
    AggregationConfig aggregation;
    DeliveryConfig delivery;
    std::vector<ArchiveConfig> archives;
    std::string archive_state_directory="/var/lib/asstat/archives";
    uint64_t archive_state_max_bytes=1073741824,archive_queue_bytes=67108864,archive_queue_minutes=8;
    uint64_t archive_max_keys=1000000,archive_outbox_rows=1000000;
    std::string output="asstat-final.json", windows_output="asstat-windows.jsonl";
    uint64_t duration=0,warmup=15,report_interval=10;
    size_t knownlinks_line=0;
};
AppConfig read_app_config(std::istream&);
KnownLinks read_knownlinks(const AppConfig&);
} // namespace asstats
