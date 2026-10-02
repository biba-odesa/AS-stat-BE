#pragma once
#include "config.hpp"
#include "aggregation.hpp"
#include "delivery.hpp"
namespace asstats {
struct AppConfig {
    std::vector<SourceConfig> sources;
    AggregationConfig aggregation;
    DeliveryConfig delivery;
    std::string output="asstat-final.json", windows_output="asstat-windows.jsonl";
    uint64_t duration=0,warmup=15,report_interval=10;
    size_t knownlinks_line=0;
};
AppConfig read_app_config(std::istream&);
KnownLinks read_knownlinks(const AppConfig&);
} // namespace asstats
