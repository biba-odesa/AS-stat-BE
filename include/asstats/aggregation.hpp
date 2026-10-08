#pragma once
#include "byte_count.hpp"
#include "record.hpp"
#include <deque>
#include <array>
#include <atomic>
#include <functional>
#include <istream>
#include <map>
#include <mutex>
#include <tuple>
namespace asstats {
struct SamplingPolicy {bool sampled=true;uint64_t rate=1;uint64_t multiplier() const {return sampled?rate:1;}};
struct AggregationConfig {
    std::string knownlinks_path;
    uint64_t close_delay_seconds=5;
    size_t max_active_windows=4,max_active_keys=100000,writer_queue_records=32768,writer_queue_bytes=16777216;
    std::optional<uint32_t> replace_asn;
    std::vector<std::pair<uint32_t,uint32_t>> private_asn_ranges;
    std::vector<std::pair<uint32_t,uint32_t>> excluded_asns;
    std::map<std::string,SamplingPolicy> sampling;
};
class KnownLinks {
public:
    explicit KnownLinks(std::istream&);
    const std::string* find(const std::string& exporter,uint32_t index) const;
    size_t size() const {return links_.size();}
private:
    std::map<std::pair<std::string,uint32_t>,std::string> links_;
};
struct SeriesKey {
    std::string link_id;
    uint32_t asn{};
    uint8_t direction{},ip_version{};
    auto operator<=>(const SeriesKey&) const = default;
};
struct WindowRow {uint64_t start{},end{};SeriesKey key;ByteCount bytes;bool partial{};};
std::string window_json(const WindowRow&);
struct WindowSummary {uint64_t start{};size_t keys{};bool partial{};};
struct AccountingStats {
    uint64_t records{},missing_bytes{},invalid_family{},eligible_sides{},accounted_sides{};
    uint64_t missing_ifindex{},zero_ifindex{},unknown_ifindex{},missing_asn{},excluded_asn{};
    uint64_t late_records{},late_sides{},limit_sides{},key_limit_sides{},window_limit_sides{},overflow_errors{},output_rejections{},closed_windows{},rows{};
    uint64_t options_checked{},options_mismatch{},last_mismatch_source_id{},last_mismatch_value{};
    std::string last_mismatch_source, last_mismatch_scope;
    ByteCount accounted_bytes,late_side_bytes,late_raw_record_bytes,limit_bytes;
    size_t active_keys{},active_windows{},peak_keys{},peak_windows{};
    bool failed{};
    std::deque<WindowSummary> recent_windows;
};
// A closed-window sink must return promptly. False means a failed verification, never silent loss.
using WindowSink=std::function<bool(const WindowRow&)>;
// One global minute, including an empty minute, crosses this boundary atomically.
using MinuteSink=std::function<bool(uint64_t,bool,const std::vector<WindowRow>&)>;
class MinuteAggregator {
public:
    MinuteAggregator(AggregationConfig,KnownLinks,uint64_t coverage_start_ns,WindowSink,MinuteSink={});
    void add(const FlowRecord&,uint64_t processing_ns);
    void options(const OptionsRecord&);
    void incomplete_minute(uint64_t received_ns);
    void tick(uint64_t now_ns);
    void stop_receiving(uint64_t coverage_end_ns);
    void finish(uint64_t coverage_end_ns);
    AccountingStats stats() const;
private:
    using Values=std::map<SeriesKey,ByteCount>;
    mutable std::mutex mutex_;
    AggregationConfig config_;
    KnownLinks links_;
    uint64_t start_ns_,stop_ns_{},watermark_ns_{};
    bool finished_{};
    std::array<std::atomic<uint64_t>,128> incomplete_minutes_{};
    WindowSink sink_;
    MinuteSink minute_sink_;
    uint64_t next_emit_minute_{};
    void emit_empty(uint64_t);
    AccountingStats stats_;
    std::map<uint64_t,Values> windows_;
    void close_due(uint64_t);
    void emit(std::map<uint64_t,Values>::iterator);
};
std::string accounting_json(const AccountingStats&);
} // namespace asstats
