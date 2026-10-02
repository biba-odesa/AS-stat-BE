#include "asstats/aggregation.hpp"
#include <algorithm>
#include <sstream>
namespace asstats {
namespace {
constexpr uint64_t ns=1000000000ULL;
}
std::string window_json(const WindowRow& r) {
    std::ostringstream out;out<<"{\"window_start\":"<<r.start<<",\"window_end\":"<<r.end<<",\"link_id\":\""<<r.key.link_id
      <<"\",\"asn\":"<<r.key.asn<<",\"direction\":\""<<(r.key.direction?"out":"in")<<"\",\"ip_version\":"<<unsigned(r.key.ip_version)
      <<",\"bytes\":\""<<r.bytes.str()<<"\",\"partial\":"<<(r.partial?"true":"false")<<"}\n";return out.str();
}
MinuteAggregator::MinuteAggregator(AggregationConfig config,KnownLinks links,uint64_t start,WindowSink sink)
    :config_(std::move(config)),links_(std::move(links)),start_ns_(start),sink_(std::move(sink)) {
    if(!config_.max_active_windows||!config_.max_active_keys||config_.close_delay_seconds>3600||!sink_) throw std::invalid_argument("Invalid aggregator limits/sink");
}
void MinuteAggregator::emit(std::map<uint64_t,Values>::iterator it) {
    const uint64_t start=it->first,end=start+60;
    const bool partial=UInt128(start)*ns<start_ns_||(stop_ns_&&UInt128(end)*ns>stop_ns_);
    for(const auto& [key,value]:it->second) {
        if(!sink_({start,end,key,value,partial})) {++stats_.output_rejections;stats_.failed=true;}
        else ++stats_.rows;
    }
    ++stats_.closed_windows;
    stats_.recent_windows.push_back({start,it->second.size(),partial});
    if(stats_.recent_windows.size()>64)stats_.recent_windows.pop_front();
    stats_.active_keys-=it->second.size();windows_.erase(it);stats_.active_windows=windows_.size();
}
void MinuteAggregator::close_due(uint64_t now) {
    watermark_ns_=std::max(watermark_ns_,now);
    while(!windows_.empty()) {
        auto it=windows_.begin();
        if(watermark_ns_/ns<it->first+60+config_.close_delay_seconds) break;
        emit(it);
    }
}
void MinuteAggregator::tick(uint64_t now) {std::lock_guard lock(mutex_);close_due(now);}
void MinuteAggregator::stop_receiving(uint64_t end) {std::lock_guard lock(mutex_);stop_ns_=end;}
void MinuteAggregator::finish(uint64_t end) {
    std::lock_guard lock(mutex_);stop_ns_=end;
    while(!windows_.empty()) emit(windows_.begin());
    finished_=true;
}
void MinuteAggregator::add(const FlowRecord& r,uint64_t now) {
    std::lock_guard lock(mutex_);
    try {
        ++stats_.records;close_due(now);
        bool invalid=false;
        if(!r.bytes) {++stats_.missing_bytes;invalid=true;}
        if(!r.ip_version||(*r.ip_version!=4&&*r.ip_version!=6)) {++stats_.invalid_family;invalid=true;}
        if(invalid) return;
        auto policy=config_.sampling.find(r.identity.source.configured_source);
        if(policy==config_.sampling.end()) {stats_.failed=true;throw std::runtime_error("Missing source sampling policy");}
        ByteCount bytes=ByteCount(*r.bytes).multiplied(policy->second.multiplier());
        std::vector<SeriesKey> keys;keys.reserve(2);
        auto side=[&](const auto& index,const auto& asn,uint8_t direction) {
            if(!index) {++stats_.missing_ifindex;return;}
            if(!*index) {++stats_.zero_ifindex;return;}
            const auto* link=links_.find(r.identity.source.exporter_ip,*index);
            if(!link) {++stats_.unknown_ifindex;return;}
            if(!asn) {++stats_.missing_asn;return;}
            uint32_t accounted_asn=*asn;
            if(accounted_asn!=0&&config_.replace_asn)for(const auto& [lo,hi]:config_.private_asn_ranges)
                if(accounted_asn>=lo&&accounted_asn<=hi){accounted_asn=*config_.replace_asn;break;}
            for(const auto& [lo,hi]:config_.excluded_asns)if(accounted_asn>=lo&&accounted_asn<=hi) {++stats_.excluded_asn;return;}
            keys.push_back({*link,accounted_asn,direction,*r.ip_version});++stats_.eligible_sides;
        };
        side(r.input_ifindex,r.source_asn,0);side(r.output_ifindex,r.destination_asn,1);
        if(keys.empty())return;
        const uint64_t window=(r.identity.source.received_ns/ns/60)*60;
        if(finished_||watermark_ns_/ns>=window+60+config_.close_delay_seconds) {
            ++stats_.late_records;stats_.late_raw_record_bytes.add(ByteCount(*r.bytes));
            for(const auto& key:keys) {(void)key;++stats_.late_sides;stats_.late_side_bytes.add(bytes);}return;
        }
        for(const auto& key:keys) {
            auto w=windows_.find(window);
            const bool new_window=w==windows_.end();
            const bool new_key=new_window||!w->second.contains(key);
            if((new_window&&windows_.size()>=config_.max_active_windows)||(new_key&&stats_.active_keys>=config_.max_active_keys)) {
                if(new_window&&windows_.size()>=config_.max_active_windows)++stats_.window_limit_sides;
                if(new_key&&stats_.active_keys>=config_.max_active_keys)++stats_.key_limit_sides;
                ++stats_.limit_sides;stats_.limit_bytes.add(bytes);stats_.failed=true;continue;
            }
            auto& values=windows_[window];values[key].add(bytes);
            stats_.accounted_bytes.add(bytes);++stats_.accounted_sides;
            if(new_key)++stats_.active_keys;
            stats_.active_windows=windows_.size();stats_.peak_keys=std::max(stats_.peak_keys,stats_.active_keys);
            stats_.peak_windows=std::max(stats_.peak_windows,stats_.active_windows);
        }
    }catch(const std::overflow_error&){++stats_.overflow_errors;stats_.failed=true;throw;}
}
void MinuteAggregator::options(const OptionsRecord& r) {
    std::lock_guard lock(mutex_);
    const auto p=config_.sampling.find(r.identity.source.configured_source);
    if(p==config_.sampling.end()) {stats_.failed=true;return;}
    for(const auto& v:r.values) if(v.field==34||v.field==50) {
        ++stats_.options_checked;
        if(v.value!=p->second.rate) {
            ++stats_.options_mismatch;stats_.last_mismatch_source=r.identity.source.configured_source;
            stats_.last_mismatch_source_id=r.identity.source_id;stats_.last_mismatch_value=v.value;
            std::ostringstream scope;
            for(const auto& s:r.scopes) {
                scope<<s.type<<':';
                if(s.raw.size()<=8){uint64_t n=0;for(auto b:s.raw)n=(n<<8)|b;scope<<n;}
                else scope<<"wide";
                scope<<'/'<<s.raw.size()<<';';
            }
            stats_.last_mismatch_scope=scope.str().substr(0,1024);
        }
    }
}
AccountingStats MinuteAggregator::stats() const {std::lock_guard lock(mutex_);return stats_;}
std::string accounting_json(const AccountingStats& s) {
    std::ostringstream o;o<<'{';
#define ITEM(n) o<<"\"" #n "\":"<<s.n<<',';
    ITEM(records);ITEM(missing_bytes);ITEM(invalid_family);ITEM(eligible_sides);ITEM(accounted_sides);
    ITEM(missing_ifindex);ITEM(zero_ifindex);ITEM(unknown_ifindex);ITEM(missing_asn);ITEM(excluded_asn);
    ITEM(late_records);ITEM(late_sides);ITEM(limit_sides);ITEM(key_limit_sides);ITEM(window_limit_sides);ITEM(overflow_errors);ITEM(output_rejections);
    ITEM(closed_windows);ITEM(rows);ITEM(options_checked);ITEM(options_mismatch);ITEM(active_keys);ITEM(active_windows);ITEM(peak_keys);ITEM(peak_windows);
    ITEM(last_mismatch_source_id);ITEM(last_mismatch_value);
#undef ITEM
    o<<"\"accounted_bytes\":\""<<s.accounted_bytes.str()<<"\",\"late_side_bytes\":\""<<s.late_side_bytes.str()
     <<"\",\"late_raw_record_bytes\":\""<<s.late_raw_record_bytes.str()<<"\",\"limit_bytes\":\""<<s.limit_bytes.str()
     <<"\",\"last_mismatch_source\":\""<<s.last_mismatch_source<<"\",\"last_mismatch_scope\":\""<<s.last_mismatch_scope
     <<"\",\"failed\":"<<(s.failed?"true":"false")<<",\"recent_windows\":[";
    bool comma=false;for(const auto& w:s.recent_windows) {if(comma)o<<',';comma=true;
        o<<"{\"window_start\":"<<w.start<<",\"keys\":"<<w.keys<<",\"partial\":"<<(w.partial?"true":"false")<<'}';}
    o<<"]}";return o.str();
}
} // namespace asstats
