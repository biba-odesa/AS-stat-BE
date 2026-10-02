#include "asstats/app_config.hpp"
#include <iostream>
#include <sstream>
using namespace asstats;
void require(bool v){if(!v)throw std::runtime_error("Assertion failed");}
const std::string base=R"(# Complete fixture
netflow9_ports = 2055, 2056
bind_address = 127.0.0.1
exporters = {2055 : 127.0.0.1, 2056:127.0.0.2}
samplerate = {127.0.0.1:100,127.0.0.2:125}
exported_counters = {127.0.0.1:sampled,127.0.0.2:already_scaled}
knownlinks_file = fixture
replace_asn = 64496
private_asn_ranges = {64512,65534}, {4200000000,4294967294}
exclude_asn = 64496
)";
AppConfig parse(std::string s){std::istringstream in(s);return read_app_config(in);}
std::string replace(std::string from,std::string to){auto s=base;auto at=s.find(from);require(at!=std::string::npos);s.replace(at,from.size(),to);return s;}
int main(){try {
    auto buffer=parse(base+"socket_receive_buffer_bytes = 1048576\n");require(buffer.sources[0].socket_receive_buffer_bytes==1048576);
    auto c=parse(base);require(c.sources.size()==2&&c.sources[0].port==2055&&c.sources[1].port==2056);
    require(c.aggregation.sampling.at("netflow9-2055").multiplier()==100);
    require(c.aggregation.sampling.at("netflow9-2056").multiplier()==1);
    for(auto [from,to]:std::vector<std::pair<std::string,std::string>>{
        {"2055, 2056","2055,2055"},{"2055, 2056","0,2056"},{"2055, 2056","65536,2056"},
        {"2055 : 127.0.0.1, 2056:127.0.0.2","2055:127.0.0.1"},
        {"2055 : 127.0.0.1, 2056:127.0.0.2","2055:127.0.0.1,2055:127.0.0.2"},
        {"2056:127.0.0.2","2057:127.0.0.2"},{"127.0.0.1:100","127.0.0.1:0"},
        {"127.0.0.1:100,127.0.0.2:125","127.0.0.1:100"},
        {"127.0.0.2:125","127.0.0.3:125"},{"already_scaled","auto"},
        {"127.0.0.1:sampled,127.0.0.2:already_scaled","127.0.0.1:sampled"},
        {"bind_address = 127.0.0.1","bind_address = 0.0.0.0"},{"2056:127.0.0.2","2056:host"},
        {"64512,65534","65534,64512"},{"4294967294","4294967296"},
        {"replace_asn = 64496","replace_asn = -1"},{"exclude_asn = 64496","exclude_asn = 4294967296"},
        {"netflow9_ports","unknown"},{"knownlinks_file = fixture","knownlinks_file = "},
        {"netflow9_ports = 2055, 2056","netflow9_ports = 2055,2056,"}}){
        bool bad=false;try{parse(replace(from,to));}catch(const std::exception& e){bad=std::string(e.what()).starts_with("Config line ");}require(bad);
    }
    for(auto extra:{"bind_address = 127.0.0.1","queue_datagrams = 0","queue_memory_bytes = 1024","template_ttl_seconds = 0","max_active_keys = 0","writer_queue_bytes = 999999999","report_interval = 0","delivery_enabled = yes","socket_receive_buffer_bytes = 16777217","spool_max_files = 0","spool_max_bytes = 1","delivery_batch_bytes = 999999","delivery_http_timeout_ms = 0","victoriametrics_url = http://external:8428","spool_directory = /var/lib/victoriametrics","delivery_retry_max_ms = 1"}){
        bool bad=false;try{parse(base+extra+'\n');}catch(const std::exception& e){bad=std::string(e.what()).starts_with("Config line ");}require(bad);
    }
    bool line_ok=false;
    try{parse(base+"report_interval = 0\n");}catch(const std::exception& e){line_ok=std::string(e.what()).starts_with("Config line 11: Invalid integer");}
    require(line_ok);
    auto shared=base;
    for(auto pair:{std::pair{"2056:127.0.0.2","2056:127.0.0.1"},std::pair{",127.0.0.2:125",""},std::pair{",127.0.0.2:already_scaled",""}}){auto at=shared.find(pair.first);shared.replace(at,std::string(pair.first).size(),pair.second);}
    auto same_router=parse(shared);require(same_router.aggregation.sampling.at("netflow9-2056").multiplier()==100);
    auto empty=base;for(auto pair:{std::pair{"replace_asn = 64496","replace_asn = none"},std::pair{"private_asn_ranges = {64512,65534}, {4200000000,4294967294}","private_asn_ranges = {}"},std::pair{"exclude_asn = 64496","exclude_asn = "}}){auto at=empty.find(pair.first);empty.replace(at,std::string(pair.first).size(),pair.second);}
    auto unfiltered=parse(empty);require(!unfiltered.aggregation.replace_asn&&unfiltered.aggregation.excluded_asns.empty()&&unfiltered.aggregation.private_asn_ranges.empty());
    auto run=[&](AggregationConfig ac,std::optional<uint32_t> src,std::optional<uint32_t> dst){
        std::istringstream l("127.0.0.1 1 stable Link ff0000 999\n127.0.0.2 1 stable Link ff0000 999\n");
        std::vector<WindowRow> rows;MinuteAggregator a(ac,KnownLinks(l),60000000000ULL,[&](auto r){rows.push_back(r);return true;});
        FlowRecord r;r.identity.source.configured_source="netflow9-2055";r.identity.source.exporter_ip="127.0.0.1";r.identity.source.received_ns=61000000000ULL;
        r.ip_version=4;r.bytes=10;r.source_asn=src;r.destination_asn=dst;r.input_ifindex=1;r.output_ifindex=1;
        a.add(r,61000000000ULL);r.identity.source.configured_source="netflow9-2056";r.identity.source.exporter_ip="127.0.0.2";r.ip_version=6;a.add(r,61000000000ULL);
        a.finish(120000000000ULL);return rows;
    };
    auto rows=run(c.aggregation,64512,0);require(rows.size()==2&&rows[0].key.direction==1&&rows[0].key.asn==0&&rows[0].bytes.str()=="1000"&&rows[1].bytes.str()=="10");
    rows=run(c.aggregation,0,4200000000U);require(rows.size()==2&&rows[0].key.direction==0);
    auto retained=c.aggregation;retained.excluded_asns.clear();rows=run(retained,64512,123);require(rows.size()==4&&rows.back().key.asn==64496);
    retained.replace_asn.reset();rows=run(retained,64512,0);require(rows.size()==4&&rows.back().key.asn==64512);
    retained.excluded_asns={{0,0}};rows=run(retained,64512,0);require(rows.size()==2);
    rows=run(retained,std::nullopt,123);require(rows.size()==2&&rows[0].key.direction==1);
    retained=c.aggregation;retained.private_asn_ranges={{0,65534}};retained.excluded_asns.clear();
    rows=run(retained,0,64512);require(rows.size()==4&&rows[0].key.asn==0&&rows.back().key.asn==64496);
    std::cout<<"PASS unified parser, mappings/limits, router sampling and independent replacement/exclusion\n";
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
