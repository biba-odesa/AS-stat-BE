#include "asstats/app_config.hpp"
#include "asstats/queue.hpp"
#include <arpa/inet.h>
#include <charconv>
#include <fstream>
#include <set>
#include <sstream>
namespace asstats {
namespace {
std::string trim(std::string s) {
    auto a=s.find_first_not_of(" \t\r\n");
    return a==std::string::npos?"":s.substr(a,s.find_last_not_of(" \t\r\n")-a+1);
}
uint64_t number(const std::string& s,uint64_t low,uint64_t high) {
    uint64_t n{};auto [end,ec]=std::from_chars(s.data(),s.data()+s.size(),n);
    if(ec!=std::errc{}||end!=s.data()+s.size()||n<low||n>high)throw std::runtime_error("Invalid integer: "+s);
    return n;
}
std::string ip(const std::string& s) {
    in_addr a{};
    if(inet_pton(AF_INET,s.c_str(),&a)!=1||ntohl(a.s_addr)>>24==0||ntohl(a.s_addr)>>24>=224)
        throw std::runtime_error("Expected unicast literal IPv4: "+s);
    return ipv4_text(a.s_addr);
}
std::vector<std::string> list(const std::string& s) {
    if(s.empty()||s=="{}")return {};
    std::vector<std::string> out;size_t begin=0;
    while(true){auto end=s.find(',',begin);auto v=trim(s.substr(begin,end==std::string::npos?end:end-begin));
        if(v.empty())throw std::runtime_error("Empty list entry");
        out.push_back(v);
        if(end==std::string::npos)break;
        begin=end+1;}
    return out;
}
std::map<std::string,std::string> mapping(const std::string& s) {
    if(s.size()<2||s.front()!='{'||s.back()!='}')throw std::runtime_error("Expected {key:value,...}");
    std::map<std::string,std::string> out;
    for(const auto& entry:list(trim(s.substr(1,s.size()-2)))){
        auto colon=entry.find(':');
        if(colon==std::string::npos||entry.find(':',colon+1)!=std::string::npos)throw std::runtime_error("Expected one colon in mapping entry");
        auto k=trim(entry.substr(0,colon)),v=trim(entry.substr(colon+1));
        if(k.empty()||v.empty()||!out.emplace(k,v).second)throw std::runtime_error("Empty/duplicate mapping entry: "+k);
    }return out;
}
}
AppConfig read_app_config(std::istream& input) {
    AppConfig c;std::map<std::string,std::pair<std::string,size_t>> values;
    using Parameters=std::map<std::string,std::pair<std::string,size_t>>;
    std::vector<std::pair<ArchiveConfig,Parameters>> sections;
    std::set<std::string> names;
    std::string line;size_t ln=0,at=0;
    try {
        while(std::getline(input,line)){
            at=++ln;if(ln>1024||line.size()>4096)throw std::runtime_error("Configuration too large");
            line=trim(line);if(line.empty()||line.front()=='#')continue;
            if(line.front()=='[') {
                if(line.back()!=']')throw std::runtime_error("Incomplete archive section header");
                auto header=trim(line.substr(1,line.size()-2));
                if(!header.starts_with("archive "))throw std::runtime_error("Expected [archive NAME]");
                auto name=trim(header.substr(8));
                if(name.empty()||name.size()>32||name.front()=='-'||name.front()=='_'||
                   name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")!=std::string::npos)
                    throw std::runtime_error("Archive name must be 1..32 safe alphanumeric, underscore or hyphen characters, starting alphanumeric");
                if(!names.insert(name).second)throw std::runtime_error("Repeated archive section: "+name);
                if(sections.size()>=16)throw std::runtime_error("At most 16 archives are supported");
                ArchiveConfig archive;archive.name=name;archive.line=ln;
                sections.emplace_back(std::move(archive),Parameters{});continue;
            }
            auto eq=line.find('=');if(eq==std::string::npos)throw std::runtime_error("Expected key = value");
            auto key=trim(line.substr(0,eq)),value=trim(line.substr(eq+1));
            auto& target=sections.empty()?values:sections.back().second;
            if(!target.emplace(key,std::make_pair(value,ln)).second)throw std::runtime_error("Repeated parameter: "+key);
        }
        if(input.bad())throw std::runtime_error("Configuration read failed");
        const std::set<std::string> allowed={"netflow9_ports","bind_address","exporters","samplerate","exported_counters","knownlinks_file","replace_asn","private_asn_ranges","exclude_asn","template_ttl_seconds","queue_datagrams","queue_memory_bytes","socket_receive_buffer_bytes","close_delay_seconds","max_active_windows","max_active_keys","writer_queue_records","writer_queue_bytes","output","windows_output","duration","warmup","report_interval","delivery_enabled","victoriametrics_url","spool_directory","spool_max_bytes","spool_max_files","delivery_queue_records","delivery_queue_bytes","delivery_batch_records","delivery_batch_bytes","delivery_http_timeout_ms","delivery_retry_initial_ms","delivery_retry_max_ms","delivery_shutdown_timeout_ms","archive_state_directory","archive_state_max_bytes","archive_queue_bytes","archive_queue_minutes","archive_max_keys","archive_outbox_rows"};
        for(const auto& [k,v]:values)if(!allowed.contains(k)){at=v.second;throw std::runtime_error("Unknown parameter: "+k);}
        auto get=[&](const std::string& key)->std::string {auto it=values.find(key);if(it==values.end()){at=ln+1;throw std::runtime_error("Missing parameter: "+key);}at=it->second.second;return it->second.first;};
        auto scalar=[&](const std::string& key,uint64_t def,uint64_t lo,uint64_t hi){return values.contains(key)?number(get(key),lo,hi):def;};
        auto bind=ip(get("bind_address"));
        std::set<uint16_t> ports;
        for(const auto& p:list(get("netflow9_ports")))if(!ports.insert(static_cast<uint16_t>(number(p,1,65535))).second)throw std::runtime_error("Duplicate UDP port");
        if(ports.empty()||ports.size()>16)throw std::runtime_error("Expected 1..16 UDP ports");
        std::map<uint16_t,std::string> exporters;std::set<std::string> routers;
        for(const auto& [p,a]:mapping(get("exporters"))){auto port=static_cast<uint16_t>(number(p,1,65535));
            if(!ports.contains(port)||!exporters.emplace(port,ip(a)).second)throw std::runtime_error("Extra/conflicting exporter port: "+p);
            routers.insert(ip(a));}
        if(exporters.size()!=ports.size())throw std::runtime_error("Each netflow9 port needs exactly one exporter");
        std::map<std::string,uint64_t> rates;
        for(const auto& [a,r]:mapping(get("samplerate"))){auto addr=ip(a);if(!routers.contains(addr)||!rates.emplace(addr,number(r,1,UINT64_MAX)).second)throw std::runtime_error("Extra/conflicting samplerate router: "+a);}
        if(rates.size()!=routers.size())throw std::runtime_error("Missing samplerate for exporter");
        std::map<std::string,bool> modes;
        for(const auto& [a,m]:mapping(get("exported_counters"))){auto addr=ip(a);
            if(m!="sampled"&&m!="already_scaled")throw std::runtime_error("Expected sampled or already_scaled");
            if(!routers.contains(addr)||!modes.emplace(addr,m=="sampled").second)throw std::runtime_error("Extra/conflicting counters router: "+a);}
        if(modes.size()!=routers.size())throw std::runtime_error("Missing exported_counters policy for exporter");
        c.aggregation.knownlinks_path=get("knownlinks_file");c.knownlinks_line=at;
        if(c.aggregation.knownlinks_path.empty())throw std::runtime_error("knownlinks_file cannot be empty");
        auto replacement=get("replace_asn");if(replacement!="none")c.aggregation.replace_asn=static_cast<uint32_t>(number(replacement,0,UINT32_MAX));
        auto ranges=get("private_asn_ranges");
        if(!ranges.empty()&&ranges!="{}"){
            size_t pos=0;
            while(pos<ranges.size()){
                if(ranges[pos]!='{')throw std::runtime_error("Expected {first,last} ASN range");
                auto end=ranges.find('}',pos);if(end==std::string::npos)throw std::runtime_error("Unclosed ASN range");
                auto v=list(ranges.substr(pos+1,end-pos-1));if(v.size()!=2)throw std::runtime_error("ASN range requires two bounds");
                auto lo=number(v[0],0,UINT32_MAX),hi=number(v[1],0,UINT32_MAX);
                if(lo>hi)throw std::runtime_error("Reversed ASN range");
                c.aggregation.private_asn_ranges.emplace_back(static_cast<uint32_t>(lo),static_cast<uint32_t>(hi));
                if(c.aggregation.private_asn_ranges.size()>128)throw std::runtime_error("Too many private ASN ranges");
                auto rest=trim(ranges.substr(end+1));if(rest.empty())break;
                if(rest.front()!=',')throw std::runtime_error("Expected comma between ASN ranges");
                ranges=trim(rest.substr(1));pos=0;if(ranges.empty())throw std::runtime_error("Trailing range comma");
            }
        }
        for(const auto& a:list(get("exclude_asn"))){auto n=static_cast<uint32_t>(number(a,0,UINT32_MAX));c.aggregation.excluded_asns.emplace_back(n,n);}
        if(c.aggregation.private_asn_ranges.size()>128||c.aggregation.excluded_asns.size()>128)throw std::runtime_error("Too many ASN rules");
        auto ttl=scalar("template_ttl_seconds",300,1,86400),count=scalar("queue_datagrams",4096,1,65536),bytes=scalar("queue_memory_bytes",8388608,1024,64ULL*1024*1024);
        if(bytes<=BoundedQueue::descriptor_bytes(count)||bytes*ports.size()>256ULL*1024*1024){if(values.contains("queue_memory_bytes"))at=values.at("queue_memory_bytes").second;throw std::runtime_error("Queue memory insufficient for descriptors or total exceeds 256 MiB");}
        auto socket_bytes=scalar("socket_receive_buffer_bytes",0,0,16ULL*1024*1024);
        for(auto port:ports){
            SourceConfig source;source.name="netflow9-"+std::to_string(port);source.protocol="netflow9";
            source.port=port;source.bind_ip=bind;
            in_addr addr{};inet_pton(AF_INET,bind.c_str(),&addr);source.bind_address=addr.s_addr;
            inet_pton(AF_INET,exporters.at(port).c_str(),&addr);source.exporters={addr.s_addr};
            source.template_ttl_seconds=static_cast<uint32_t>(ttl);source.queue_datagrams=count;source.queue_memory_bytes=bytes;
            source.socket_receive_buffer_bytes=static_cast<uint32_t>(socket_bytes);
            c.aggregation.sampling[source.name]={modes.at(exporters.at(port)),rates.at(exporters.at(port))};c.sources.push_back(std::move(source));
        }
        c.aggregation.close_delay_seconds=scalar("close_delay_seconds",5,0,3600);
        c.aggregation.max_active_windows=scalar("max_active_windows",4,1,128);
        c.aggregation.max_active_keys=scalar("max_active_keys",100000,1,1000000);
        c.aggregation.writer_queue_records=scalar("writer_queue_records",32768,1,1000000);
        c.aggregation.writer_queue_bytes=scalar("writer_queue_bytes",16777216,1,256ULL*1024*1024);
        c.duration=scalar("duration",0,0,86400);c.warmup=scalar("warmup",15,0,3600);c.report_interval=scalar("report_interval",10,1,3600);
        if(values.contains("output")){c.output=get("output");if(c.output.empty())throw std::runtime_error("output cannot be empty");}
        if(values.contains("windows_output")){c.windows_output=get("windows_output");if(c.windows_output.empty())throw std::runtime_error("windows_output cannot be empty (use none to disable)");}
        if(c.output==c.windows_output)throw std::runtime_error("Output files must differ");
        if(values.contains("delivery_enabled")) {
            auto v=get("delivery_enabled");
            if(v!="true"&&v!="false")throw std::runtime_error("delivery_enabled must be true or false");
            c.delivery.enabled=v=="true";
        }
        if(values.contains("victoriametrics_url")) {
            c.delivery.url=get("victoriametrics_url");
            // This pilot deliberately restricts delivery to the existing local service.
            const std::string prefix="http://127.0.0.1:";
            if(!c.delivery.url.starts_with(prefix))throw std::runtime_error("Expected local http://127.0.0.1:PORT URL");
            number(c.delivery.url.substr(prefix.size()),1,65535);
        }
        if(values.contains("spool_directory")) {
            c.delivery.directory=get("spool_directory");
            if(c.delivery.directory.empty()||c.delivery.directory.front()!='/')throw std::runtime_error("spool_directory must be absolute");
            if(c.delivery.directory=="/var/lib/victoriametrics"||c.delivery.directory.starts_with("/var/lib/victoriametrics/"))throw std::runtime_error("VictoriaMetrics data directory cannot be used as spool");
        }
        c.delivery.spool_bytes=scalar("spool_max_bytes",1073741824,1024,1ULL<<40);
        c.delivery.spool_files=scalar("spool_max_files",8192,1,65536);
        c.delivery.queue_records=scalar("delivery_queue_records",32768,1,1000000);
        c.delivery.queue_bytes=scalar("delivery_queue_bytes",16777216,1024,256ULL*1024*1024);
        c.delivery.batch_records=scalar("delivery_batch_records",512,1,4096);
        c.delivery.batch_bytes=scalar("delivery_batch_bytes",131072,512,131072);
        c.delivery.http_timeout_ms=scalar("delivery_http_timeout_ms",3000,10,60000);
        c.delivery.retry_initial_ms=scalar("delivery_retry_initial_ms",1000,10,3600000);
        c.delivery.retry_max_ms=scalar("delivery_retry_max_ms",60000,c.delivery.retry_initial_ms,3600000);
        c.delivery.shutdown_timeout_ms=scalar("delivery_shutdown_timeout_ms",10000,10,120000);
        if(values.contains("archive_state_directory"))c.archive_state_directory=get("archive_state_directory");
        if(c.archive_state_directory.empty()||c.archive_state_directory.front()!='/')throw std::runtime_error("archive_state_directory must be absolute");
        c.archive_state_max_bytes=scalar("archive_state_max_bytes",1073741824,16777216,1ULL<<40);
        c.archive_queue_bytes=scalar("archive_queue_bytes",67108864,1048576,268435456);
        c.archive_queue_minutes=scalar("archive_queue_minutes",8,1,64);
        c.archive_max_keys=scalar("archive_max_keys",1000000,1,4000000);
        c.archive_outbox_rows=scalar("archive_outbox_rows",1000000,1,4000000);
        std::set<std::string> endpoints;
        for(auto& [archive,params]:sections) {
            for(const auto& [key,v]:params)if(key!="url"&&key!="interval_seconds"&&key!="retention_days") {
                at=v.second;throw std::runtime_error("Unknown archive parameter: "+key);
            }
            auto parameter=[&](const std::string& key) {
                auto it=params.find(key);at=it==params.end()?archive.line:it->second.second;
                if(it==params.end())throw std::runtime_error("Missing archive parameter "+key+" in "+archive.name);
                return it->second.first;
            };
            archive.url=parameter("url");
            const std::string prefix="http://127.0.0.1:";
            if(!archive.url.starts_with(prefix))throw std::runtime_error("Archive URL must be local http://127.0.0.1:PORT");
            auto port=number(archive.url.substr(prefix.size()),1,65535);
            archive.url=prefix+std::to_string(port);
            if(!endpoints.insert(archive.url).second)throw std::runtime_error("Archives must have distinct endpoints; resolutions must not share a database");
            archive.interval_seconds=number(parameter("interval_seconds"),60,86400);
            if(archive.interval_seconds%60)throw std::runtime_error("Archive interval must be a multiple of 60 seconds, at most 86400");
            archive.retention_days=number(parameter("retention_days"),1,36500);
            c.archives.push_back(std::move(archive));
        }
        if(!c.archives.empty()) {
            const auto archive_count=c.archives.size();
            if(c.archive_state_max_bytes/(archive_count+1)<1048576||c.archive_queue_bytes/archive_count<65536||c.delivery.spool_bytes/archive_count<1024||c.delivery.spool_files<archive_count||c.delivery.queue_bytes/archive_count<c.delivery.batch_bytes||c.delivery.queue_records/archive_count<c.delivery.batch_records||c.archive_max_keys<archive_count||c.archive_outbox_rows<archive_count)throw std::runtime_error("Archive aggregate budgets are insufficient for configured archive count");
        }
        return c;
    }catch(const std::exception& e){throw std::runtime_error("Config line "+std::to_string(at)+": "+e.what());}
}
KnownLinks read_knownlinks(const AppConfig& c) {
    try{std::ifstream f(c.aggregation.knownlinks_path);if(!f)throw std::runtime_error("Cannot read knownlinks_file: "+c.aggregation.knownlinks_path);return KnownLinks(f);}
    catch(const std::exception& e){throw std::runtime_error("Config line "+std::to_string(c.knownlinks_line)+": "+e.what());}
}
} // namespace asstats
