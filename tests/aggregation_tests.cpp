#include "asstats/aggregation.hpp"
#include "asstats/async_output.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <iostream>
using namespace asstats;
void check(bool b){if(!b)throw std::runtime_error("assertion failed");}
template<class F>void throws(F f){bool caught=false;try{f();}catch(const std::exception&){caught=true;}check(caught);}
constexpr uint64_t ns=1000000000ULL;
KnownLinks links(std::string s="192.0.2.1 1 stable First ff0000 ignored\n192.0.2.1 2 other Second 0000ff 100\n") {std::istringstream in(s);return KnownLinks(in);}
AggregationConfig config(){AggregationConfig c;c.sampling["test"]={true,100};c.excluded_asns={{64496,64496},{64512,65534},{4200000000U,4294967294U}};return c;}
FlowRecord record(){FlowRecord r;r.identity.source={"test","192.0.2.1","",0,0,61*ns};r.ip_version=4;r.bytes=10;r.input_ifindex=1;r.output_ifindex=2;r.source_asn=0;r.destination_asn=123;return r;}
int main(int argc,char**) {try {
    if(argc>1){
        int fds[2];check(pipe(fds)==0);
        AsyncOutput blocked(fds[1],1,200000,false);check(blocked.submit(std::string(100000,'x')));check(!blocked.submit("overflow"));
        auto start=std::chrono::steady_clock::now();check(!blocked.finish(std::chrono::milliseconds(100)));
        check(std::chrono::steady_clock::now()-start<std::chrono::seconds(2));check(blocked.stats().timed_out);close(fds[0]);close(fds[1]);
        int fd=open("/dev/full",O_WRONLY);check(fd>=0);AsyncOutput fail(fd,4,4096,false);check(fail.submit("failure\n"));check(!fail.finish());check(fail.stats().errors);close(fd);
        int nullfd=open("/dev/null",O_WRONLY);AsyncOutput memory(nullfd,10,100,false);check(!memory.submit(std::string(200,'x')));check(!memory.finish());close(nullfd);
        std::cout<<"PASS bounded blocked output, queue/memory limits, write failure\n";return 0;
    }
    throws([]{auto x=ByteCount::maximum();x.add(ByteCount(1));});throws([]{ByteCount::maximum().multiplied(2);});
    check(ByteCount(UINT64_MAX).multiplied(100).str()=="1844674407370955161500");
    throws([]{links("192.0.2.1 1 a A ff0000 1\n192.0.2.1 1 b B ff0000 1\n");});
    auto c=config();std::vector<WindowRow> rows;MinuteAggregator a(c,links(),60*ns,[&](auto r){rows.push_back(r);return true;});
    auto r=record();a.add(r,61*ns);r.output_ifindex.reset();a.add(r,62*ns);r.input_ifindex=0;r.output_ifindex=2;a.add(r,62*ns);
    r.input_ifindex=999;a.add(r,62*ns);r.input_ifindex=1;r.source_asn.reset();a.add(r,62*ns);
    for(auto asn:{64496U,64512U,65534U,4200000000U,4294967294U}){r.source_asn=asn;a.add(r,62*ns);}
    r.source_asn=4294967295U;r.ip_version=6;a.add(r,62*ns);
    r.bytes.reset();a.add(r,62*ns);r.bytes=10;r.ip_version.reset();a.add(r,62*ns);
    a.tick(124*ns);check(rows.empty());a.tick(125*ns);check(rows.size()==4);check(!rows[0].partial);check(rows[0].bytes.str()=="9000");
    r=record();a.add(r,126*ns);check(a.stats().late_records==1&&a.stats().late_side_bytes.str()=="2000");
    check(a.stats().missing_asn==1&&a.stats().zero_ifindex==1&&a.stats().unknown_ifindex==1&&a.stats().excluded_asn==5);
    check(a.stats().missing_bytes==1&&a.stats().invalid_family==1);
    rows.clear();c.sampling["test"]={false,100};MinuteAggregator b(c,links(),61*ns,[&](auto row){rows.push_back(row);return true;});
    r=record();b.add(r,124*ns);r.identity.source.received_ns=120*ns;b.add(r,124*ns);b.tick(125*ns);check(rows.size()==2&&rows[0].partial&&rows[0].bytes.str()=="10");
    b.finish(150*ns);check(rows.size()==4&&rows[2].partial);
    OptionsRecord op;op.identity.source.configured_source="test";op.identity.source_id=42;op.values={{34,100},{50,200}};op.scopes={{1,{}}};b.options(op);check(b.stats().options_checked==2&&b.stats().options_mismatch==1);
    c=config();c.sampling["test"].rate=UINT64_MAX;
    MinuteAggregator overflow(c,links(),60*ns,[](auto){return true;});
    r=record();r.bytes=UINT64_MAX;r.output_ifindex=0;overflow.add(r,61*ns);
    throws([&]{overflow.add(r,61*ns);});check(overflow.stats().overflow_errors==1&&overflow.stats().failed);
    c=config();c.max_active_keys=1;MinuteAggregator limited(c,links(),60*ns,[](auto){return true;});limited.add(record(),61*ns);check(limited.stats().failed&&limited.stats().key_limit_sides==1);
    c=config();c.max_active_windows=1;MinuteAggregator windows(c,links(),60*ns,[](auto){return true;});windows.add(record(),61*ns);r=record();r.identity.source.received_ns=120*ns;windows.add(r,121*ns);check(windows.stats().window_limit_sides==2);
    MinuteAggregator rejected(config(),links(),60*ns,[](auto){return false;});rejected.add(record(),61*ns);rejected.finish(120*ns);check(rejected.stats().output_rejections==2&&rejected.stats().failed);
    rows.clear();MinuteAggregator changed(config(),links("198.51.100.1 99 stable Renamed abcdef 999\n"),60*ns,[&](auto row){rows.push_back(row);return true;});
    r=record();r.identity.source.exporter_ip="198.51.100.1";r.input_ifindex=99;changed.add(r,61*ns);changed.finish(120*ns);check(rows.size()==1&&rows[0].key.link_id=="stable"&&rows[0].bytes.str()=="1000");
    rows.clear();c=config();c.sampling["second"]={true,125};
    MinuteAggregator combined(c,links("192.0.2.1 1 stable Same ff0000 0\n192.0.2.2 9 stable Same ff0000 999\n"),60*ns,[&](auto row){rows.push_back(row);return true;});
    r=record();r.output_ifindex=0;combined.add(r,61*ns);
    r.identity.source.configured_source="second";r.identity.source.exporter_ip="192.0.2.2";r.identity.source_id=99;r.input_ifindex=9;
    combined.add(r,61*ns);combined.finish(120*ns);
    check(rows.size()==1&&rows[0].bytes.str()=="2250"&&!rows[0].partial&&rows[0].key.link_id=="stable");
    std::cout<<"PASS sides, ASN policy/zero, missing fields, families, exact scale/overflow, windows/late/idle/partial, limits, stable identity\n";
    return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
