#include "asstats/pcap.hpp"
#include "fixtures.hpp"
#include <iostream>
#include <sstream>
using namespace asstats;
Bytes ethernet(bool ipv6=false, bool vlan=false) {
    Bytes b(12,0); put(b,vlan?0x8100:ipv6?0x86dd:0x0800,2);
    if (vlan) { put(b,1,2); put(b,ipv6?0x86dd:0x0800,2); }
    if (ipv6) {
        put(b,0x60000000,4); put(b,12,2); put(b,17,1); put(b,64,1);
        Bytes addr(16,0); addr[0]=0x20;addr[1]=1;addr[15]=1;
        b=join(b,addr);addr[15]=2;b=join(b,addr);
    } else {
        put(b,0x4500,2);put(b,32,2);put(b,0,4);put(b,64,1);put(b,17,1);put(b,0,2);
        put(b,0xc0000201,4);put(b,0xc0000202,4);
    }
    put(b,1000,2);put(b,2057,2);put(b,12,2);put(b,0,2);
    return join(b,{0,9,0,1});
}
Bytes pcap(const Bytes& frame,bool little=true,bool nano=false,uint32_t original=0) {
    Bytes b;
    const auto val=[&](uint64_t n,unsigned width) {
        if (!little) put(b,n,width);
        else for(unsigned i=0;i<width;++i) b.push_back(static_cast<uint8_t>(n>>(8*i)));
    };
    val(nano?0xa1b23c4d:0xa1b2c3d4,4);val(2,2);val(4,2);val(0,4);val(0,4);val(262144,4);val(1,4);
    val(123,4);val(456,4);val(frame.size(),4);val(original?original:frame.size(),4);
    return join(b,frame);
}
struct Result { PcapDiagnostics stats; std::vector<SourceMetadata> sources; std::vector<Bytes> payloads; };
Result read(const Bytes& b,uint16_t port=2057) {
    std::istringstream in(std::string(b.begin(),b.end()));
    PcapReader reader; Result r;
    reader.read(in,"test",port,[&](auto p,const auto& m){r.sources.push_back(m);r.payloads.emplace_back(p.begin(),p.end());});
    r.stats=reader.diagnostics();return r;
}
int main() {
    try {
        for (bool little:{false,true}) for(bool nano:{false,true}) for(bool ipv6:{false,true}) {
            auto r=read(pcap(ethernet(ipv6,true),little,nano));
            require(r.stats.udp_datagrams==1 && r.payloads[0]==Bytes({0,9,0,1}),"payload and VLAN");
            require(r.sources[0].received_ns==123000000000ULL+456*(nano?1:1000),"exact PCAP timestamp");
            require(r.sources[0].exporter_ip==(ipv6?"2001::1":"192.0.2.1"),"exporter address");
        }
        require(read(pcap(ethernet()),9999).stats.filtered_datagrams==1,"port filter");
        auto frame=ethernet();
        require(read(pcap(frame,true,false,100)).stats.truncated_packets==1,"snaplen truncation");
        frame[14]=0x44;
        require(read(pcap(frame)).stats.malformed_packets==1,"invalid IHL");
        frame=ethernet();frame[20]=0x20;
        require(read(pcap(frame)).stats.fragments==1,"IPv4 fragment skipped");
        frame=ethernet();frame[38]=0;frame[39]=99;
        require(read(pcap(frame)).stats.malformed_packets==1,"UDP boundary");
        frame=ethernet(true);frame[20]=44;
        require(read(pcap(frame)).stats.fragments==1,"IPv6 fragment skipped");
        frame=ethernet(true);frame[20]=0;frame[19]=20;
        frame.insert(frame.begin()+54,{17,0,0,0,0,0,0,0});
        require(read(pcap(frame)).stats.udp_datagrams==1,"IPv6 extension header");
        const auto complete=pcap(ethernet());
        for(size_t n=0;n<complete.size();++n) {
            if(n==24) continue; // A header-only capture is valid.
            bool failed=false;
            try { read(Bytes(complete.begin(),complete.begin()+static_cast<std::ptrdiff_t>(n))); }
            catch(const std::runtime_error&) {failed=true;}
            require(failed,"truncated file must fail");
        }
        auto oversized=complete; oversized[32]=0xff;oversized[33]=0xff;oversized[34]=0xff;oversized[35]=0x7f;
        bool failed=false;
        try {read(oversized);} catch(const std::runtime_error&) {failed=true;}
        require(failed,"oversized allocation rejected");
        std::cout << "PASS PCAP endian/resolution/VLAN/IP/filter/fragments/extension/bounds/truncation\n";
    } catch(const std::exception& e) {std::cerr << "FAIL: " << e.what() << '\n';return 1;}
}
