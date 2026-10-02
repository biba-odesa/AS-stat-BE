#include "asstats/config.hpp"
#include "asstats/decoder.hpp"
#include "asstats/queue.hpp"
#include "asstats/udp.hpp"
#include "fixtures.hpp"
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <thread>
using namespace asstats;
void queue_limits() {
    BoundedQueue q(2,BoundedQueue::descriptor_bytes(2)+10);
    DatagramInfo info;info.received_ns=123;
    require(q.push(info,Bytes(6,1)),"first push");
    require(!q.push(info,Bytes(5,2)),"byte bound independent of count");
    require(q.push(info,Bytes(4,3)),"fill byte bound exactly");
    require(!q.push(info,{}),"count bound even on empty payload");
    QueuedDatagram out;
    require(q.pop(out,std::chrono::milliseconds(0))&&out.size==6&&out.payload[5]==1&&out.info.received_ns==123,"own bytes and metadata");
    require(q.push(info,Bytes(6,4)),"wrap ring");
    require(q.pop(out,std::chrono::milliseconds(0))&&out.size==4&&out.payload[0]==3,"FIFO");
    require(q.pop(out,std::chrono::milliseconds(0))&&out.size==6&&out.payload[5]==4,"wrapped payload intact");
    q.push(info,Bytes(1,5));q.close();
    require(!q.push(info,Bytes(1,6))&&!q.drained(),"closed queue retains pending data");
    require(q.pop(out,std::chrono::milliseconds(0))&&q.drained(),"drain closed queue");
    const auto s=q.snapshot();require(s.drops==2&&s.closed_rejections==1&&s.peak_count==2&&s.peak_payload_bytes==10,"queue diagnostics");
    BoundedQueue concurrent(16,4096);
    std::thread producer([&]{for(uint32_t i=0;i<10000;++i){Bytes b;put(b,i,4);while(!concurrent.push(info,b)) std::this_thread::yield();}concurrent.close();});
    uint32_t expected=0;bool ordered=true;
    while(!concurrent.drained()) if(concurrent.pop(out,std::chrono::milliseconds(10))) {
        uint32_t n=0;for(size_t j=0;j<out.size;++j)n=(n<<8)|out.payload[j];
        ordered&=n==expected++;
    }
    producer.join();require(ordered&&expected==10000,"threaded FIFO ownership");
}
void ttl() {
    uint64_t now=0;
    Decoder d({},10,[&]{return now;});
    SourceMetadata meta{"fixture","192.0.2.1","192.0.2.2",10,20,999};
    const auto templ=nf(1,set(0,schema(900,{{16,4}})));
    const auto data=nf(1,set(900,{0,0,0,0}));
    auto send=[&](const Bytes& b){d.decode(b,meta,{},{});};
    send(templ);now=9;send(data);now=10;send(data);
    require(d.diagnostics().data_records==1&&d.diagnostics().template_expiries==1&&d.diagnostics().unknown_template_flowsets==1,"data use must not extend TTL");
    send(templ);now=19;auto bad=schema(900,{{17,4}});bad.pop_back();send(nf(1,set(0,bad)));
    now=20;d.expire_templates();require(d.cache_size()==0&&d.diagnostics().template_expiries==2,"bad refresh does not extend TTL");
    send(templ);now=29;send(templ);now=30;send(data);
    require(d.cache_size()==1&&d.diagnostics().data_records==2,"valid refresh extends TTL");
    auto reversed=data;reversed[7]=0;reversed[15]=0;send(reversed);
    require(d.cache_size()==1,"sequence/uptime decrease is not automatic restart");
    now=39;d.expire_templates();require(d.cache_size()==0,"idle expiry at exact deadline");
    now=50;send(nf(2,set(1,option_schema(901,{{1,0}},{{34,4}}))));
    now=59;send(nf(2,set(901,{0,0,0,100})));now=60;d.expire_templates();
    require(d.cache_size()==0&&d.diagnostics().options_records==1,"options data do not refresh TTL");
    Decoder off({},0,[&]{return now;});off.decode(templ,meta,{},{});now=UINT64_MAX;off.expire_templates();
    require(off.cache_size()==1,"TTL disabled for offline replay");
}
void sockets() {
    SourceConfig c;c.name="loop";c.protocol="netflow9";c.bind_ip="127.0.0.1";c.bind_address=inet_addr("127.0.0.1");c.exporters={c.bind_address};
    c.port=0;UdpSocket listener(c);
    auto requested=c;requested.socket_receive_buffer_bytes=1048576;
    UdpSocket buffered(requested);require(buffered.receive_buffer_bytes()>0,"configured kernel receive buffer");
    c.port=listener.port();
    bool busy=false;try{UdpSocket duplicate(c);}catch(const std::exception& e){busy=std::string(e.what()).find("bind")!=std::string::npos;}
    require(busy,"occupied endpoint must fail");
    int sender=socket(AF_INET,SOCK_DGRAM|SOCK_CLOEXEC,0);require(sender>=0,"sender socket");
    sockaddr_in target{};target.sin_family=AF_INET;target.sin_addr.s_addr=c.bind_address;target.sin_port=htons(c.port);
    Bytes bytes(100,7);
    require(sendto(sender,bytes.data(),bytes.size(),0,reinterpret_cast<sockaddr*>(&target),sizeof(target))==100,"send fixture");
    pollfd p{listener.fd(),POLLIN,0};require(poll(&p,1,1000)==1,"receive ready");
    std::array<uint8_t,8> small{};ReceivedDatagram r;
    require(listener.receive(small,r)&&r.truncated&&r.original_size==100,"MSG_TRUNC preserves original length");
    require(r.info.destination==c.bind_address&&r.info.destination_port==c.port&&r.info.source_port!=0,"socket addresses and ports");
    require(r.info.received_ns>0&&r.info.received_monotonic_ns>0,"two timestamp domains");
    require(!listener.timestamp_enabled()||r.kernel_timestamp,"kernel timestamp ancillary");
    c.port=0;UdpSocket fallback(c,false);target.sin_port=htons(fallback.port());
    require(sendto(sender,bytes.data(),8,0,reinterpret_cast<sockaddr*>(&target),sizeof(target))==8,"fallback fixture");
    p={fallback.fd(),POLLIN,0};require(poll(&p,1,1000)==1,"fallback ready");
    require(fallback.receive(small,r)&&!r.kernel_timestamp&&!r.truncated&&r.info.received_ns>0,"explicit timestamp fallback");
    ::close(sender);
}
int main(int argc,char**) {
    try {
        if(argc>1) {sockets();std::cout<<"PASS socket bind/truncation/timestamps\n";}
        else {queue_limits();ttl();std::cout<<"PASS queue/TTL/refresh\n";}
    } catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
