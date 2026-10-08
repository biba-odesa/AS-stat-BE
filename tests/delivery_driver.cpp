#include "asstats/delivery.hpp"
#include <sys/stat.h>
#include <iostream>
#include <ctime>
#include <thread>
using namespace asstats;
int main(int argc,char** argv){try {
    if(argc!=4)return 2;
    std::string mode=argv[3];DeliveryConfig c;c.directory=argv[1];c.url=argv[2];
    c.http_timeout_ms=100;c.retry_initial_ms=30;c.retry_max_ms=100;c.shutdown_timeout_ms=500;
    c.batch_records=2;c.batch_bytes=1024;
    if(mode=="overflow")c.spool_bytes=1024;
    if(mode=="files")c.spool_files=1;
    if(mode=="queue")c.queue_records=1;
    if(mode=="extended_retention")c.retention_seconds=31*86400;
    if(mode=="short_retention")c.retention_seconds=86400;
    if(mode=="durable"||mode=="durable_offline")c.interval_seconds=300;
    bool publication_called=false,publication_ok=false;
    Delivery d(c);
    if(mode=="hold"){std::cout<<"ready\n"<<std::flush;std::string line;std::getline(std::cin,line);}
    if(mode=="disk")chmod(c.directory.c_str(),0500);
    if(mode!="recover"&&mode!="hold") {
        uint64_t now=static_cast<uint64_t>(time(nullptr));uint64_t start=now/60*60-120;
        if(mode=="expired"||mode=="extended_retention")start-=8*86400;
        if(mode=="short_retention")start-=2*86400;
        if(mode=="durable"||mode=="durable_offline") {
            start=start/300*300;std::vector<WindowRow> rows;
            for(uint32_t i=0;i<6;++i)rows.push_back({start,start+300,{"stable",i,0,4},ByteCount(100+i),false});
            if(!d.submit_durable(rows,[&](bool ok){publication_called=true;publication_ok=ok;}))throw std::runtime_error("Durable enqueue rejected");
        }else for(uint32_t i=0;i<6;++i){WindowRow r{start,start+60,{"stable",i,static_cast<uint8_t>(i%2),static_cast<uint8_t>(i%2?6:4)},ByteCount(100+i),false};d.submit(r);}
        d.submit(WindowRow{start,start+60,{"stable",999,0,4},ByteCount(999),true});
    }
    if(mode=="wait")std::this_thread::sleep_for(std::chrono::milliseconds(700));
    bool ok=d.finish();
    if((mode=="durable"||mode=="durable_offline")&&(!publication_called||!publication_ok))throw std::runtime_error("Missing durable publication callback");if(mode=="disk")chmod(c.directory.c_str(),0750);
    std::cout<<d.diagnostics()<<'\n';return ok?0:1;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
