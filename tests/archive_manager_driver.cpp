#include "asstats/archive_manager.hpp"
#include <fstream>
#include <iostream>
#include <ctime>
#include <thread>
#include <signal.h>
#include <atomic>
std::atomic<bool> stopped=false;
using namespace asstats;
int main(int argc,char** argv){try {
    if(argc!=3)return 2;std::ifstream input(argv[1]);auto app=read_app_config(input);
    bool serving=std::string(argv[2])=="serve";
    signal(SIGTERM,+[](int){stopped=true;});
    const auto start=serving?static_cast<uint64_t>(time(nullptr))/60*60:static_cast<uint64_t>(time(nullptr))/7200*7200-7200;
    ArchiveManager manager(app,argv[1],start);
    if(!serving&&std::string(argv[2])!="recover")for(uint64_t minute=start;minute<start+7200;minute+=60){
        std::vector<WindowRow> rows={{minute,minute+60,{"link-a",0,0,4},ByteCount(100),false},{minute,minute+60,{"link-b",65536,1,6},ByteCount(200),false}};
        if(!manager.submit(minute,false,rows))throw std::runtime_error("Unexpected manager queue rejection");
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    if(serving)for(unsigned i=0;i<450&&!stopped;++i)std::this_thread::sleep_for(std::chrono::milliseconds(100));
    else std::this_thread::sleep_for(std::chrono::seconds(2));
    bool ok=manager.finish();std::cout<<manager.diagnostics()<<'\n';return ok?0:1;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
