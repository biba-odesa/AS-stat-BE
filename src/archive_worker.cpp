#include "asstats/archive_journal.hpp"
#include "asstats/udp.hpp"
#include "archive_protocol.hpp"
#include <curl/curl.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <future>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <sstream>
using namespace asstats;
namespace {
std::string quoted_error(const std::string& text) {
    std::string out="\"";for(unsigned char c:text.substr(0,512)){if(c=='"'||c=='\\')out+='\\';if(c>=32&&c<127)out+=static_cast<char>(c);else out+=' ';}return out+'"';
}
bool transfer(void* data,size_t size,bool write){size_t offset=0;while(offset<size){auto n=write?send(3,static_cast<char*>(data)+offset,size-offset,MSG_NOSIGNAL):recv(3,static_cast<char*>(data)+offset,size-offset,0);if(n<0&&errno==EINTR)continue;if(n<=0)return false;offset+=static_cast<size_t>(n);}return true;}
bool reply(bool ok,std::string body){ArchiveReply r{body.size(),ok?1U:0U,0};return transfer(&r,sizeof(r),true)&&transfer(body.data(),body.size(),true);}
std::string retention(const ArchiveConfig& a,uint64_t timeout) {
    CURL* curl=curl_easy_init();if(!curl)return "unavailable";std::string flags;
    curl_easy_setopt(curl,CURLOPT_URL,(a.url+"/flags").c_str());curl_easy_setopt(curl,CURLOPT_PROXY,"");curl_easy_setopt(curl,CURLOPT_NOPROXY,"*");curl_easy_setopt(curl,CURLOPT_NOSIGNAL,1L);curl_easy_setopt(curl,CURLOPT_TIMEOUT_MS,static_cast<long>(timeout));
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,+[](char* data,size_t n,size_t size,void* output)->size_t{auto* s=static_cast<std::string*>(output);if((n&&size>65536/n)||n*size>65536-s->size())return 0;s->append(data,n*size);return n*size;});curl_easy_setopt(curl,CURLOPT_WRITEDATA,&flags);
    const auto result=curl_easy_perform(curl);long status=0;curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&status);curl_easy_cleanup(curl);
    if(result!=CURLE_OK||status!=200)return "unavailable";
    auto start=flags.find("retentionPeriod=");if(start==std::string::npos)return "unrecognized";
    start+=16;auto end=flags.find('\n',start);auto value=flags.substr(start,end-start);value.erase(std::remove(value.begin(),value.end(),'"'),value.end());
    while(!value.empty()&&(value.back()=='\r'||value.back()==' '))value.pop_back();
    try {size_t used=0;auto days=std::stod(value,&used);if(used<value.size()&&value.substr(used)=="d"){}else if(used==value.size())days*=31;else return "unrecognized";
        return days<static_cast<double>(a.retention_days)?"TOO_SHORT":"ok";
    }catch(...){return "unrecognized";}
}
}
int main(int argc,char** argv) {
    try {
        if(argc==3&&std::string(argv[1])=="--drain-legacy") {
            std::ifstream config(argv[2]);auto app=read_app_config(config);
            for(const auto& a:app.archives)app.delivery.permitted_child_directories.push_back(a.name);
            Delivery d(app.delivery);bool ok=d.finish();auto diagnostic=d.diagnostics();std::cout<<diagnostic<<'\n';return ok&&diagnostic.find("\"pending_batches\":0,")!=std::string::npos?0:1;
        }
        bool bootstrap=argc==5&&std::string(argv[1])=="--bootstrap";
        bool initialize=argc==5&&std::string(argv[1])=="--init-current";bool abandon=argc==5&&std::string(argv[1])=="--abandon-history";bool admin=bootstrap||initialize||abandon;
        if(!admin&&(prctl(PR_SET_PDEATHSIG,SIGKILL)||getppid()==1))throw std::runtime_error("Archive parent absent");
        if((!admin&&argc!=4))throw std::runtime_error("Internal worker or --bootstrap CONFIG ARCHIVE FIRST_MINUTE required");
        const auto config_path=argv[admin?2:1],name=argv[admin?3:2];
        const auto first=std::stoull(argv[admin?4:3]);
        std::ifstream file(config_path);if(!file)throw std::runtime_error("Cannot read archive config");auto app=read_app_config(file);
        auto found=std::find_if(app.archives.begin(),app.archives.end(),[&](const auto& a){return a.name==name;});if(found==app.archives.end())throw std::runtime_error("Unknown archive");
        auto a=*found;const auto count=app.archives.size();
        if(admin)validate_archive_registry(app.archive_state_directory,app.archives);
        ArchiveJournalLimits limits;limits.database_bytes=app.archive_state_max_bytes/(count+1);limits.minute_keys=app.aggregation.max_active_keys;limits.minute_bytes=app.archive_queue_bytes/count;limits.active_keys=app.archive_max_keys/count;limits.outbox_rows=app.archive_outbox_rows/count;limits.receipt_minutes=4;
        const auto state_file=app.archive_state_directory+"/"+a.name+"/archives.sqlite";
        if(!admin&&!std::filesystem::exists(state_file))throw std::runtime_error("Archive requires explicit --bootstrap or --init-current initialization");
        if(admin&&!abandon&&std::filesystem::exists(state_file))throw std::runtime_error("Existing archive state is preserved; initialization cannot reset it");
        if(abandon&&!std::filesystem::exists(state_file))throw std::runtime_error("Abandonment requires existing state");
        ArchiveJournal journal(app.archive_state_directory+"/"+a.name,{a},first,limits);
        if(admin){if(abandon)journal.abandon_history(first);else if(bootstrap)journal.prepare_bootstrap();else journal.set_history_boundary(first);std::cout<<journal.diagnostics()<<'\n';return 0;}
        journal.activate_live(first);
        DeliveryConfig d=app.delivery;d.directory+="/"+a.name;d.url=a.url;d.interval_seconds=a.interval_seconds;d.retention_seconds=a.retention_days*86400;
        d.spool_bytes/=count;d.spool_files/=count;d.queue_bytes/=count;d.queue_records/=count;
        std::mutex completion_mutex;std::optional<std::pair<ArchiveBatch,bool>> completion;bool inflight=false;
        Delivery delivery(d);
        // Flags discovery cannot delay readiness or make an unavailable VM prevent collection.
        auto flags=std::async(std::launch::async,[&]{return retention(a,d.http_timeout_ms);});std::string retention_status="checking";
        uint64_t disk_errors=0,stage_failures=0,handoff_retries=0,next_handoff=0,process_retry=0;
        uint64_t handoff_delay=d.retry_initial_ms,process_delay=d.retry_initial_ms;
        std::string last_state_error,last_stage_error;int last_state_code=0,last_stage_code=0;uint64_t last_failed_minute=0;
        auto diagnostic=[&]{return "{\"state\":"+journal.diagnostics()+",\"delivery\":"+delivery.diagnostics()+",\"retention_status\":\""+retention_status+"\",\"state_processing_errors\":"+std::to_string(disk_errors)+",\"stage_failures\":"+std::to_string(stage_failures)+",\"handoff_retries\":"+std::to_string(handoff_retries)+",\"last_state_error\":"+quoted_error(last_state_error)+",\"last_state_sqlite_code\":"+std::to_string(last_state_code)+",\"last_stage_error\":"+quoted_error(last_stage_error)+",\"last_stage_sqlite_code\":"+std::to_string(last_stage_code)+",\"last_failed_minute\":"+std::to_string(last_failed_minute)+"}";};
        if(!reply(true,diagnostic()))return 1;
        for(;;){
            if(flags.valid()&&flags.wait_for(std::chrono::milliseconds(0))==std::future_status::ready)retention_status=flags.get();
            {std::optional<std::pair<ArchiveBatch,bool>> done;{std::lock_guard lock(completion_mutex);done=std::move(completion);completion.reset();}
             if(done){
                if(done->second){journal.acknowledge(done->first);handoff_delay=d.retry_initial_ms;next_handoff=0;}
                else {++handoff_retries;next_handoff=monotonic_ns()/1000000+handoff_delay;handoff_delay=std::min(d.retry_max_ms,handoff_delay*2);}
                inflight=false;
             }}
            bool processed=false;
            if(monotonic_ns()/1000000>=process_retry) {
                try {processed=journal.process_one();if(processed)process_delay=d.retry_initial_ms;}
                catch(const std::exception& e){last_state_error=e.what();auto* storage=dynamic_cast<const ArchiveStorageError*>(&e);last_state_code=storage?storage->sqlite_code():0;++disk_errors;process_retry=monotonic_ns()/1000000+process_delay;process_delay=std::min(d.retry_max_ms,process_delay*2);}
            }
            if(!inflight&&retention_status!="TOO_SHORT"&&monotonic_ns()/1000000>=next_handoff) {
                auto batch=journal.pending(a.name,d.batch_records);
                if(batch){auto saved=std::make_shared<ArchiveBatch>(std::move(*batch));
                    if(delivery.submit_durable(saved->rows,[&,saved](bool ok){std::lock_guard lock(completion_mutex);completion=std::make_pair(*saved,ok);})){inflight=true;}else next_handoff=monotonic_ns()/1000000+d.retry_initial_ms;}
            }
            pollfd p{3,POLLIN,0};if(poll(&p,1,processed?0:50)<0&&errno!=EINTR)throw std::runtime_error("Archive poll failed");
            if(p.revents&(POLLHUP|POLLERR))break;
            if(!(p.revents&POLLIN))continue;
            ArchiveRequest request;if(!transfer(&request,sizeof(request),false))break;
            if(request.size>app.archive_queue_bytes/count||request.partial>1||request.operation>2)throw std::runtime_error("Invalid archive request");
            std::string payload(request.size,'\0');if(!transfer(payload.data(),payload.size(),false))break;
            bool ok=true;
            if(request.operation==1){try {journal.stage_live(request.minute,request.partial,payload);}catch(const std::exception& e){last_stage_error=e.what();last_failed_minute=request.minute;auto* storage=dynamic_cast<const ArchiveStorageError*>(&e);last_stage_code=storage?storage->sqlite_code():0;++stage_failures;ok=false;}}
            if(request.operation==2){delivery.finish();{std::lock_guard lock(completion_mutex);if(completion&&completion->second)journal.acknowledge(completion->first);completion.reset();}reply(ok,diagnostic());return 0;}
            if(!reply(ok,diagnostic()))return 0;
        }
        delivery.finish();return 0;
    }catch(const std::exception& e){std::string message=e.what();if(argc==5||argc==3){std::cerr<<message<<'\n';return 1;}std::string escaped;for(char c:message.substr(0,1024)){if(c=='"'||c=='\\')escaped+='\\';if(c>=' ')escaped+=c;}
        reply(false,"{\"fatal\":true,\"reason\":\""+escaped+"\"}");return 1;}
}
