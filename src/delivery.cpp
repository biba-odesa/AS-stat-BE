#include "asstats/delivery.hpp"
#include "asstats/udp.hpp"
#include "delivery_protocol.hpp"
#include <sys/socket.h>
#include <sys/wait.h>
#include <spawn.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>
#include <array>
#include <cstring>
#include <filesystem>
#include <stdexcept>
extern char** environ;
namespace asstats {
std::string import_json(const WindowRow& r) {
    if(r.start%60||r.end<=r.start||(r.end-r.start)%60||(r.end-r.start)>86400||r.start>UINT64_MAX/1000||
       (r.key.ip_version!=4&&r.key.ip_version!=6)||r.key.direction>1||r.key.link_id.empty()||
       r.key.link_id.size()>64||r.key.link_id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")!=std::string::npos)
        throw std::runtime_error("Invalid delivery interval row");
    return "{\"metric\":{\"__name__\":\"asstat_traffic_bytes\",\"link_id\":\""+r.key.link_id+
        "\",\"asn\":\""+std::to_string(r.key.asn)+"\",\"direction\":\""+(r.key.direction?"out":"in")+
        "\",\"ip_version\":\""+std::to_string(r.key.ip_version)+"\"},\"values\":["+r.bytes.str()+
        "],\"timestamps\":["+std::to_string(r.start*1000)+"]}\n";
}
Delivery::Delivery(DeliveryConfig c):config_(std::move(c)) {
    int pair[2];if(socketpair(AF_UNIX,SOCK_SEQPACKET|SOCK_CLOEXEC,0,pair))throw std::runtime_error("delivery socketpair failed");
    int size=1024*1024;setsockopt(pair[0],SOL_SOCKET,SO_SNDBUF,&size,sizeof(size));setsockopt(pair[1],SOL_SOCKET,SO_SNDBUF,&size,sizeof(size));
    const auto exe=std::filesystem::read_symlink("/proc/self/exe").parent_path()/"nf9-delivery-worker";
    std::vector<std::string> args={exe.string(),config_.directory,config_.url,std::to_string(config_.spool_bytes),
        std::to_string(config_.spool_files),std::to_string(config_.batch_bytes),std::to_string(config_.http_timeout_ms),
        std::to_string(config_.retry_initial_ms),std::to_string(config_.retry_max_ms),std::to_string(config_.retention_seconds)};
    std::string children;for(const auto& name:config_.permitted_child_directories){if(!children.empty())children+=',';children+=name;}args.push_back(children);
    std::vector<char*> argv;for(auto& a:args)argv.push_back(a.data());argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions,pair[1],3);
    posix_spawn_file_actions_addclosefrom_np(&actions,4);
    posix_spawn_file_actions_addopen(&actions,0,"/dev/null",0,0);
    posix_spawn_file_actions_addopen(&actions,1,"/dev/null",1,0);
    posix_spawn_file_actions_addopen(&actions,2,"/dev/null",1,0);
    pid_t child;int rc=posix_spawn(&child,exe.c_str(),&actions,nullptr,argv.data(),environ);
    posix_spawn_file_actions_destroy(&actions);close(pair[1]);
    if(rc){close(pair[0]);throw std::runtime_error("Cannot spawn delivery worker: "+std::string(strerror(rc)));}
    fd_=pair[0];pid_=child;
    pollfd p{fd_,POLLIN,0};DeliveryReply reply;
    if(poll(&p,1,5000)<=0||recv(fd_,&reply,sizeof(reply),MSG_TRUNC)!=sizeof(reply)||reply.error[0]) {
        kill(pid_,SIGKILL);
        for(int i=0;i<20&&waitpid(pid_,nullptr,WNOHANG)==0;++i)std::this_thread::sleep_for(std::chrono::milliseconds(5));
        close(fd_);fd_=-1;pid_=-1;
        throw std::runtime_error(std::string("Delivery worker startup failed: ")+reply.error);
    }
    worker_stats_=delivery_reply_json(reply);
    pump_=std::thread([this]{run();});
}
Delivery::~Delivery(){finish();}
bool Delivery::submit(const WindowRow& r) {
    if(r.partial){std::lock_guard lock(mutex_);++partial_;return true;}
    auto text=import_json(r);const auto charge=text.capacity()+sizeof(Item)+64;
    std::lock_guard lock(mutex_);
    if(closing_||done_||queue_.size()+inflight_>=config_.queue_records||charge>config_.queue_bytes-bytes_) {
        ++lost_;failed_=true;return false;
    }
    bytes_+=charge;queue_.push_back({std::move(text),r.start,{}});++accepted_;
    peak_bytes_=std::max(peak_bytes_,bytes_);peak_records_=std::max<uint64_t>(peak_records_,queue_.size()+inflight_);cv_.notify_one();return true;
}
bool Delivery::submit_durable(const std::vector<WindowRow>& rows,std::function<void(bool)> completion) {
    if(rows.empty()||!completion)throw std::invalid_argument("Empty durable delivery batch/callback");
    std::vector<Item> items;size_t charge=0;
    auto receipt=std::make_shared<Receipt>();receipt->remaining=rows.size();receipt->completion=std::move(completion);
    for(const auto& r:rows) {
        if(r.partial||r.end-r.start!=config_.interval_seconds||r.start%config_.interval_seconds)throw std::runtime_error("Invalid durable archive interval");
        auto text=import_json(r);
        if(text.size()>config_.batch_bytes)throw std::runtime_error("Archive row exceeds HTTP batch size");
        charge+=text.capacity()+sizeof(Item)+64;items.push_back({std::move(text),r.start,receipt});
    }
    std::lock_guard lock(mutex_);
    if(closing_||done_||rows.size()>config_.queue_records-std::min<uint64_t>(config_.queue_records,queue_.size()+inflight_)||charge>config_.queue_bytes-bytes_)return false;
    bytes_+=charge;for(auto& item:items)queue_.push_back(std::move(item));accepted_+=rows.size();
    peak_bytes_=std::max(peak_bytes_,bytes_);peak_records_=std::max<uint64_t>(peak_records_,queue_.size()+inflight_);cv_.notify_one();return true;
}
void Delivery::run() {
    uint64_t inflight=0,saved_rows=0;bool deadline_hit=false;
    std::vector<std::shared_ptr<Receipt>> receipts;
    auto complete=[&](bool success) {
        for(auto& r:receipts)if(r) {
            r->failed|=!success;
            if(--r->remaining==0) {
                try {r->completion(!r->failed);}
                catch(...) {std::lock_guard lock(mutex_);failed_=true;}
            }
        }
        receipts.clear();
    };
    try {
        for(;;) {
            DeliveryRequest request;std::string body;size_t charge=0;
            {
                std::unique_lock lock(mutex_);
                if(queue_.empty())cv_.wait_for(lock,std::chrono::milliseconds(100));
                else if(!closing_&&queue_.size()<config_.batch_records)
                    cv_.wait_for(lock,std::chrono::milliseconds(50),[&]{return closing_||queue_.size()>=config_.batch_records;});
                while(!queue_.empty()&&request.rows<config_.batch_records) {
                    auto& item=queue_.front();
                    if(body.size()+item.text.size()>config_.batch_bytes)break;
                    if(!request.rows)request.first=item.timestamp;
                    request.first=std::min(request.first,item.timestamp);request.last=std::max(request.last,item.timestamp);
                    body+=item.text;charge+=item.text.capacity()+sizeof(Item)+64;receipts.push_back(item.receipt);queue_.pop_front();++request.rows;
                }
                if(!queue_.empty()&&!request.rows)throw std::runtime_error("delivery row exceeds batch limit");
                request.stop=closing_&&queue_.empty();
                inflight=request.rows;inflight_=inflight;
            }
            request.size=static_cast<uint32_t>(body.size());
            std::string packet(sizeof(request),'\0');memcpy(packet.data(),&request,sizeof(request));packet+=body;
            auto wait=[&](short event) {
                for(;;) {
                    {std::lock_guard lock(mutex_);if(closing_&&monotonic_ns()>=deadline_){deadline_hit=true;return false;}}
                    pollfd p{fd_,event,0};int rc=poll(&p,1,20);
                    if(rc<0&&errno!=EINTR)return false;
                    if(rc>0)return bool(p.revents&event);
                }
            };
            if(!wait(POLLOUT)||send(fd_,packet.data(),packet.size(),MSG_NOSIGNAL|MSG_DONTWAIT)!=static_cast<ssize_t>(packet.size()))throw std::runtime_error("delivery IPC send failed");
            DeliveryReply reply;
            if(!wait(POLLIN)||recv(fd_,&reply,sizeof(reply),MSG_TRUNC)!=sizeof(reply))throw std::runtime_error("delivery IPC reply failed");
            {
                std::lock_guard lock(mutex_);bytes_-=charge;inflight=0;inflight_=0;worker_stats_=delivery_reply_json(reply);
                if(reply.error[0]||reply.disk_errors||reply.lost_rows||reply.corrupt_files||reply.unfinished_files||reply.permanent_errors||reply.expired_batches)failed_=true;
            }
            const bool durable=reply.saved_rows>=saved_rows&&reply.saved_rows-saved_rows==request.rows;
            saved_rows=reply.saved_rows;complete(durable);
            if(reply.error[0])throw std::runtime_error("delivery worker failed");
            if(request.stop&&reply.pending_batches==0)break;
            // Pending permanent/expired batches remain on disk; shutdown is bounded by the parent deadline.
        }
    }catch(...){std::lock_guard lock(mutex_);if(inflight||!queue_.empty()||!closing_||!deadline_hit)failed_=true;unconfirmed_+=inflight;inflight_=0;lost_+=queue_.size();for(auto& item:queue_)receipts.push_back(item.receipt);queue_.clear();bytes_=0;}
    complete(false);
    close(fd_);fd_=-1;
    // Never wait indefinitely for a child blocked inside disk I/O.
    int status=0;pid_t result=waitpid(pid_,&status,WNOHANG);
    if(result==0){kill(pid_,SIGKILL);for(int i=0;i<20&&result==0;++i){std::this_thread::sleep_for(std::chrono::milliseconds(5));result=waitpid(pid_,&status,WNOHANG);}}
    {std::lock_guard lock(mutex_);if(result<=0)failed_=true;done_=true;}
}
bool Delivery::finish() {
    {std::lock_guard lock(mutex_);if(!closing_){closing_=true;deadline_=monotonic_ns()+config_.shutdown_timeout_ms*1000000;cv_.notify_one();}}
    if(pump_.joinable())pump_.join();
    std::lock_guard lock(mutex_);return !failed_;
}
std::string Delivery::diagnostics() const {
    std::lock_guard lock(mutex_);
    return "{\"accepted_rows\":"+std::to_string(accepted_)+",\"partial_rows_skipped\":"+std::to_string(partial_)+
        ",\"lost_before_spool_rows\":"+std::to_string(lost_)+",\"unconfirmed_rows\":"+std::to_string(unconfirmed_)+
        ",\"queue_records\":"+std::to_string(queue_.size()+inflight_)+",\"queue_bytes\":"+std::to_string(bytes_)+
        ",\"peak_queue_records\":"+std::to_string(peak_records_)+",\"peak_queue_bytes\":"+std::to_string(peak_bytes_)+
        ",\"failed\":"+(failed_?"true":"false")+",\"spool\":"+worker_stats_+"}";
}
}
