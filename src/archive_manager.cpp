#include "asstats/archive_manager.hpp"
#include "asstats/archive_journal.hpp"
#include "asstats/udp.hpp"
#include "archive_protocol.hpp"
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <spawn.h>
#include <poll.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <condition_variable>
#include <cstring>
#include <thread>
extern char** environ;
namespace asstats {
namespace {
void directory(const std::string& path) {
    struct stat st{};
    if(lstat(path.c_str(),&st)||!S_ISDIR(st.st_mode)||st.st_uid!=geteuid()||(st.st_mode&0022))throw std::runtime_error("Unsafe/missing receiver-owned archive directory: "+path);
}
struct Item {uint64_t minute{};bool partial{};std::string payload;};
class Channel {
    mutable std::mutex mutex_;std::condition_variable cv_;std::deque<Item> queue_;
    std::thread thread_;int fd_=-1;pid_t pid_=-1;uint64_t bytes_=0,peak_=0,lost_=0,committed_=0;
    uint64_t byte_limit_,count_limit_,timeout_,deadline_=0;bool closing_=false,failed_=false,dead_=false,in_flight_=false;
    std::string stats_="{}",name_;
    bool transfer(void* data,size_t size,bool write) {
        size_t offset=0;
        while(offset<size){
            {std::lock_guard lock(mutex_);if(closing_&&monotonic_ns()>=deadline_)return false;}
            pollfd p{fd_,static_cast<short>(write?POLLOUT:POLLIN),0};auto r=poll(&p,1,20);
            if(r<0&&errno!=EINTR)return false;
            if(r<=0)continue;
            if(!(p.revents&p.events))return false;
            auto n=write?send(fd_,static_cast<char*>(data)+offset,size-offset,MSG_NOSIGNAL|MSG_DONTWAIT):recv(fd_,static_cast<char*>(data)+offset,size-offset,MSG_DONTWAIT);
            if(n<0&&(errno==EAGAIN||errno==EINTR))continue;
            if(n<=0)return false;
            offset+=static_cast<size_t>(n);
        }
        return true;
    }
    void run() {
        try {
            for(;;){
                Item item;ArchiveRequest request;size_t charge=0;
                {std::unique_lock lock(mutex_);if(queue_.empty()&&!closing_)cv_.wait_for(lock,std::chrono::milliseconds(1000));
                 if(!queue_.empty()){item=std::move(queue_.front());queue_.pop_front();request={item.minute,item.payload.size(),item.partial,1};charge=item.payload.capacity()+sizeof(Item);in_flight_=true;}
                 else if(closing_)request.operation=2;}
                if(!transfer(&request,sizeof(request),true)||(!item.payload.empty()&&!transfer(item.payload.data(),item.payload.size(),true)))throw std::runtime_error("Archive IPC write failed");
                ArchiveReply reply;
                if(!transfer(&reply,sizeof(reply),false)||reply.size>archive_reply_limit)throw std::runtime_error("Archive IPC reply failed");
                std::string diagnostic(reply.size,'\0');if(!diagnostic.empty()&&!transfer(diagnostic.data(),diagnostic.size(),false))throw std::runtime_error("Archive IPC diagnostic failed");
                {std::lock_guard lock(mutex_);stats_=std::move(diagnostic);bytes_-=charge;in_flight_=false;if(request.operation==1){if(reply.ok)++committed_;else {++lost_;failed_=true;}}}
                if(request.operation==2)break;
            }
        }catch(...){std::lock_guard lock(mutex_);failed_=true;dead_=true;lost_+=queue_.size()+(in_flight_?1:0);queue_.clear();bytes_=0;}
        close(fd_);fd_=-1;
        int status=0;auto r=waitpid(pid_,&status,WNOHANG);
        for(unsigned i=0;i<40&&r==0;++i){std::this_thread::sleep_for(std::chrono::milliseconds(5));r=waitpid(pid_,&status,WNOHANG);}
        if(r==0){kill(pid_,SIGKILL);for(unsigned i=0;i<20&&r==0;++i){std::this_thread::sleep_for(std::chrono::milliseconds(5));r=waitpid(pid_,&status,WNOHANG);}}
        if(r<=0||!WIFEXITED(status)||WEXITSTATUS(status)){std::lock_guard lock(mutex_);failed_=true;}
    }
public:
    Channel(const AppConfig& app,const std::string& config,const ArchiveConfig& a,uint64_t first):byte_limit_(app.archive_queue_bytes/app.archives.size()),count_limit_(app.archive_queue_minutes),timeout_(app.delivery.shutdown_timeout_ms+2000),name_(a.name) {
        int sockets[2];if(socketpair(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0,sockets))throw std::runtime_error("Archive IPC socketpair failed");
        const auto executable=std::filesystem::read_symlink("/proc/self/exe").parent_path()/"nf9-archive-worker";
        std::vector<std::string> args={executable.string(),std::filesystem::absolute(config).string(),a.name,std::to_string(first)};std::vector<char*> argv;for(auto& arg:args)argv.push_back(arg.data());argv.push_back(nullptr);
        posix_spawn_file_actions_t actions;posix_spawn_file_actions_init(&actions);posix_spawn_file_actions_adddup2(&actions,sockets[1],3);posix_spawn_file_actions_addclosefrom_np(&actions,4);
        posix_spawn_file_actions_addopen(&actions,0,"/dev/null",O_RDONLY,0);posix_spawn_file_actions_addopen(&actions,1,"/dev/null",O_WRONLY,0);posix_spawn_file_actions_addopen(&actions,2,"/dev/null",O_WRONLY,0);
        auto rc=posix_spawn(&pid_,executable.c_str(),&actions,nullptr,argv.data(),environ);posix_spawn_file_actions_destroy(&actions);close(sockets[1]);fd_=sockets[0];
        if(rc){close(fd_);fd_=-1;throw std::runtime_error("Cannot spawn archive worker");}
        // A failed state fingerprint/lock must fail receiver startup before accepting traffic.
        closing_=true;deadline_=monotonic_ns()+6000000000ULL;
        auto abort=[&]{kill(pid_,SIGKILL);for(unsigned n=0;n<20&&waitpid(pid_,nullptr,WNOHANG)==0;++n)std::this_thread::sleep_for(std::chrono::milliseconds(5));close(fd_);fd_=-1;};
        ArchiveReply reply;
        if(!transfer(&reply,sizeof(reply),false)||reply.size>archive_reply_limit){abort();throw std::runtime_error("Archive startup handshake failed: "+a.name);}
        std::string body(reply.size,'\0');if(reply.size&&!transfer(body.data(),body.size(),false))reply.ok=0;
        if(!reply.ok){abort();throw std::runtime_error("Archive worker startup failed: "+a.name+": "+body);}
        closing_=false;deadline_=0;stats_=std::move(body);thread_=std::thread([this]{run();});
    }
    ~Channel(){stop();join();}
    void reject(){std::lock_guard lock(mutex_);++lost_;failed_=true;}
    bool submit(uint64_t minute,bool partial,const std::string& payload) {
        const auto charge=payload.size()+sizeof(Item)+1;
        std::lock_guard lock(mutex_);
        if(closing_||dead_||queue_.size()+(in_flight_?1:0)>=count_limit_||charge>byte_limit_-bytes_){++lost_;failed_=true;return false;}
        Item item{minute,partial,payload};const auto actual=item.payload.capacity()+sizeof(Item);
        if(actual>byte_limit_-bytes_){++lost_;failed_=true;return false;}
        bytes_+=actual;peak_=std::max(peak_,bytes_);queue_.push_back(std::move(item));cv_.notify_one();return true;
    }
    void stop(){std::lock_guard lock(mutex_);if(!closing_){closing_=true;deadline_=monotonic_ns()+timeout_*1000000;cv_.notify_one();}}
    bool join(){if(thread_.joinable())thread_.join();std::lock_guard lock(mutex_);return !failed_;}
    std::string diagnostics()const{std::lock_guard lock(mutex_);return "{\"queue_bytes\":"+std::to_string(bytes_)+",\"peak_queue_bytes\":"+std::to_string(peak_)+",\"queue_minutes\":"+std::to_string(queue_.size())+",\"lost_minutes\":"+std::to_string(lost_)+",\"persisted_minutes\":"+std::to_string(committed_)+",\"failed\":"+(failed_?"true":"false")+",\"worker\":"+stats_+"}";}
};
}
struct ArchiveManager::Impl {int lock=-1;size_t payload_limit=0;std::vector<std::pair<std::string,std::unique_ptr<Channel>>> channels;~Impl(){channels.clear();if(lock>=0)close(lock);}};
ArchiveManager::ArchiveManager(const AppConfig& app,const std::string& config,uint64_t first):impl_(std::make_unique<Impl>()) {
    impl_->payload_limit=app.archive_queue_bytes/app.archives.size()-sizeof(Item)-1;
    directory(app.delivery.directory);directory(app.archive_state_directory);
    validate_archive_registry(app.archive_state_directory,app.archives);
    impl_->lock=open((app.delivery.directory+"/.lock").c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW,0600);
    if(impl_->lock<0||flock(impl_->lock,LOCK_EX|LOCK_NB))throw std::runtime_error("Legacy/root spool is locked by another receiver/sender");
    for(const auto& entry:std::filesystem::directory_iterator(app.delivery.directory)) {
        if(entry.path().filename()==".lock")continue;
        if(!entry.is_directory()||entry.is_symlink())throw std::runtime_error("Legacy root spool contains pending/unfinished files; drain them to the original minute endpoint first");
        directory(entry.path().string());
    }
    for(const auto& a:app.archives){
        const auto spool=app.delivery.directory+"/"+a.name;
        if(!std::filesystem::exists(spool)){if(mkdir(spool.c_str(),0750))throw std::runtime_error("Cannot create archive spool directory");}
        directory(spool);directory(app.archive_state_directory+"/"+a.name);
        const int root=open(app.delivery.directory.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);
        if(root<0)throw std::runtime_error("Cannot open spool parent for fsync");
        const int result=fsync(root);close(root);if(result)throw std::runtime_error("Cannot fsync spool parent");
        impl_->channels.emplace_back(a.name,std::make_unique<Channel>(app,config,a,first));
    }
}
ArchiveManager::~ArchiveManager(){finish();}
bool ArchiveManager::submit(uint64_t minute,bool partial,const std::vector<WindowRow>& rows){std::string payload;
    try {payload=archive_payload(rows,impl_->payload_limit);}catch(const std::length_error&){for(auto& [name,c]:impl_->channels){(void)name;c->reject();}return false;}
    bool ok=true;for(auto& [name,channel]:impl_->channels){(void)name;if(!channel->submit(minute,partial,payload))ok=false;}return ok;}
bool ArchiveManager::finish(){for(auto& [name,c]:impl_->channels){(void)name;c->stop();}bool ok=true;for(auto& [name,c]:impl_->channels){(void)name;if(!c->join())ok=false;}return ok;}
std::string ArchiveManager::diagnostics()const{std::string out="{";for(const auto& [name,c]:impl_->channels){if(out.size()>1)out+=',';out+='"'+name+'"'+':'+c->diagnostics();}return out+'}';}
}
