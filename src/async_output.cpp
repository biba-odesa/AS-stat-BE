#include "asstats/async_output.hpp"
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <sstream>
#include <stdexcept>
namespace asstats {
namespace {
constexpr size_t max_message=256*1024;
// After fork, use only syscall-level operations: other threads may own libc/C++ locks.
[[noreturn]] void child_loop(int ipc,int target,bool sync) {
    const int a=fcntl(ipc,F_DUPFD_CLOEXEC,10),b=fcntl(target,F_DUPFD_CLOEXEC,10);
    if(a<0||b<0||dup2(a,3)<0||dup2(b,4)<0) _exit(2);
    if(syscall(SYS_close_range,5,~0U,0)!=0) _exit(2);
    close(0);close(1);close(2);
    char buffer[max_message];
    for(;;) {
        ssize_t n=recv(3,buffer,sizeof(buffer),0);
        if(n<0&&errno==EINTR) continue;
        if(n<0) _exit(2);
        char ack=1;
        if(n==0) {
            if(sync&&fsync(4)!=0) ack=0;
            if(close(4)!=0) ack=0;
            send(3,&ack,1,MSG_NOSIGNAL);_exit(ack?0:2);
        }
        size_t pos=0;
        while(pos<static_cast<size_t>(n)) {
            ssize_t k=write(4,buffer+pos,static_cast<size_t>(n)-pos);
            if(k<0&&errno==EINTR) continue;
            if(k<=0) {ack=0;break;}
            pos+=static_cast<size_t>(k);
        }
        if(send(3,&ack,1,MSG_NOSIGNAL)!=1||!ack) _exit(2);
    }
}
}
AsyncOutput::AsyncOutput(int fd,size_t rows,size_t bytes,bool sync):max_messages_(rows),max_bytes_(bytes) {start(fd,sync);}
AsyncOutput::AsyncOutput(const std::string& path,size_t rows,size_t bytes):max_messages_(rows),max_bytes_(bytes) {
    int fd=open(path.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
    if(fd<0) throw std::runtime_error("Cannot create new output "+path+": "+strerror(errno));
    try {
        struct stat s{};
        if(fstat(fd,&s)!=0||!S_ISREG(s.st_mode)) throw std::runtime_error("Output must be a new regular file");
        start(fd,true);
    } catch(...) {close(fd);throw;}
    close(fd);
}
void AsyncOutput::start(int fd,bool sync) {
    if(!max_messages_||!max_bytes_) throw std::invalid_argument("Zero output queue limit");
    int pair[2];if(socketpair(AF_UNIX,SOCK_SEQPACKET|SOCK_CLOEXEC,0,pair)!=0) throw std::runtime_error("output socketpair failed");
    int capacity=1024*1024;setsockopt(pair[0],SOL_SOCKET,SO_SNDBUF,&capacity,sizeof(capacity));
    child_=fork();
    if(child_<0) {close(pair[0]);close(pair[1]);throw std::runtime_error("output fork failed");}
    if(child_==0) child_loop(pair[1],fd,sync);
    close(pair[1]);socket_=pair[0];
    try {worker_=std::thread([this]{pump();});}
    catch(...) {close(socket_);kill(child_,SIGKILL);waitpid(child_,nullptr,0);throw;}
}
AsyncOutput::~AsyncOutput() {finish(std::chrono::milliseconds(200));}
bool AsyncOutput::submit(std::string message) {
    std::lock_guard lock(mutex_);
    const size_t bytes=charge(message);
    if(message.empty()||message.size()>max_message||closing_||stats_.errors||
       queue_.size()>=max_messages_||bytes>max_bytes_-stats_.queued_bytes) {++stats_.rejected;return false;}
    stats_.queued_bytes+=bytes;queue_.push_back(std::move(message));++stats_.accepted;
    stats_.queued=queue_.size();stats_.peak_queued=std::max(stats_.peak_queued,stats_.queued);
    stats_.peak_bytes=std::max(stats_.peak_bytes,stats_.queued_bytes);
    ready_.notify_one();return true;
}
bool AsyncOutput::expired() {
    std::lock_guard lock(mutex_);
    if(closing_&&std::chrono::steady_clock::now()>=deadline_) {stats_.timed_out=true;return true;}
    return false;
}
bool AsyncOutput::exchange(const std::string& message) {
    bool sent=false;
    while(!expired()) {
        pollfd p{socket_,static_cast<short>(sent?POLLIN:POLLOUT),0};
        int result=poll(&p,1,25);
        if(result<0&&errno==EINTR) continue;
        if(result<0) return false;
        if(!result) continue;
        if(!sent && (p.revents&POLLOUT)) {
            auto n=send(socket_,message.data(),message.size(),MSG_DONTWAIT|MSG_NOSIGNAL);
            if(n<0&&(errno==EAGAIN||errno==EINTR)) continue;
            if(n!=static_cast<ssize_t>(message.size())) return false;
            sent=true;
        } else if(sent&&(p.revents&POLLIN)) {
            char ack=0;return recv(socket_,&ack,1,MSG_DONTWAIT)==1&&ack==1;
        } else if(p.revents&(POLLERR|POLLHUP|POLLNVAL)) return false;
    }
    return false;
}
void AsyncOutput::pump() {
    bool ok=true;
    for(;;) {
        std::unique_lock lock(mutex_);
        ready_.wait_for(lock,std::chrono::milliseconds(25),[&]{return closing_||!queue_.empty();});
        if(queue_.empty()) {
            if(!closing_) continue;
            lock.unlock();ok=exchange("");break;
        }
        // Only this thread removes entries; push_back does not invalidate deque references.
        const auto& message=queue_.front();lock.unlock();
        if(!exchange(message)) {ok=false;break;}
        lock.lock();++stats_.written;stats_.written_bytes+=message.size();
        stats_.queued_bytes-=charge(message);queue_.pop_front();stats_.queued=queue_.size();
    }
    if(!ok) {std::lock_guard lock(mutex_);++stats_.errors;}
    close(socket_);socket_=-1;
    if(!ok) kill(child_,SIGKILL);
    const auto until=std::chrono::steady_clock::now()+std::chrono::milliseconds(100);
    int status{};
    while(waitpid(child_,&status,WNOHANG)==0) {
        if(std::chrono::steady_clock::now()>=until) {kill(child_,SIGKILL);std::lock_guard lock(mutex_);stats_.unreaped=true;break;}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::lock_guard lock(mutex_);
    if(!stats_.unreaped && (!WIFEXITED(status)||WEXITSTATUS(status)!=0) && !stats_.errors) ++stats_.errors;
}
bool AsyncOutput::finish(std::chrono::milliseconds timeout) {
    {
        std::lock_guard lock(mutex_);
        if(finished_) return !stats_.errors&&!stats_.rejected&&!stats_.queued&&!stats_.unreaped;
        closing_=true;deadline_=std::chrono::steady_clock::now()+timeout;ready_.notify_all();
    }
    if(worker_.joinable()) worker_.join();
    std::lock_guard lock(mutex_);finished_=true;
    return !stats_.errors&&!stats_.rejected&&!stats_.queued&&!stats_.unreaped;
}
OutputStats AsyncOutput::stats() const {std::lock_guard lock(mutex_);return stats_;}
std::string output_stats_json(const OutputStats& s) {
    std::ostringstream o;o<<"{\"accepted\":"<<s.accepted<<",\"written\":"<<s.written<<",\"written_bytes\":"<<s.written_bytes
       <<",\"rejected\":"<<s.rejected<<",\"errors\":"<<s.errors<<",\"queued\":"<<s.queued<<",\"queued_bytes\":"<<s.queued_bytes
       <<",\"peak_queued\":"<<s.peak_queued<<",\"peak_bytes\":"<<s.peak_bytes<<",\"timed_out\":"<<(s.timed_out?"true":"false")
       <<",\"unreaped\":"<<(s.unreaped?"true":"false")<<'}';return o.str();
}
} // namespace asstats
