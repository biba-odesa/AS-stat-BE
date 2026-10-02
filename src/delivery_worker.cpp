#include "asstats/delivery.hpp"
#include "asstats/udp.hpp"
#include "delivery_protocol.hpp"
#include <curl/curl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <stdexcept>
namespace asstats {
namespace {
uint64_t checksum(const std::string& body) {
    uint64_t hash=14695981039346656037ULL;
    for(unsigned char c:body){hash^=c;hash*=1099511628211ULL;}return hash;
}
struct Batch {std::string name;uint64_t size{},rows{},first{},last{},due{},attempts{};bool blocked=false;};
class Spool {
    int dir_=-1,lock_=-1;
    DeliveryConfig c_;
    DeliveryReply stats_;
    std::deque<Batch> batches_;
    uint64_t sequence_=0;
    CURL* curl_=nullptr;
    bool disk_uncertain_=false;
    void space() {
        struct statvfs s{};if(fstatvfs(dir_,&s))throw std::runtime_error("Cannot inspect spool free space");
        const UInt128 free=UInt128(s.f_bavail)*s.f_frsize;
        stats_.free_bytes=static_cast<uint64_t>(std::min(free,UInt128(UINT64_MAX)));
    }
    void update() {
        stats_.pending_batches=batches_.size();stats_.oldest_timestamp=0;
        for(const auto& b:batches_)if(b.first&&(!stats_.oldest_timestamp||b.first<stats_.oldest_timestamp))stats_.oldest_timestamp=b.first;
        stats_.peak_batches=std::max(stats_.peak_batches,stats_.pending_batches);
        stats_.peak_bytes=std::max(stats_.peak_bytes,stats_.pending_bytes);
    }
    std::string read(Batch& b) {
        if(b.size>c_.batch_bytes+512||b.size<10)throw std::runtime_error("Invalid spool size");
        int fd=openat(dir_,b.name.c_str(),O_RDONLY|O_NOFOLLOW|O_CLOEXEC);
        if(fd<0)throw std::runtime_error("Cannot read spool batch");
        std::string data(static_cast<size_t>(b.size),'\0');size_t off=0;
        while(off<data.size()){auto n=::read(fd,data.data()+off,data.size()-off);if(n<=0){close(fd);throw std::runtime_error("Short spool read");}off+=static_cast<size_t>(n);}
        close(fd);const auto nl=data.find('\n');if(nl==std::string::npos||nl>256)throw std::runtime_error("Missing spool header");
        std::istringstream head(data.substr(0,nl));std::string magic,extra;uint64_t size=0,hash=0;
        if(!(head>>magic>>b.rows>>b.first>>b.last>>size>>hash)||head>>extra||magic!="ASSTAT1"||!b.rows||b.rows>4096||b.first%60||b.last%60||b.last<b.first||size!=data.size()-nl-1)
            throw std::runtime_error("Invalid spool header");
        auto body=data.substr(nl+1);
        if(checksum(body)!=hash||body.empty()||body.back()!='\n'||static_cast<uint64_t>(std::count(body.begin(),body.end(),'\n'))!=b.rows)throw std::runtime_error("Corrupt spool payload");
        return body;
    }
public:
    explicit Spool(DeliveryConfig c):c_(std::move(c)) {
        const auto resolved=std::filesystem::canonical(c_.directory).string();
        if(resolved=="/var/lib/victoriametrics"||resolved.starts_with("/var/lib/victoriametrics/"))throw std::runtime_error("VM storage cannot be spool");
        dir_=open(c_.directory.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
        struct stat ds{};
        if(dir_<0||fstat(dir_,&ds)||ds.st_uid!=getuid()||(ds.st_mode&0022))throw std::runtime_error("Spool must be owned by receiver and not group/world writable");
        lock_=openat(dir_,".lock",O_RDWR|O_CREAT|O_NOFOLLOW|O_CLOEXEC,0600);
        if(lock_<0||flock(lock_,LOCK_EX|LOCK_NB))throw std::runtime_error("Spool is locked by another sender or lock is inaccessible");
        DIR* listing=fdopendir(dup(dir_));if(!listing)throw std::runtime_error("Cannot enumerate spool");
        try {
            while(auto* e=readdir(listing)) {
                std::string name=e->d_name;if(name=="."||name==".."||name==".lock")continue;
                if(batches_.size()>=c_.spool_files)throw std::runtime_error("Existing spool exceeds file limit; preserve and inspect it");
                struct stat st{};if(fstatat(dir_,name.c_str(),&st,AT_SYMLINK_NOFOLLOW)||!S_ISREG(st.st_mode)||st.st_uid!=getuid()||st.st_nlink!=1)throw std::runtime_error("Unexpected spool entry");
                Batch b;b.name=name;b.size=static_cast<uint64_t>(st.st_size);
                if(b.size>UINT64_MAX-stats_.pending_bytes)throw std::runtime_error("Spool size overflow");
                stats_.pending_bytes+=b.size;
                if(name.ends_with(".ready")) {
                    try {read(b);}catch(...){b.blocked=true;++stats_.corrupt_files;}
                }else {b.blocked=true;if(name.ends_with(".tmp"))++stats_.unfinished_files;else ++stats_.corrupt_files;}
                batches_.push_back(std::move(b));
            }
            closedir(listing);
        }catch(...){closedir(listing);throw;}
        std::sort(batches_.begin(),batches_.end(),[](const auto& a,const auto& b){return a.name<b.name;});
        space();update();
        if(curl_global_init(CURL_GLOBAL_DEFAULT)!=CURLE_OK)throw std::runtime_error("curl initialization failed");
        curl_=curl_easy_init();if(!curl_)throw std::runtime_error("curl allocation failed");
    }
    ~Spool(){if(curl_)curl_easy_cleanup(curl_);if(lock_>=0)close(lock_);if(dir_>=0)close(dir_);}
    DeliveryReply stats(){update();return stats_;}
    void save(const DeliveryRequest& r,const std::string& body) {
        if(!r.rows)return;
        const std::string header="ASSTAT1 "+std::to_string(r.rows)+" "+std::to_string(r.first)+" "+std::to_string(r.last)+" "+std::to_string(body.size())+" "+std::to_string(checksum(body))+"\n";
        const auto size=header.size()+body.size();
        try {space();}catch(...){++stats_.disk_errors;stats_.lost_rows+=r.rows;return;}
        if(disk_uncertain_||batches_.size()>=c_.spool_files||stats_.pending_bytes>c_.spool_bytes||size>c_.spool_bytes-stats_.pending_bytes||size+65536>stats_.free_bytes) {
            ++stats_.overflow_batches;stats_.lost_rows+=r.rows;return;
        }
        Batch batch;batch.name=std::to_string(realtime_ns())+"-"+std::to_string(getpid())+"-"+std::to_string(++sequence_)+".tmp";
        batch.rows=r.rows;batch.first=r.first;batch.last=r.last;
        int fd=openat(dir_,batch.name.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        if(fd<0){++stats_.disk_errors;stats_.lost_rows+=r.rows;return;}
        const std::string data=header+body;size_t off=0;bool good=true;
        while(off<data.size()){auto n=write(fd,data.data()+off,data.size()-off);if(n<0&&errno==EINTR)continue;if(n<=0){good=false;break;}off+=static_cast<size_t>(n);}
        batch.size=off;
        if(good&&fsync(fd))good=false;
        if(close(fd))good=false;
        if(good) {
            const auto ready=batch.name.substr(0,batch.name.size()-4)+".ready";
            if(renameat2(dir_,batch.name.c_str(),dir_,ready.c_str(),RENAME_NOREPLACE))good=false;
            else {batch.name=ready;if(fsync(dir_)){good=false;disk_uncertain_=true;}}
        }
        // Never remove an unacknowledged file, including partial writes.
        if(!good){++stats_.disk_errors;stats_.lost_rows+=r.rows;batch.blocked=true;++stats_.unfinished_files;}
        else {++stats_.saved_batches;stats_.saved_rows+=r.rows;}
        stats_.pending_bytes+=batch.size;batches_.push_back(std::move(batch));update();
    }
    void send_one() {
        const auto now=monotonic_ns()/1000000,wall=realtime_ns()/1000000000;
        for(size_t i=0,n=batches_.size();i<n;++i) {
            Batch b=std::move(batches_.front());batches_.pop_front();
            if(!b.blocked&&wall>604800&&b.first<wall-604800){b.blocked=true;++stats_.expired_batches;}
            if(b.blocked||b.due>now){batches_.push_back(std::move(b));continue;}
            std::string body;
            try {body=read(b);}catch(...){b.blocked=true;++stats_.corrupt_files;batches_.push_back(std::move(b));continue;}
            if(b.attempts++)++stats_.retries;
            curl_easy_reset(curl_);
            const auto url=c_.url+"/api/v1/import";
            curl_easy_setopt(curl_,CURLOPT_URL,url.c_str());
            curl_easy_setopt(curl_,CURLOPT_PROXY,"");
            curl_easy_setopt(curl_,CURLOPT_NOPROXY,"*");
            curl_easy_setopt(curl_,CURLOPT_NOSIGNAL,1L);
            curl_easy_setopt(curl_,CURLOPT_TIMEOUT_MS,static_cast<long>(c_.http_timeout_ms));
            curl_easy_setopt(curl_,CURLOPT_CONNECTTIMEOUT_MS,static_cast<long>(c_.http_timeout_ms));
            curl_easy_setopt(curl_,CURLOPT_FOLLOWLOCATION,0L);
            curl_easy_setopt(curl_,CURLOPT_POST,1L);
            curl_easy_setopt(curl_,CURLOPT_POSTFIELDS,body.data());
            curl_easy_setopt(curl_,CURLOPT_POSTFIELDSIZE_LARGE,static_cast<curl_off_t>(body.size()));
            curl_easy_setopt(curl_,CURLOPT_WRITEFUNCTION,+[](char*,size_t a,size_t z,void*)->size_t{return a*z;});
            curl_slist* headers=nullptr;headers=curl_slist_append(headers,"Content-Type: application/json");headers=curl_slist_append(headers,"Expect:");
            curl_easy_setopt(curl_,CURLOPT_HTTPHEADER,headers);
            const auto result=curl_easy_perform(curl_);long status=0;curl_easy_getinfo(curl_,CURLINFO_RESPONSE_CODE,&status);curl_slist_free_all(headers);
            if(result==CURLE_OK&&status>=200&&status<300) {
                // API acknowledgement is not a claim about VM crash durability.
                if(unlinkat(dir_,b.name.c_str(),0)||fsync(dir_)) {
                    ++stats_.disk_errors;disk_uncertain_=true;b.blocked=true;batches_.push_back(std::move(b));
                }else {stats_.pending_bytes-=b.size;++stats_.sent_batches;stats_.sent_rows+=b.rows;}
            }else {
                ++stats_.http_errors;
                if(result==CURLE_OK&&status>=400&&status<500&&status!=408&&status!=429) {b.blocked=true;++stats_.permanent_errors;}
                else {
                    const auto shift=std::min<uint64_t>(b.attempts-1,20);
                    b.due=monotonic_ns()/1000000+std::min(c_.retry_max_ms,c_.retry_initial_ms*(uint64_t(1)<<shift));
                }
                batches_.push_back(std::move(b));
            }
            update();return;
        }
        update();
    }
};
uint64_t integer(const char* v){uint64_t n=0;auto len=strlen(v);auto [p,e]=std::from_chars(v,v+len,n);if(e!=std::errc{}||p!=v+len||!n)throw std::runtime_error("Invalid worker argument");return n;}
}
int delivery_worker(int argc,char** argv) {
    try {
        if(argc!=9)throw std::runtime_error("Internal delivery worker arguments required");
        if(prctl(PR_SET_PDEATHSIG,SIGKILL)||getppid()==1)throw std::runtime_error("Delivery parent absent");
        DeliveryConfig c;c.directory=argv[1];c.url=argv[2];c.spool_bytes=integer(argv[3]);c.spool_files=integer(argv[4]);
        c.batch_bytes=integer(argv[5]);c.http_timeout_ms=integer(argv[6]);c.retry_initial_ms=integer(argv[7]);c.retry_max_ms=integer(argv[8]);
        if(c.batch_bytes>delivery_payload_limit||c.spool_files>65536)throw std::runtime_error("Worker limits invalid");
        Spool spool(c);auto reply=spool.stats();if(send(3,&reply,sizeof(reply),MSG_NOSIGNAL)!=sizeof(reply))return 1;
        std::array<char,delivery_payload_limit+sizeof(DeliveryRequest)> packet{};
        for(;;) {
            auto n=recv(3,packet.data(),packet.size(),MSG_TRUNC);if(n<=0)return 0;
            if(n<static_cast<ssize_t>(sizeof(DeliveryRequest))||n>static_cast<ssize_t>(packet.size()))throw std::runtime_error("Invalid delivery packet");
            DeliveryRequest r;memcpy(&r,packet.data(),sizeof(r));
            if(r.size!=static_cast<size_t>(n)-sizeof(r)||r.size>c.batch_bytes||r.rows>4096||r.first%60||r.last%60||r.last<r.first)throw std::runtime_error("Invalid delivery request");
            spool.save(r,std::string(packet.data()+sizeof(r),r.size));spool.send_one();reply=spool.stats();
            if(send(3,&reply,sizeof(reply),MSG_NOSIGNAL)!=sizeof(reply))return 0;
            if(r.stop&&!reply.pending_batches)return 0;
        }
    }catch(const std::exception& e){DeliveryReply r;strncpy(r.error,e.what(),sizeof(r.error)-1);send(3,&r,sizeof(r),MSG_NOSIGNAL);return 1;}
}
}
int main(int argc,char** argv){return asstats::delivery_worker(argc,argv);}
