#include "asstats/app_config.hpp"
#include "asstats/aggregation.hpp"
#include "asstats/async_output.hpp"
#include "asstats/decoder.hpp"
#include "asstats/queue.hpp"
#include "asstats/udp.hpp"
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <charconv>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace {
using namespace asstats;
constexpr uint64_t second=1000000000ULL;
#define RECEIVE_COUNTERS(X) X(datagrams) X(udp_bytes) X(unexpected_exporters) X(truncated) \
    X(control_truncated) X(kernel_timestamps) X(timestamp_fallbacks) X(destination_fallbacks) \
    X(socket_drops) X(socket_drop_samples) X(queue_drops) X(enqueued) X(receive_errors)
struct ReceiveStats {
#define FIELD(n) std::atomic<uint64_t> n{};
    RECEIVE_COUNTERS(FIELD)
#undef FIELD
};
struct InterfaceStats {uint64_t input_missing{},input_zero{},output_missing{},output_zero{};};
struct DecodeSnapshot {
    Diagnostics all, warm;
    std::array<InterfaceStats,3> interfaces{}, warm_interfaces{};
    size_t cache_entries{};
};
struct Source {
    SourceConfig config;
    UdpSocket socket;
    BoundedQueue queue;
    Decoder decoder;
    ReceiveStats received, warm_received;
    std::atomic<uint64_t> last_received_ns{}, last_allowed_received_ns{};
    MinuteAggregator* aggregator=nullptr;
    uint32_t last_socket_drop_counter{};
    std::mutex decoded_mutex;
    DecodeSnapshot decoded;
    std::thread worker;
    std::atomic<bool> failed=false;
    std::string failure;
    explicit Source(SourceConfig c) : config(std::move(c)),socket(config),
        queue(config.queue_datagrams,config.queue_memory_bytes),
        decoder({},uint64_t(config.template_ttl_seconds)*second,monotonic_ns) {}
    ~Source() {queue.close();if(worker.joinable()) worker.join();}
    void start(uint64_t warm_boundary) {
        worker=std::thread([this,warm_boundary] {
            try {
                QueuedDatagram data;
                while(true) {
                    const bool have=queue.pop(data,std::chrono::milliseconds(100));
                    {
                        std::lock_guard lock(decoded_mutex);
                        const auto before=decoder.diagnostics();
                        const bool warm=(have?data.info.received_monotonic_ns:monotonic_ns())>=warm_boundary;
                        if(have) {
                            SourceMetadata metadata{config.name,ipv4_text(data.info.exporter),ipv4_text(data.info.destination),
                                data.info.source_port,data.info.destination_port,data.info.received_ns};
                            decoder.decode(std::span(data.payload).first(data.size),metadata,[&](const FlowRecord& r) {
                                const size_t family=r.ip_version==4?1:r.ip_version==6?2:0;
                                auto count=[&](InterfaceStats& s) {
                                    if(!r.input_ifindex) ++s.input_missing;else if(*r.input_ifindex==0) ++s.input_zero;
                                    if(!r.output_ifindex) ++s.output_missing;else if(*r.output_ifindex==0) ++s.output_zero;
                                };
                                count(decoded.interfaces[family]);
                                if(warm) count(decoded.warm_interfaces[family]);
                                if(aggregator) aggregator->add(r,realtime_ns());
                            },[&](const OptionsRecord& r){if(aggregator)aggregator->options(r);});
                        } else decoder.expire_templates();
                        decoded.all=decoder.diagnostics();decoded.cache_entries=decoder.cache_size();
                        if(warm) add_difference(decoded.warm,decoded.all,before);
                    }
                    if(!have && queue.drained()) break;
                }
            } catch(const std::exception& e) {
                std::lock_guard lock(decoded_mutex);failure=e.what();failed=true;queue.close();
            }
        });
    }
    DecodeSnapshot snapshot() {std::lock_guard lock(decoded_mutex);return decoded;}
    void receive_one(const ReceivedDatagram& d,std::span<const uint8_t> payload,uint64_t boundary) {
        ReceiveStats increment;
        increment.datagrams=1;increment.udp_bytes=d.original_size;
        increment.truncated=d.truncated;increment.control_truncated=d.control_truncated;
        increment.kernel_timestamps=d.kernel_timestamp;increment.timestamp_fallbacks=!d.kernel_timestamp;
        increment.destination_fallbacks=d.destination_fallback;
        if(d.socket_drop_counter) {
            // Linux reports a cumulative uint32 counter; unsigned subtraction handles one rollover.
            increment.socket_drops=static_cast<uint32_t>(*d.socket_drop_counter-last_socket_drop_counter);
            increment.socket_drop_samples=1;last_socket_drop_counter=*d.socket_drop_counter;
        }
        last_received_ns=d.info.received_ns;
        if(!config.allows(d.info.exporter)) increment.unexpected_exporters=1;
        else {
            last_allowed_received_ns=d.info.received_ns;
            if(!d.truncated) {
                if(queue.push(d.info,payload)) increment.enqueued=1;
                else increment.queue_drops=1;
            }
        }
#define ADD(n) received.n+=increment.n; if(d.info.received_monotonic_ns>=boundary) warm_received.n+=increment.n;
        RECEIVE_COUNTERS(ADD)
#undef ADD
    }
};
// Keep the aggregator alive until every worker has stopped, including exceptional startup.
struct WorkerLifetime {
    std::vector<std::unique_ptr<Source>>& sources;
    ~WorkerLifetime() {
        for(auto& s:sources) {s->socket.close();s->queue.close();}
        for(auto& s:sources) if(s->worker.joinable()) s->worker.join();
    }
};
std::string quoted(const std::string& s) {
    std::string out="\"";
    for(unsigned char c:s) {
        if(c=='"'||c=='\\') {out+='\\';out+=static_cast<char>(c);}
        else if(c<32) out+='?';
        else out+=static_cast<char>(c);
    }
    return out+'"';
}
void receive_json(std::ostream& out,const ReceiveStats& s) {
    out<<'{';
#define OUTPUT(n) out<<"\"" #n "\":"<<s.n<<',';
    RECEIVE_COUNTERS(OUTPUT)
#undef OUTPUT
    out<<"\"udp_bytes_exclude_headers\":true}";
}
void decode_json(std::ostream& out,const Diagnostics& s) {
    out<<'{';
#define OUTPUT(n) out<<"\"" #n "\":"<<s.n<<',';
    ASSTATS_DECODER_COUNTERS(OUTPUT)
#undef OUTPUT
    out<<"\"sampling_applied\":false}";
}
void interfaces_json(std::ostream& out,const std::array<InterfaceStats,3>& groups) {
    out<<'{';
    for(size_t i=0;i<3;++i) {
        if(i) out<<',';
        out<<quoted(i==0?"unknown":i==1?"4":"6")<<":{";
        const auto& s=groups[i];
        out<<"\"input_missing\":"<<s.input_missing<<",\"input_zero\":"<<s.input_zero
           <<",\"output_missing\":"<<s.output_missing<<",\"output_zero\":"<<s.output_zero<<'}';
    }
    out<<'}';
}
std::string report(const std::vector<std::unique_ptr<Source>>& sources,uint64_t start,uint64_t reception_end,
                   uint64_t warm_seconds,const std::string& reason,bool final,
                   MinuteAggregator* aggregator=nullptr, AsyncOutput* writer=nullptr, AsyncOutput* diagnostic=nullptr, Delivery* delivery=nullptr) {
    const uint64_t now=monotonic_ns();
    const double elapsed=double((reception_end?reception_end:now)-start)/double(second);
    const double warm_elapsed=std::max(0.0,elapsed-double(warm_seconds));
    rusage usage{};if(getrusage(RUSAGE_SELF,&usage)!=0) throw std::runtime_error("getrusage failed");
    const double cpu=double(usage.ru_utime.tv_sec+usage.ru_stime.tv_sec)+double(usage.ru_utime.tv_usec+usage.ru_stime.tv_usec)/1e6;
    rusage children{};getrusage(RUSAGE_CHILDREN,&children);
    const double child_cpu=double(children.ru_utime.tv_sec+children.ru_stime.tv_sec)+double(children.ru_utime.tv_usec+children.ru_stime.tv_usec)/1e6;
    std::ostringstream out;out.precision(12);
    out<<"{\"final\":"<<(final?"true":"false")<<",\"reason\":"<<quoted(reason)
       <<",\"uid\":"<<getuid()<<",\"reception_seconds\":"<<elapsed
       <<",\"warmup_seconds\":"<<warm_seconds<<",\"post_warmup_seconds\":"<<warm_elapsed
       <<",\"drain_seconds\":"<<(reception_end?double(now-reception_end)/double(second):0)
       <<",\"cpu_seconds\":"<<cpu<<",\"cpu_percent_one_core\":"<<(elapsed>0?cpu/elapsed*100:0)
       <<",\"reaped_helpers_cpu_seconds\":"<<child_cpu<<",\"reaped_helpers_peak_rss_kib\":"<<children.ru_maxrss
       <<",\"peak_rss_kib\":"<<usage.ru_maxrss<<",\"sources\":[";
    bool comma=false;
    for(const auto& ptr:sources) {
        auto& s=*ptr;const auto d=s.snapshot();const auto q=s.queue.snapshot();
        if(comma) out<<',';
        comma=true;
        out<<"{\"name\":"<<quoted(s.config.name)<<",\"bind\":"<<quoted(s.config.bind_ip)
           <<",\"port\":"<<s.socket.port()<<",\"template_ttl_seconds\":"<<s.config.template_ttl_seconds
           <<",\"kernel_timestamp_enabled\":"<<(s.socket.timestamp_enabled()?"true":"false")
           <<",\"socket_drops_available\":"<<(s.socket.overflow_enabled()?"true":"false")
           <<",\"socket_receive_buffer_bytes\":"<<s.socket.receive_buffer_bytes()
           <<",\"last_received_ns\":"<<s.last_received_ns<<",\"last_allowed_received_ns\":"<<s.last_allowed_received_ns
           <<",\"received\":";receive_json(out,s.received);
        out<<",\"decoded\":";decode_json(out,d.all);
        out<<",\"ifindex\":";interfaces_json(out,d.interfaces);
        out<<",\"datagrams_per_second\":"<<(elapsed>0?double(s.received.datagrams)/elapsed:0)
           <<",\"data_records_per_second\":"<<(elapsed>0?double(d.all.data_records)/elapsed:0)
           <<",\"post_warmup\":{\"received\":";receive_json(out,s.warm_received);
        out<<",\"decoded\":";decode_json(out,d.warm);
        out<<",\"ifindex\":";interfaces_json(out,d.warm_interfaces);
        out<<",\"datagrams_per_second\":"<<(warm_elapsed>0?double(s.warm_received.datagrams)/warm_elapsed:0)
           <<",\"data_records_per_second\":"<<(warm_elapsed>0?double(d.warm.data_records)/warm_elapsed:0)<<'}';
        out<<",\"queue\":{\"current_datagrams\":"<<q.count<<",\"current_payload_bytes\":"<<q.payload_bytes
           <<",\"peak_datagrams\":"<<q.peak_count<<",\"peak_payload_bytes\":"<<q.peak_payload_bytes
           <<",\"allocated_bytes\":"<<q.allocated_bytes<<",\"datagram_limit\":"<<q.count_limit
           <<",\"payload_capacity\":"<<q.payload_capacity<<",\"accepted\":"<<q.accepted
           <<",\"drops\":"<<q.drops<<",\"closed_rejections\":"<<q.closed_rejections<<'}'
           <<",\"cache_entries\":"<<d.cache_entries<<",\"worker_failed\":"<<(s.failed?"true":"false")<<'}';
    }
    out<<']';
    if(aggregator)out<<",\"aggregation\":"<<accounting_json(aggregator->stats());
    if(writer)out<<",\"writer\":"<<output_stats_json(writer->stats());
    if(diagnostic)out<<",\"diagnostic_output\":"<<output_stats_json(diagnostic->stats());
    if(delivery)out<<",\"delivery\":"<<delivery->diagnostics();
    out<<"}\n";return out.str();
}
uint64_t number(const std::string& s,uint64_t max) {
    uint64_t n{};auto [end,ec]=std::from_chars(s.data(),s.data()+s.size(),n);
    if(ec!=std::errc{}||end!=s.data()+s.size()||n>max) throw std::runtime_error("Invalid number: "+s);
    return n;
}
struct File {
    int fd=-1;
    ~File(){if(fd>=0) ::close(fd);}

};
}
int main(int argc,char** argv) {
    std::unique_ptr<AsyncOutput> diagnostic;
    try {
        std::string config_path,output_path,windows_path;
        std::optional<uint64_t> duration_override,warmup_override,interval_override;
        uint64_t duration=0,warmup=15,interval=10;
        bool check=false;
        for(int i=1;i<argc;++i) {
            std::string key=argv[i];
            if(key=="--check-config") {check=true;continue;}
            if(++i>=argc) throw std::runtime_error("Missing argument value");
            const std::string value=argv[i];
            if(key=="--config") config_path=value;
            else if(key=="--output") output_path=value;
            else if(key=="--windows-output") windows_path=value;
            else if(key=="--duration") duration_override=number(value,86400);
            else if(key=="--warmup") warmup_override=number(value,3600);
            else if(key=="--report-interval") interval_override=number(value,3600);
            else throw std::runtime_error("Unknown argument: "+key);
        }
        if(config_path.empty())throw std::runtime_error("Usage: nf9-receiver --config FILE [--check-config] [--output NEW_JSON] [--windows-output NEW_JSONL] [--duration SECONDS] [--warmup SECONDS] [--report-interval SECONDS]");
        std::ifstream input(config_path);if(!input)throw std::runtime_error("Config line 0: Cannot open config");
        auto app=read_app_config(input);
        auto configs=app.sources;
        auto links=std::make_unique<KnownLinks>(read_knownlinks(app));
        if(output_path.empty())output_path=app.output;
        if(windows_path.empty())windows_path=app.windows_output;
        duration=duration_override.value_or(app.duration);warmup=warmup_override.value_or(app.warmup);interval=interval_override.value_or(app.report_interval);
        if(!interval)throw std::runtime_error("report interval must be positive");
        std::optional<AggregationConfig> accounting_config;
        if(windows_path!="none"||app.delivery.enabled)accounting_config=app.aggregation;
        if(check) {std::cout<<"Valid sources: "<<configs.size()<<'\n';return 0;}
        sigset_t mask;sigemptyset(&mask);sigaddset(&mask,SIGINT);sigaddset(&mask,SIGTERM);
        if(pthread_sigmask(SIG_BLOCK,&mask,nullptr)!=0) throw std::runtime_error("Cannot block stop signals");
        diagnostic=std::make_unique<AsyncOutput>(STDERR_FILENO,8,512*1024,false);
        File signals;signals.fd=signalfd(-1,&mask,SFD_CLOEXEC|SFD_NONBLOCK);
        if(signals.fd<0) throw std::runtime_error("signalfd failed");
        std::vector<std::unique_ptr<Source>> sources;
        for(auto& c:configs) sources.push_back(std::make_unique<Source>(std::move(c)));
        AsyncOutput output(output_path,1,512*1024);
        std::unique_ptr<AsyncOutput> window_writer;
        if(windows_path!="none")window_writer=std::make_unique<AsyncOutput>(windows_path,accounting_config->writer_queue_records,accounting_config->writer_queue_bytes);
        std::unique_ptr<Delivery> delivery;
        if(app.delivery.enabled)delivery=std::make_unique<Delivery>(app.delivery);
        const uint64_t start=monotonic_ns(),boundary=start+warmup*second;
        std::unique_ptr<MinuteAggregator> aggregator;
        if(accounting_config)aggregator=std::make_unique<MinuteAggregator>(*accounting_config,std::move(*links),realtime_ns(),
            [&](const WindowRow& row){
                bool ok=true;
                if(window_writer)ok=window_writer->submit(window_json(row));
                if(delivery&&!delivery->submit(row))ok=false;
                return ok;
            });
        WorkerLifetime workers{sources};
        for(auto& s:sources) {s->aggregator=aggregator.get();s->start(boundary);}
        diagnostic->submit("{\"event\":\"ready\",\"sources\":"+std::to_string(sources.size())+"}\n");
        std::atomic<bool> reporter_failed=false;
        std::jthread reporter([&](std::stop_token stop) {
            try {
                uint64_t next=start+interval*second;
                while(!stop.stop_requested()) {
                    if(aggregator)aggregator->tick(realtime_ns());
                    const auto now=monotonic_ns();
                    if(now>=next) {
                        diagnostic->submit(report(sources,start,0,warmup,"running",false,aggregator.get(),window_writer.get(),diagnostic.get(),delivery.get()));
                        next=now+interval*second;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }catch(const std::exception&){reporter_failed=true;}
        });
        std::vector<pollfd> polls{{signals.fd,POLLIN,0}};
        for(auto& s:sources) polls.push_back({s->socket.fd(),POLLIN,0});
        std::string reason="duration";
        bool stop=false,failed=false;
        try {
            std::array<uint8_t,65535> payload{};
            while(!stop) {
                const auto now=monotonic_ns();
                if(duration && now-start>=duration*second) break;
                for(auto& s:sources) if(s->failed) throw std::runtime_error("Decoder worker failed: "+s->config.name);
                if(reporter_failed)throw std::runtime_error("Aggregation/reporter timer failed");
                if(poll(polls.data(),polls.size(),100)<0) {
                    if(errno==EINTR) continue;
                    throw std::runtime_error("poll failed");
                }
                if(polls[0].revents&POLLIN) {
                    signalfd_siginfo signal{};
                    if(read(signals.fd,&signal,sizeof(signal))==sizeof(signal))
                        reason=signal.ssi_signo==SIGINT?"SIGINT":"SIGTERM";
                    break;
                }
                for(size_t i=0;i<sources.size()&&!stop;++i) {
                    auto& s=*sources[i];
                    if(polls[i+1].revents&(POLLERR|POLLHUP|POLLNVAL)) {
                        ++s.received.receive_errors;throw std::runtime_error("Socket poll error: "+s.config.name);
                    }
                    if(!(polls[i+1].revents&POLLIN)) continue;
                    // Bound each batch so other sources, signals and timers cannot starve.
                    for(unsigned batch=0;batch<64;++batch) {
                        if(duration && monotonic_ns()-start>=duration*second) {stop=true;break;}
                        ReceivedDatagram d;
                        try {if(!s.socket.receive(payload,d)) break;}
                        catch(...) {++s.received.receive_errors;throw;}
                        s.receive_one(d,std::span(payload).first(std::min(d.original_size,payload.size())),boundary);
                    }
                }
            }
        } catch(const std::exception& e) {reason="error";failed=true;diagnostic->submit(std::string(e.what())+'\n');}
        const uint64_t reception_end=monotonic_ns(),coverage_end=realtime_ns();
        if(aggregator)aggregator->stop_receiving(coverage_end);
        for(auto& s:sources) {s->socket.close();s->queue.close();}
        for(auto& s:sources) {if(s->worker.joinable()) s->worker.join();if(s->failed) failed=true;}
        reporter.request_stop();reporter.join();
        if(reporter_failed)failed=true;
        if(aggregator) {aggregator->finish(coverage_end);if(aggregator->stats().failed)failed=true;}
        if(delivery&&!delivery->finish())failed=true;
        if(window_writer&&!window_writer->finish(std::chrono::seconds(5)))failed=true;
        // Diagnostics are lossy by design; a blocked sink cannot hold shutdown hostage.
        diagnostic->finish(std::chrono::milliseconds(200));
        if(failed) reason="error";
        const auto final=report(sources,start,reception_end,warmup,reason,true,aggregator.get(),window_writer.get(),diagnostic.get(),delivery.get());
        if(!output.submit(final)||!output.finish(std::chrono::seconds(2)))failed=true;
        return failed?1:0;
    } catch(const std::exception& e) {
        try {
            if(!diagnostic)diagnostic=std::make_unique<AsyncOutput>(STDERR_FILENO,1,4096,false);
            diagnostic->submit(std::string("nf9-receiver: ")+e.what()+'\n');
            diagnostic->finish(std::chrono::milliseconds(200));
        } catch(...) {}
        return 1;
    }
}
