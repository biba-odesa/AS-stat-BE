#pragma once
#include "aggregation.hpp"
#include <condition_variable>
#include <thread>
#include <memory>
namespace asstats {
struct DeliveryConfig {
    bool enabled=false;
    std::vector<std::string> permitted_child_directories;
    std::string url="http://127.0.0.1:8428",directory="/var/spool/asstat";
    uint64_t spool_bytes=1073741824,spool_files=8192,queue_records=32768,queue_bytes=16777216;
    uint64_t batch_records=512,batch_bytes=131072,http_timeout_ms=3000;
    uint64_t retry_initial_ms=1000,retry_max_ms=60000,shutdown_timeout_ms=10000;
    uint64_t retention_seconds=604800,interval_seconds=60;
};
// Only complete, immutable global minute totals enter this boundary.
std::string import_json(const WindowRow&);
class Delivery {
public:
    explicit Delivery(DeliveryConfig);
    ~Delivery();
    bool submit(const WindowRow&);
    // Callback means publication to the local durable spool, not VM crash durability.
    bool submit_durable(const std::vector<WindowRow>&,std::function<void(bool)>);
    bool finish();
    std::string diagnostics() const;
private:
    struct Receipt {size_t remaining{};bool failed{};std::function<void(bool)> completion;};
    struct Item {std::string text;uint64_t timestamp;std::shared_ptr<Receipt> receipt;};
    DeliveryConfig config_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Item> queue_;
    std::thread pump_;
    int fd_=-1,pid_=-1;
    bool closing_=false,done_=false,failed_=false;
    uint64_t deadline_=0,bytes_=0,peak_bytes_=0,peak_records_=0;
    uint64_t partial_=0,accepted_=0,lost_=0,unconfirmed_=0,inflight_=0;
    std::string worker_stats_="{}";
    void run();
};
// Internal worker entry point; executable isolation bounds receiver shutdown.
int delivery_worker(int argc,char** argv);
} // namespace asstats
