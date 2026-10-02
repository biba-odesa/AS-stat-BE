#pragma once
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
namespace asstats {
struct OutputStats {
    uint64_t accepted{}, written{}, written_bytes{}, rejected{}, errors{};
    size_t queued{}, queued_bytes{}, peak_queued{}, peak_bytes{};
    bool timed_out{}, unreaped{};
};
// File I/O is isolated in a helper process; the pump thread only waits on bounded IPC.
class AsyncOutput {
public:
    AsyncOutput(int target_fd,size_t max_messages,size_t max_bytes,bool sync_on_close);
    AsyncOutput(const std::string& new_file,size_t max_messages,size_t max_bytes);
    ~AsyncOutput();
    bool submit(std::string message);
    bool finish(std::chrono::milliseconds timeout=std::chrono::seconds(5));
    OutputStats stats() const;
    AsyncOutput(const AsyncOutput&)=delete;
private:
    void start(int target_fd,bool sync_on_close);
    void pump();
    bool exchange(const std::string& message);
    bool expired();
    static size_t charge(const std::string& s) {return s.capacity()+sizeof(std::string)+64;}
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::string> queue_;
    size_t max_messages_, max_bytes_;
    OutputStats stats_;
    bool closing_{}, finished_{};
    std::chrono::steady_clock::time_point deadline_{};
    int socket_=-1, child_=-1;
    std::thread worker_;
};
std::string output_stats_json(const OutputStats&);
} // namespace asstats
