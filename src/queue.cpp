#include "asstats/queue.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
namespace asstats {
size_t BoundedQueue::descriptor_bytes(size_t count) {
    if(count>std::numeric_limits<size_t>::max()/sizeof(Slot)) throw std::invalid_argument("Queue count overflow");
    return count*sizeof(Slot);
}
BoundedQueue::BoundedQueue(size_t count,size_t memory) {
    const size_t fixed=descriptor_bytes(count);
    if(!count || memory<=fixed) throw std::invalid_argument("Queue memory/count invalid");
    stats_.allocated_bytes=memory; stats_.count_limit=count; stats_.payload_capacity=memory-fixed;
    slots_=std::make_unique<Slot[]>(count);
    bytes_=std::make_unique<uint8_t[]>(stats_.payload_capacity);
}
bool BoundedQueue::push(const DatagramInfo& info,std::span<const uint8_t> payload) {
    std::lock_guard lock(mutex_);
    if(closed_) {++stats_.closed_rejections;return false;}
    if(payload.size()>65535 || stats_.count==stats_.count_limit || payload.size()>stats_.payload_capacity-stats_.payload_bytes) {
        ++stats_.drops;return false;
    }
    // Two bounded copies allow payloads to wrap without fragmentation or per-packet allocation.
    const size_t first=std::min(payload.size(),stats_.payload_capacity-write_byte_);
    if(first) std::memcpy(bytes_.get()+write_byte_,payload.data(),first);
    if(payload.size()>first) std::memcpy(bytes_.get(),payload.data()+first,payload.size()-first);
    slots_[write_slot_]={info,payload.size()};
    write_slot_=(write_slot_+1)%stats_.count_limit;
    write_byte_=(write_byte_+payload.size())%stats_.payload_capacity;
    ++stats_.count; stats_.payload_bytes+=payload.size(); ++stats_.accepted;
    stats_.peak_count=std::max(stats_.peak_count,stats_.count);
    stats_.peak_payload_bytes=std::max(stats_.peak_payload_bytes,stats_.payload_bytes);
    ready_.notify_one();return true;
}
bool BoundedQueue::pop(QueuedDatagram& out,std::chrono::milliseconds wait) {
    std::unique_lock lock(mutex_);
    ready_.wait_for(lock,wait,[&]{return closed_ || stats_.count;});
    if(!stats_.count) return false;
    const auto& slot=slots_[read_slot_];out.info=slot.info;out.size=slot.length;
    const size_t first=std::min(out.size,stats_.payload_capacity-read_byte_);
    if(first) std::memcpy(out.payload.data(),bytes_.get()+read_byte_,first);
    if(out.size>first) std::memcpy(out.payload.data()+first,bytes_.get(),out.size-first);
    read_slot_=(read_slot_+1)%stats_.count_limit;
    read_byte_=(read_byte_+out.size)%stats_.payload_capacity;
    --stats_.count; stats_.payload_bytes-=out.size;
    return true;
}
void BoundedQueue::close() {std::lock_guard lock(mutex_);closed_=true;ready_.notify_all();}
bool BoundedQueue::drained() const {std::lock_guard lock(mutex_);return closed_ && !stats_.count;}
QueueSnapshot BoundedQueue::snapshot() const {std::lock_guard lock(mutex_);return stats_;}
} // namespace asstats
