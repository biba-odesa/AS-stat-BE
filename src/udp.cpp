#include "asstats/udp.hpp"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <time.h>
namespace asstats {
namespace {
uint64_t now(clockid_t id) {
    timespec t{};
    if(clock_gettime(id,&t)!=0) throw std::runtime_error("clock_gettime failed");
    return uint64_t(t.tv_sec)*1000000000ULL+uint64_t(t.tv_nsec);
}
void error(const std::string& action) {throw std::runtime_error(action+": "+std::strerror(errno));}
}
uint64_t monotonic_ns() {return now(CLOCK_MONOTONIC);}
uint64_t realtime_ns() {return now(CLOCK_REALTIME);}
UdpSocket::UdpSocket(const SourceConfig& c,bool timestamps) : destination_(c.bind_address) {
    fd_=socket(AF_INET,SOCK_DGRAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0);
    if(fd_<0) error("socket "+c.name);
    try {
        const int yes=1;
        if(c.socket_receive_buffer_bytes) {
            const int requested=static_cast<int>(c.socket_receive_buffer_bytes);
            // Linux may cap and double the request; report the actual getsockopt value.
            if(setsockopt(fd_,SOL_SOCKET,SO_RCVBUF,&requested,sizeof(requested))!=0)error("SO_RCVBUF "+c.name);
        }
        if(timestamps) timestamp_enabled_=setsockopt(fd_,SOL_SOCKET,SO_TIMESTAMPNS,&yes,sizeof(yes))==0;
        overflow_enabled_=setsockopt(fd_,SOL_SOCKET,SO_RXQ_OVFL,&yes,sizeof(yes))==0;
        if(setsockopt(fd_,IPPROTO_IP,IP_PKTINFO,&yes,sizeof(yes))!=0) error("IP_PKTINFO "+c.name);
        sockaddr_in bind_address{}; bind_address.sin_family=AF_INET;
        bind_address.sin_addr.s_addr=c.bind_address;bind_address.sin_port=htons(c.port);
        // Deliberately do not set SO_REUSEADDR or SO_REUSEPORT.
        if(bind(fd_,reinterpret_cast<sockaddr*>(&bind_address),sizeof(bind_address))!=0)
            error("bind "+c.name+" "+c.bind_ip+":"+std::to_string(c.port));
        socklen_t size=sizeof(bind_address);
        if(getsockname(fd_,reinterpret_cast<sockaddr*>(&bind_address),&size)!=0) error("getsockname");
        port_=ntohs(bind_address.sin_port);
        size=sizeof(receive_buffer_bytes_);
        if(getsockopt(fd_,SOL_SOCKET,SO_RCVBUF,&receive_buffer_bytes_,&size)!=0) error("SO_RCVBUF");
    } catch(...) {close();throw;}
}
UdpSocket::~UdpSocket() {close();}
void UdpSocket::close() {if(fd_>=0) {::close(fd_);fd_=-1;}}
bool UdpSocket::receive(std::span<uint8_t> payload,ReceivedDatagram& out) {
    sockaddr_in peer{};
    alignas(cmsghdr) std::array<unsigned char,256> control{};
    iovec iov{payload.data(),payload.size()};
    msghdr msg{};msg.msg_name=&peer;msg.msg_namelen=sizeof(peer);
    msg.msg_iov=&iov;msg.msg_iovlen=1;msg.msg_control=control.data();msg.msg_controllen=control.size();
    const auto n=recvmsg(fd_,&msg,MSG_DONTWAIT|MSG_TRUNC);
    if(n<0) {
        if(errno==EAGAIN || errno==EWOULDBLOCK || errno==EINTR) return false;
        error("recvmsg");
    }
    out={};out.original_size=static_cast<size_t>(n);
    out.truncated=(msg.msg_flags&MSG_TRUNC) || out.original_size>payload.size();
    out.control_truncated=msg.msg_flags&MSG_CTRUNC;
    out.info.received_monotonic_ns=monotonic_ns();
    out.info.exporter=peer.sin_addr.s_addr;out.info.destination=destination_;
    out.info.source_port=ntohs(peer.sin_port);out.info.destination_port=port_;
    out.destination_fallback=true;
    for(auto* c=CMSG_FIRSTHDR(&msg);c;c=CMSG_NXTHDR(&msg,c)) {
        if(c->cmsg_level==SOL_SOCKET && c->cmsg_type==SCM_TIMESTAMPNS && c->cmsg_len>=CMSG_LEN(sizeof(timespec))) {
            timespec t{};std::memcpy(&t,CMSG_DATA(c),sizeof(t));
            if(t.tv_sec>=0 && t.tv_nsec>=0 && t.tv_nsec<1000000000) {
                out.info.received_ns=uint64_t(t.tv_sec)*1000000000ULL+uint64_t(t.tv_nsec);
                out.kernel_timestamp=true;
            }
        } else if(c->cmsg_level==SOL_SOCKET && c->cmsg_type==SO_RXQ_OVFL && c->cmsg_len>=CMSG_LEN(sizeof(uint32_t))) {
            uint32_t drops{};std::memcpy(&drops,CMSG_DATA(c),sizeof(drops));out.socket_drop_counter=drops;
        } else if(c->cmsg_level==IPPROTO_IP && c->cmsg_type==IP_PKTINFO && c->cmsg_len>=CMSG_LEN(sizeof(in_pktinfo))) {
            in_pktinfo info{};std::memcpy(&info,CMSG_DATA(c),sizeof(info));
            out.info.destination=info.ipi_addr.s_addr;out.destination_fallback=false;
        }
    }
    if(!out.kernel_timestamp) out.info.received_ns=realtime_ns();
    return true;
}
} // namespace asstats
