#include "asstats/aggregation.hpp"
#include <arpa/inet.h>
#include <charconv>
#include <iomanip>
#include <set>
#include <sstream>
#include <stdexcept>
namespace asstats {
namespace {
uint64_t num(const std::string& s,uint64_t max) {
    uint64_t n{};const auto [end,ec]=std::from_chars(s.data(),s.data()+s.size(),n);
    if(ec!=std::errc{}||end!=s.data()+s.size()||n>max) throw std::runtime_error("Invalid integer: "+s);
    return n;
}
bool name(const std::string& s) {return !s.empty()&&s.size()<=64&&s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")==std::string::npos;}
}
KnownLinks::KnownLinks(std::istream& input) {
    std::string line;size_t lineno=0;
    std::map<std::string,std::pair<std::string,std::string>> metadata;
    while(std::getline(input,line)) {
        ++lineno;if(lineno>131072||line.size()>4096) throw std::runtime_error("knownlinks too large");
        std::istringstream in(line);in>>std::ws;if(in.eof()||in.peek()=='#') continue;
        std::string ip,index,id,label,color,ignored,extra;
        if(!(in>>ip>>index>>id>>std::quoted(label)>>color>>ignored)) throw std::runtime_error("Invalid knownlinks line "+std::to_string(lineno));
        if(in>>extra && !extra.starts_with('#')) throw std::runtime_error("Extra knownlinks columns");
        in_addr addr{};char canonical[INET_ADDRSTRLEN];
        if(inet_pton(AF_INET,ip.c_str(),&addr)!=1||!inet_ntop(AF_INET,&addr,canonical,sizeof(canonical))) throw std::runtime_error("Invalid knownlinks exporter");
        uint32_t n=static_cast<uint32_t>(num(index,UINT32_MAX));
        if(!n||!name(id)||label.empty()||label.size()>256||color.size()!=6||color.find_first_not_of("0123456789abcdefABCDEF")!=std::string::npos)
            throw std::runtime_error("Invalid knownlinks attributes");
        if(links_.size()>=65536||!links_.emplace(std::make_pair(std::string(canonical),n),id).second)
            throw std::runtime_error("Duplicate/conflicting exporter+ifIndex mapping");
        auto [m,inserted]=metadata.emplace(id,std::make_pair(label,color));
        if(!inserted&&m->second!=std::make_pair(label,color)) throw std::runtime_error("Conflicting metadata for link_id");
    }
    if(input.bad()) throw std::runtime_error("knownlinks read failed");
}
const std::string* KnownLinks::find(const std::string& exporter,uint32_t index) const {
    auto it=links_.find({exporter,index});return it==links_.end()?nullptr:&it->second;
}
} // namespace asstats
