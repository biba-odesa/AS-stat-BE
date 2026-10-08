#include "asstats/aggregation.hpp"
#include <sstream>
#include <stdexcept>
using namespace asstats;
int main(){
    std::istringstream links("192.0.2.1 1 link-a Name 123456 100\n");
    std::vector<std::pair<uint64_t,bool>> minutes;
    AggregationConfig config;
    MinuteAggregator a(config,KnownLinks(links),1000000000ULL,[](const auto&){return true;},
        [&](uint64_t m,bool partial,const auto& rows){if(!rows.empty())throw std::runtime_error("Unexpected rows");minutes.emplace_back(m,partial);return true;});
    a.tick(125000000000ULL);
    if(minutes.size()!=2||minutes[0]!=std::pair<uint64_t,bool>{0,true}||minutes[1]!=std::pair<uint64_t,bool>{60,false})throw std::runtime_error("Missing empty minute coverage");
    a.finish(150000000000ULL);
    if(minutes.size()!=3||minutes[2]!=std::pair<uint64_t,bool>{120,true})throw std::runtime_error("Missing final partial coverage");
    std::istringstream other("192.0.2.1 1 link-a Name 123456 100\n");minutes.clear();
    MinuteAggregator b(config,KnownLinks(other),0,[](const auto&){return true;},
        [&](uint64_t m,bool partial,const auto&){minutes.emplace_back(m,partial);return true;});
    b.incomplete_minute(60000000000ULL);b.tick(125000000000ULL);
    if(minutes.size()!=2||minutes[0].second||!minutes[1].second)throw std::runtime_error("Known incomplete minute was labeled full");
}
