#include "asstats/config.hpp"
#include "asstats/queue.hpp"
#include <arpa/inet.h>
#include <algorithm>
#include <charconv>
#include <set>
#include <sstream>
#include <stdexcept>
namespace asstats {
bool SourceConfig::allows(uint32_t a) const { return std::find(exporters.begin(),exporters.end(),a)!=exporters.end(); }
std::string ipv4_text(uint32_t a) {
    char text[INET_ADDRSTRLEN]{};
    if (!inet_ntop(AF_INET,&a,text,sizeof(text))) throw std::runtime_error("Cannot format IPv4 address");
    return text;
}
} // namespace asstats
