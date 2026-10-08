#include "asstats/app_config.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>
using namespace asstats;
int main(int argc,char** argv) {
    if(argc!=2)return 2;
    std::ifstream input(argv[1]);std::string base((std::istreambuf_iterator<char>(input)),{});
    const auto marker=base.find("[archive ");if(marker!=std::string::npos)base.resize(marker);
    auto read=[&](std::string suffix){std::istringstream f(base+suffix);return read_app_config(f);};
    const std::string valid="\n[archive arbitrary]\nurl = http://127.0.0.1:18429\ninterval_seconds = 420\nretention_days = 8\n# [archive four_years]\n# url = http://127.0.0.1:8432\n# interval_seconds = 14400\n# retention_days = 1461\n";
    auto c=read(valid);if(c.archives.size()!=1||c.archives[0].interval_seconds!=420)return 1;
    for(const auto& bad:{std::string("[archive ../unsafe]\n"),
        std::string("[archive x]\nurl = https://127.0.0.1:8429\ninterval_seconds = 300\nretention_days = 7\n"),
        std::string("[archive x]\nurl = http://127.0.0.1:8429\ninterval_seconds = 61\nretention_days = 7\n"),
        std::string("[archive x]\nurl = http://127.0.0.1:8429\ninterval_seconds = 86460\nretention_days = 7\n"),
        std::string("[archive x]\nurl = http://127.0.0.1:8429\ninterval_seconds = 300\nretention_days = 0\n"),
        std::string("[archive x]\nunknown = 1\n"),valid+valid,
        valid+std::string("[archive another]\nurl = http://127.0.0.1:18429\ninterval_seconds = 1800\nretention_days = 31\n")}) {
        bool rejected=false;try{read(bad);}catch(const std::exception& e){rejected=std::string(e.what()).starts_with("Config line ");}
        if(!rejected)throw std::runtime_error("Invalid archive configuration accepted");
    }
    std::cout<<"Archive configuration tests passed\n";
}
