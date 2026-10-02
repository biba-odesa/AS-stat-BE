#include "asstats/app_config.hpp"
#include "asstats/async_output.hpp"
#include "asstats/decoder.hpp"
#include "asstats/pcap.hpp"
#include <fstream>
#include <iostream>
#include <memory>
// Capture time drives both closure and template expiry, with no wall-clock replay acceleration.
int main(int argc,char** argv) {
    try {
        if(argc!=7||std::string(argv[2])!="--config"||std::string(argv[4])!="--port")
            throw std::runtime_error("Usage: nf9-aggregate-replay PCAP --config FILE --port PORT NEW_JSONL");
        std::ifstream cfg(argv[3]);auto app=asstats::read_app_config(cfg);auto c=app.aggregation;
        auto mapping=asstats::read_knownlinks(app);
        size_t consumed=0;auto port=std::stoul(argv[5],&consumed);
        if(consumed!=std::string(argv[5]).size()||!port||port>65535)throw std::runtime_error("Invalid port");
        const asstats::SourceConfig* source=nullptr;
        for(const auto& s:app.sources)if(s.port==port)source=&s;
        if(!source)throw std::runtime_error("Port is not configured");
        asstats::AsyncOutput writer(argv[6],c.writer_queue_records,c.writer_queue_bytes);
        std::unique_ptr<asstats::MinuteAggregator> agg;
        uint64_t first=0,last=0,clock=0;
        asstats::Decoder decoder({},uint64_t(source->template_ttl_seconds)*1000000000ULL,[&]{return clock;});asstats::PcapReader reader;
        std::ifstream pcap(argv[1],std::ios::binary);if(!pcap)throw std::runtime_error("Cannot read PCAP");
        reader.read(pcap,source->name,static_cast<uint16_t>(port),[&](auto payload,const auto& meta){
            if(meta.exporter_ip!=asstats::ipv4_text(source->exporters.front())||meta.destination_ip!=source->bind_ip)return;
            if(!agg){first=meta.received_ns;agg=std::make_unique<asstats::MinuteAggregator>(c,std::move(mapping),first,[&](const auto& row){return writer.submit(asstats::window_json(row));});}
            last=meta.received_ns;clock=std::max(clock,last);agg->tick(clock);
            decoder.decode(payload,meta,[&](const auto& r){agg->add(r,clock);},[&](const auto& r){agg->options(r);});
        });
        if(!agg)throw std::runtime_error("No matching UDP datagrams");
        agg->finish(last);bool ok=writer.finish();const auto& d=decoder.diagnostics();
        std::cout<<"{\"first_received_ns\":"<<first<<",\"last_received_ns\":"<<last
            <<",\"datagrams\":"<<d.datagrams<<",\"data_records\":"<<d.data_records<<",\"options_records\":"<<d.options_records
            <<",\"unknown_template_flowsets\":"<<d.unknown_template_flowsets<<",\"aggregation\":"<<asstats::accounting_json(agg->stats())
            <<",\"writer\":"<<asstats::output_stats_json(writer.stats())<<"}\n";
        return ok&&!agg->stats().failed?0:1;
    }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
