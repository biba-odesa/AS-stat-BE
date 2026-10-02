#include "asstats/decoder.hpp"
#include "asstats/decimal_sum.hpp"
#include "fixtures.hpp"
#include <iostream>
#include <limits>
#include <random>

using namespace asstats;
struct Harness {
    Decoder decoder;
    SourceMetadata source{"fixture", "192.0.2.1", "192.0.2.2", 1234, 2057, 1234567890123456789ULL};
    std::vector<FlowRecord> flows;
    std::vector<OptionsRecord> options;
    explicit Harness(DecoderLimits limits = {}) : decoder(limits) {}
    void send(const Bytes& data) {
        decoder.decode(data, source, [&](const auto& r) { flows.push_back(r); }, [&](const auto& r) { options.push_back(r); });
    }
    void install(uint32_t domain, uint16_t id, Fields fields) { send(nf(domain, set(0, schema(id, fields)))); }
};
void isolation() {
    Harness h;
    h.install(1, 900, {{16,4}});
    h.install(2, 900, {{17,4}});
    h.send(nf(1, set(900, {0,0,0,7})));
    h.send(nf(2, set(900, {0,0,0,8})));
    require(h.flows[0].source_asn == 7 && !h.flows[0].destination_asn, "domain 1 schema");
    require(h.flows[1].destination_asn == 8 && !h.flows[1].source_asn, "domain 2 schema");
    h.source.configured_source = "other";
    h.send(nf(1, set(900, {0,0,0,9})));
    h.install(1, 900, {{14,4}});
    h.send(nf(1, set(900, {0,0,0,9})));
    require(h.flows.back().output_ifindex == 9, "configured source isolation");
    h.source.exporter_ip = "192.0.2.3";
    h.send(nf(1, set(900, {0,0,0,10})));
    h.install(1, 900, {{10,4}});
    h.source.source_port = 4321;
    h.send(nf(1, set(900, {0,0,0,10})));
    require(h.flows.back().input_ifindex == 10, "exporter isolation, port independent");
    require(h.decoder.diagnostics().unknown_template_flowsets == 2, "unknown isolated schemas");
}
void normalized() {
    Harness h;
    h.install(42, 1234, {{2,8},{65000,19},{17,4},{8,4},{1,8},{16,4},{14,4},{10,4},{22,4},{21,4}});
    Bytes b; put(b, UINT64_MAX, 8); b.resize(b.size()+19, 0xab);
    put(b, 4200000001U, 4); put(b, 0xc0000201, 4); put(b, UINT64_MAX-1, 8);
    put(b, 4000000000U, 4); put(b, 123456789, 4); put(b, 100000, 4);
    put(b, 0xffffffff, 4); put(b, 0, 4);
    h.send(nf(42, set(1234, b)));
    require(h.flows.size() == 1, "one record");
    const auto& r = h.flows[0];
    require(r.complete() && r.ip_version == 4, "complete ipv4");
    require(r.source_asn == 4000000000U && r.destination_asn == 4200000001U, "32 bit ASN");
    require(r.bytes == UINT64_MAX-1 && r.packets == UINT64_MAX, "64 bit counters without sampling");
    require(r.first_switched == UINT32_MAX && r.last_switched == 0, "raw switched times");
    require(r.identity.source.received_ns == h.source.received_ns && r.identity.source_id == 42 &&
            r.identity.source.configured_source == "fixture", "identity and timestamp");
    h.install(42, 1235, {{28,16},{16,2},{17,4},{1,8},{2,4},{10,2},{14,4}});
    b.assign(16,0); put(b, 0,2); put(b, 70000,4); put(b, 1ULL<<40,8); put(b,0,4); put(b,0,2); put(b,0,4);
    h.send(nf(42, set(1235,b)));
    require(h.flows.back().complete() && h.flows.back().ip_version == 6, "ipv6 without field 60");
    require(h.flows.back().source_asn == 0 && h.flows.back().packets == 0, "present zero");
}
void missing_and_family() {
    Harness h;
    h.install(1,800,{{16,4}});
    h.send(nf(1,set(800,{0,0,0,0})));
    const auto& r = h.flows.back();
    require(r.source_asn.has_value() && *r.source_asn == 0 && !r.destination_asn && !r.bytes && !r.ip_version,
            "missing must differ from zero");
    require(!r.complete() && h.decoder.diagnostics().incomplete_records == 1, "incomplete diagnostic");
    h.install(1,801,{{60,1},{1,4}});
    h.send(nf(1,set(801,{6,0,0,0,1})));
    require(h.flows.back().ip_version == 6, "explicit family without addresses");
    h.install(1,802,{{60,1},{8,4}});
    h.send(nf(1,set(802,{6,0,0,0,1})));
    require(h.decoder.diagnostics().invalid_family_records == 1, "family conflict");
}
void options_scopes() {
    Harness h;
    h.send(nf(99,set(1,option_schema(1000,{{1,0},{2,4}},{{34,4},{35,1},{65000,8}}))));
    Bytes b; put(b,12345,4); put(b,100,4); put(b,2,1); put(b,UINT64_MAX,8);
    h.send(nf(99,set(1000,b)));
    require(h.flows.empty() && h.options.size()==1, "options are separate");
    const auto& o = h.options[0];
    require(o.identity.source_id == 99 && o.scopes.size()==2 && o.scopes[0].type==1 && o.scopes[0].raw.empty(), "zero System scope explicit");
    require(o.scopes[1].raw == Bytes({0,0,48,57}), "interface scope raw value");
    require(o.values.size()==2 && o.values[0].field==34 && o.values[0].value==100, "sampling scoped value");
    require(h.decoder.diagnostics().zero_length_system_scopes==1, "scope compatibility diagnostic");
    h.send(nf(100,set(1000,b)));
    require(h.decoder.diagnostics().unknown_template_flowsets==1, "options not global");
    h.send(nf(99,set(1,option_schema(1001,{{2,0}},{{34,4}}))));
    require(h.decoder.cache_size()==1, "only System may have zero scope length");
    Harness strict({1024,256,65535,false});
    strict.send(nf(99,set(1,option_schema(1000,{{1,0}},{{34,4}}))));
    require(strict.decoder.cache_size()==0, "compatibility switch");
}
void unknown() {
    Harness h;
    h.send(nf(1,set(444,{1,2,3,4})));
    h.install(1,444,{{16,4}});
    require(h.flows.empty() && h.decoder.diagnostics().unknown_template_flowsets==1 &&
            h.decoder.diagnostics().unknown_template_bytes==4, "unknown skipped without retrodecode");
}
void replacement_atomic() {
    Harness h;
    h.install(1,900,{{16,4}});
    auto incomplete = schema(900,{{17,4},{1,8}}); incomplete.pop_back();
    h.send(nf(1,set(0,incomplete)));
    h.send(nf(1,set(900,{0,0,0,3})));
    require(h.flows.back().source_asn==3, "incomplete replacement preserves old");
    auto suffix = join(schema(900,{{17,4}}), Bytes{0xff});
    h.send(nf(1,set(0,suffix)));
    h.send(nf(1,set(900,{0,0,0,4})));
    require(h.flows.back().source_asn==4, "malformed template FlowSet transactional");
    h.install(1,900,{{17,4}});
    h.send(nf(1,set(900,{0,0,0,5})));
    require(h.flows.back().destination_asn==5 && !h.flows.back().source_asn, "valid replacement");
    h.install(1,900,{{17,4}});
    require(h.decoder.diagnostics().template_replacements==1 && h.decoder.diagnostics().template_refreshes==1, "refresh vs replace");
    auto bad_packet = nf(1,join(set(0,schema(900,{{16,4}})),Bytes{0,1,0,0}));
    h.send(bad_packet);
    h.send(nf(1,set(900,{0,0,0,6})));
    require(h.flows.back().destination_asn==6, "bad outer boundary cannot change cache");
    h.install(1,900,{{1,8}});
    Bytes wide; put(wide,UINT64_MAX,8);
    h.send(nf(1,set(900,wide)));
    require(h.flows.back().bytes==UINT64_MAX && !h.flows.back().destination_asn, "replacement changes record width");
    h.send(nf(1,set(1,option_schema(900,{{1,0}},{{34,4}}))));
    h.send(nf(1,set(900,{0,0,0,100})));
    require(h.options.size()==1, "replacement can change template kind");
}
void padding() {
    Harness h;
    h.install(1,700,{{16,4},{60,1}});
    h.send(nf(1,set(700,{0,0,0,0,4,0,0,0})));
    require(h.flows.size()==1 && h.flows.back().source_asn==0, "zero ASN record, 3 padding bytes");
    h.send(nf(1,set(700,{0,0,0,0,4})));
    require(h.flows.size()==2, "exact unaligned record supported");
    h.send(nf(1,set(700,{0,0,0,0,4,0,0,1})));
    h.send(nf(1,set(700,{0,0,0,0,4,0,0})));
    require(h.flows.size()==2 && h.decoder.diagnostics().malformed_data_flowsets==2, "nonzero or misaligned tail rejected");
    h.install(1,701,{{16,4}});
    h.send(nf(1,set(701,{0,0,0,0})));
    require(h.flows.size()==3, "full zero record is not padding");
    h.install(1,702,{{60,1}});
    h.send(nf(1,set(702,{4,0,0,0})));
    require(h.flows.size()==3 && h.decoder.diagnostics().unsupported_short_records==1, "ambiguous short records are not invented");
    auto o = option_schema(800,{{1,0}},{{34,4}});
    o.insert(o.end(),2,0);
    h.send(nf(1,set(1,o)));
    h.send(nf(1,set(800,{0,0,0,100})));
    require(h.options.size()==1, "options template padding");
}
void invalid_templates() {
    Harness h;
    for (auto fields : std::vector<Fields>{{{16,8}},{{1,9}},{{21,2}},{{16,4},{16,4}},{{8,16}},{{65000,0}}})
        h.install(1,900,fields);
    auto bad_options=option_schema(900,{{1,0}},{{34,4}}); bad_options[3]=3;
    h.send(nf(1,set(1,bad_options)));
    require(h.decoder.cache_size()==0 && h.decoder.diagnostics().malformed_template_flowsets==7, "invalid widths, duplicates and scope descriptors");
}
void limits() {
    Harness h({1,4,16,true});
    h.install(1,900,{{16,4}});
    h.install(1,901,{{16,4}});
    require(h.decoder.cache_size()==1 && h.decoder.diagnostics().cache_limit_rejections==1, "cache cap");
    h.install(1,900,{{17,4}});
    h.send(nf(1,set(900,{0,0,0,7})));
    require(h.flows.back().destination_asn==7, "replacement allowed at cap");
    h.install(1,900,{{65000,65535},{65001,65535}});
    h.install(1,900,{{1,4},{2,4},{16,4},{17,4},{60,1}});
    require(h.decoder.diagnostics().malformed_template_flowsets==2, "record width and field count caps");
    h.decoder.clear_cache();
    require(h.decoder.cache_size()==0, "explicit cache reset");
}
void malformed_and_fuzz() {
    Harness h;
    h.send({0,10});
    require(h.decoder.diagnostics().wrong_version==1, "wrong version diagnostic");
    auto data=nf(1,set(0,schema(900,{{16,4},{1,8}})));
    for (size_t n=0;n<data.size();++n) {
        Harness isolated;
        isolated.send(Bytes(data.begin(),data.begin()+static_cast<std::ptrdiff_t>(n)));
        require(isolated.decoder.cache_size()==0, "every template truncation is rejected");
    }
    std::mt19937 rng(20260929);
    for (unsigned i=0;i<10000;++i) {
        Bytes bytes(rng()%512);
        for (auto& b:bytes) b=static_cast<uint8_t>(rng());
        if (bytes.size()>=2 && i%2==0) { bytes[0]=0; bytes[1]=9; }
        h.send(bytes);
    }
    require(h.decoder.cache_size()<=1024, "fuzz bounded cache");
}
void sum_overflow() {
    DecimalSum n;
    require(n.str()=="0","empty total");
    n.add(UINT64_MAX); n.add(UINT64_MAX); n.add(2);
    require(n.str()=="36893488147419103232", "exact sum beyond uint64");
}
int main() {
    unsigned passed=0;
    try {
        for (const auto& [name,test] : std::vector<std::pair<const char*,void(*)()>>{
            {"isolation",isolation},{"normalized",normalized},{"missing_and_family",missing_and_family},
            {"options_scopes",options_scopes},{"unknown",unknown},{"replacement_atomic",replacement_atomic},
            {"padding",padding},{"invalid_templates",invalid_templates},{"limits",limits},
            {"malformed_and_fuzz",malformed_and_fuzz},{"sum_overflow",sum_overflow}}) {
            test(); ++passed; std::cout << "PASS " << name << '\n';
        }
    } catch (const std::exception& e) { std::cerr << "FAIL after " << passed << ": " << e.what() << '\n'; return 1; }
}
