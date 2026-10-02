#include "asstats/decoder.hpp"
#include "asstats/pcap.hpp"
#include "asstats/decimal_sum.hpp"
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace {
std::string quote(const std::string& s) {
    std::ostringstream out;
    out << '"';
    for (unsigned char ch : s) {
        if (ch == '"' || ch == '\\') out << '\\' << ch;
        else if (ch < 32) out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << unsigned(ch) << std::dec;
        else out << ch;
    }
    out << '"';
    return out.str();
}
void identity(std::ostream& out, const asstats::RecordIdentity& id) {
    out << "\"configured_source\":" << quote(id.source.configured_source)
        << ",\"exporter_ip\":" << quote(id.source.exporter_ip)
        << ",\"destination_ip\":" << quote(id.source.destination_ip)
        << ",\"source_port\":" << id.source.source_port
        << ",\"destination_port\":" << id.source.destination_port
        << ",\"received_ns\":" << id.source.received_ns
        << ",\"source_id\":" << id.source_id << ",\"template_id\":" << id.template_id;
}
template<class T> void optional(std::ostream& out, const char* name, const std::optional<T>& value) {
    out << ',' << quote(name) << ':';
    if (value) out << uint64_t(*value);
    else out << "null";
}
struct Totals {
    uint64_t records{}, bytes_present{}, packets_present{};
    asstats::DecimalSum bytes, packets;
};
std::ofstream output_file(const std::string& path) {
    if (path.empty()) return {};
    if (std::filesystem::exists(path)) throw std::runtime_error("Output already exists: " + path);
    std::ofstream out(path);
    if (!out) throw std::runtime_error("Cannot create output: " + path);
    out.exceptions(std::ios::badbit | std::ios::failbit);
    return out;
}
}
int main(int argc, char** argv) {
    try {
        if (argc < 2) throw std::runtime_error("Usage: nf9-replay PCAP --source NAME --port PORT [--options-jsonl PATH] [--records-jsonl PATH] [--max-templates N]");
        std::string source, options_path, records_path;
        uint16_t port = 0;
        asstats::DecoderLimits limits;
        for (int i = 2; i < argc; i += 2) {
            if (i + 1 >= argc) throw std::runtime_error("Missing argument value");
            const std::string key = argv[i], value = argv[i + 1];
            if (key == "--source") source = value;
            else if (key == "--options-jsonl") options_path = value;
            else if (key == "--records-jsonl") records_path = value;
            else if (key == "--port" || key == "--max-templates") {
                uint64_t n{};
                const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), n);
                if (ec != std::errc{} || end != value.data() + value.size() || !n || n > 65535)
                    throw std::runtime_error("Invalid numeric argument");
                if (key == "--port") port = static_cast<uint16_t>(n);
                else limits.max_templates = static_cast<size_t>(n);
            } else throw std::runtime_error("Unknown argument: " + key);
        }
        if (source.empty() || source.size() > 256 || !port) throw std::runtime_error("Explicit --source and --port are required");
        std::ifstream input(argv[1], std::ios::binary);
        if (!input) throw std::runtime_error("Cannot read PCAP");
        auto options_out = output_file(options_path);
        auto records_out = output_file(records_path);
        asstats::Decoder decoder(limits);
        asstats::PcapReader reader;
        using TotalKey = std::tuple<std::string, std::string, uint32_t, unsigned>;
        std::map<TotalKey, Totals> totals;
        uint64_t first = 0, last = 0;
        reader.read(input, source, port, [&](auto payload, const auto& metadata) {
            if (reader.diagnostics().udp_datagrams == 1) first = metadata.received_ns;
            last = metadata.received_ns;
            decoder.decode(payload, metadata, [&](const asstats::FlowRecord& r) {
                TotalKey key{r.identity.source.configured_source, r.identity.source.exporter_ip,
                             r.identity.source_id, r.ip_version.value_or(0)};
                // Bound diagnostic grouping independently of the decoder cache.
                if (!totals.contains(key) && totals.size() >= 4096) throw std::runtime_error("Too many offline total groups");
                auto& total = totals[key];
                ++total.records;
                if (r.bytes) { total.bytes.add(*r.bytes); ++total.bytes_present; }
                if (r.packets) { total.packets.add(*r.packets); ++total.packets_present; }
                if (records_out.is_open()) {
                    records_out << '{'; identity(records_out, r.identity);
                    optional(records_out, "ip_version", r.ip_version);
                    optional(records_out, "source_asn", r.source_asn);
                    optional(records_out, "destination_asn", r.destination_asn);
                    optional(records_out, "input_ifindex", r.input_ifindex);
                    optional(records_out, "output_ifindex", r.output_ifindex);
                    optional(records_out, "bytes", r.bytes); optional(records_out, "packets", r.packets);
                    optional(records_out, "first_switched", r.first_switched);
                    optional(records_out, "last_switched", r.last_switched);
                    records_out << "}\n";
                }
            }, [&](const asstats::OptionsRecord& r) {
                if (!options_out.is_open()) return;
                options_out << '{'; identity(options_out, r.identity);
                options_out << ",\"scopes\":[";
                bool comma = false;
                for (const auto& s : r.scopes) {
                    if (comma) options_out << ',';
                    comma = true;
                    options_out << "{\"type\":" << s.type << ",\"length\":" << s.raw.size() << ",\"raw_hex\":\"";
                    for (auto byte : s.raw) options_out << std::hex << std::setw(2) << std::setfill('0') << unsigned(byte);
                    options_out << std::dec << "\"}";
                }
                options_out << "],\"options\":[";
                comma = false;
                for (const auto& v : r.values) {
                    if (comma) options_out << ',';
                    comma = true;
                    options_out << "{\"field\":" << v.field << ",\"value\":" << v.value << '}';
                }
                options_out << "]}\n";
            });
        });
        // Flush before printing the success summary so disk errors cannot masquerade as success.
        if (options_out.is_open()) options_out.close();
        if (records_out.is_open()) records_out.close();
        const auto& p = reader.diagnostics();
        const auto& d = decoder.diagnostics();
        std::cout << "{\n\"pcap\":{";
#define P(name) std::cout << "\"" #name "\":" << p.name << ','
        P(packets); P(udp_datagrams); P(truncated_packets); P(malformed_packets);
        P(unsupported_packets); P(fragments);
#undef P
        std::cout << "\"filtered_datagrams\":" << p.filtered_datagrams << "},\n\"decoder\":{";
#define D(name) std::cout << "\"" #name "\":" << d.name << ','
        D(datagrams); D(wrong_version); D(malformed_datagrams); D(flowsets);
        D(data_template_flowsets); D(options_template_flowsets); D(data_templates); D(options_templates);
        D(template_refreshes); D(template_replacements); D(malformed_template_flowsets); D(cache_limit_rejections);
        D(unsupported_short_records); D(unknown_template_flowsets); D(unknown_template_bytes);
        D(malformed_data_flowsets); D(data_records); D(options_records); D(ipv4_records); D(ipv6_records);
        D(incomplete_records); D(invalid_family_records); D(reserved_flowsets); D(unaligned_flowsets);
        D(zero_length_system_scopes);
#undef D
        std::cout << "\"cache_entries\":" << decoder.cache_size() << "},\n\"first_received_ns\":" << first
                  << ",\"last_received_ns\":" << last << ",\n\"totals\":[";
        bool comma = false;
        for (const auto& [key, t] : totals) {
            if (comma) std::cout << ',';
            comma = true;
            const auto& [configured, exporter, domain, family] = key;
            std::cout << "\n{\"configured_source\":" << quote(configured) << ",\"exporter_ip\":" << quote(exporter)
                      << ",\"source_id\":" << domain << ",\"ip_version\":" << family
                      << ",\"records\":" << t.records << ",\"bytes_present\":" << t.bytes_present
                      << ",\"packets_present\":" << t.packets_present
                      << ",\"raw_bytes\":" << quote(t.bytes.str()) << ",\"raw_packets\":" << quote(t.packets.str()) << '}';
        }
        std::cout << "\n]}\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "nf9-replay: " << e.what() << '\n';
        return 1;
    }
}
