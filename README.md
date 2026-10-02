# AS-stat-BE — Biba Edition

A Linux C++20 NetFlow v9 collector with minute aggregation and persistent batch delivery to VictoriaMetrics OSS single-node. Offline PCAP replay and synthetic tests are included. The project is in an operational pilot; long-term resource sizing and absolute sampling calibration remain open. sFlow is not implemented. The web interface is a separate project.

## Accounting contract

Each flow side is independent: input ifIndex maps to a known link, source ASN and `in`; output ifIndex maps to a known link, destination ASN and `out`. A missing, zero or unknown interface excludes only that side. Missing ASN is distinct from ASN 0; ASN 0 is retained unless explicitly excluded. Missing bytes or unknown IP family makes the record unsuitable for volume accounting. No inferred output interfaces and no deduplication of similar records across Source IDs.

The stable series identity is `link_id + asn + direction + ip_version`. Exporter address, UDP port, Source ID, display name and color are not labels. The sum across links does **not** guarantee unique traffic: one flow may contribute to multiple monitored sides/links.

`knownlinks` columns: exporter IPv4, ifIndex, stable link_id, display name, six-digit hex color, legacy sampling rate. Column six is ignored. Quoted display names, blank lines and comments are supported. Conflicting mappings are rejected; runtime reload is not implemented. Changing exporter/interface/name/color while retaining link_id preserves series identity.

The decoder returns raw counters. Each router has an explicit `sampled` (multiply once by samplerate) or `already_scaled` (effective multiplier one) policy for all Source IDs and both families. Options report sampling consistency; they do not silently change scale. The ASN policy replaces present nonzero private ASN values when configured, then applies exclusions independently to each side. `replace_asn = none` and empty ranges/exclusions disable these operations. Example ASNs and addresses are documentation values, not a deployment configuration.

Windows use server receive timestamps and cover UTC `[start, start + 60s)`. Closure has configurable grace and a timer independent of packet arrival. Starting/stopping mid-minute produces partial windows; partial windows are not delivered. Late records after closure are counted separately, never shifted to the present minute. Absent combinations do not produce zeros; a gap does not prove absence of traffic.

## Storage and reliability

`asstat_traffic_bytes` is a **gauge of bytes in one complete minute**, after accounting policy. Labels: `link_id`, `asn`, `direction` (`in`/`out`), `ip_version` (`4`/`6`). Timestamp is the UTC minute start in milliseconds. It is not a cumulative counter: do not apply `rate()` or `increase()`.

One global aggregate combines all configured sources before producing immutable totals. VictoriaMetrics deduplication removes repeated delivery; it does not add pieces of a minute. Aggregation uses checked uint128 arithmetic; VictoriaMetrics numeric encoding cannot represent every uint128 value exactly. Do not infer general precision from small test values.

Receive, decode, aggregate, diagnostic output and delivery use bounded queues. Closed batches are written to a separate disk spool, file-fsynced, atomically published and directory-fsynced before delivery. Unclosed minutes and RAM data not yet durably published can be lost on a receiver crash. HTTP success acknowledges API acceptance, not crash durability of VictoriaMetrics. Acknowledged batches are removed and the directory is fsynced; a crash before removal can replay identical points. Disk/full-queue errors are explicit losses, never silent deletion of old batches. Corrupt, unfinished and retention-expired batches are retained for inspection. One sender owns a spool via an exclusive lock. Retries preserve timestamps and values. See [installation](docs/installation.md), [configuration](docs/configuration.md) and [queries](docs/queries.md).

## Quick build

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
ctest --test-dir build-release --output-on-failure
```

Dependencies: Linux, C++20 compiler, CMake >=3.20, Python 3 for tests/launcher, pthreads and libcurl development headers. PCAP reading does not require libpcap. Tests use synthetic fixtures; real captures and operational data are not distributed.

## Origin and acknowledgements

[AS-Stats](https://github.com/manuelkasper/AS-Stats), by Manuel Kasper for Monzoon Networks AG, originated the ASN/link accounting idea and knownlinks format. According to the project owner, the earlier local Python implementation adapted the AS-Stats idea; AS-stat-BE developed from that adaptation into the current C++ implementation. This is an independent project, not an official AS-Stats v2 release, and no affiliation or endorsement is claimed.

New AS-stat-BE code is licensed under [BSD-2-Clause](LICENSE), copyright 2026 biba-odesa. No concrete transfer of upstream source into the C++ implementation was found in the review. The upstream project is acknowledged as an idea and format source, not as the author of this C++ code. See [provenance review](docs/provenance.md).
