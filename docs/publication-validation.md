# Publication validation

The public tree is prepared separately from an operational checkout; no running collector, helper, config, service or storage directory is renamed or restarted. Operational context, captures, live results, private configurations, pilot units/wrappers, SQLite references, spool and build artifacts are excluded. Only synthetic tests and documentation addresses are included.

Validation performed: Release build with GCC 14.2.0, CMake 3.31.6, Python 3.13.5 and libcurl 8.14.1; 11/11 CTest cases passed (decoder, PCAP, receive queues/sockets, aggregation, diagnostics, configuration and delivery fault tests). The published three-source configuration passed --check-config after substituting only its installed knownlinks path with the supplied example file. No live capture or real receiver was started.

Launcher checked with a fake receiver: 35 sequential starts wrote unique new final files; rotation retained 32 runs. systemd-analyze verify passed in a temporary installation tree with executable/target stubs; units were not installed or started. Verification on the live filesystem reports missing not-yet-installed example binaries, so static verification does not demonstrate deployment runtime behavior.

Official VictoriaMetrics documentation and release v1.153.0 assets were checked. The official linux-amd64 checksum file contains both archive and extracted-binary SHA256 entries, which the install instructions verify separately. Existing binary CLI help confirms the documented retention, dedup and listen flags. The pinned release is an instruction example, not a claim that the existing installation runs this version or a performance recommendation.

The only C++ deployment-specific publication change substitutes the private VM storage exclusion path with the documented default /var/lib/victoriametrics in config validation and canonical spool validation; the matching synthetic test changes accordingly. Fixture/example home ASN is a documentation ASN. Decoder, aggregation windows and HTTP delivery algorithms are unchanged. Operational checkout binaries/configs retain their original behavior.

New code license: BSD-2-Clause, copyright 2026 biba-odesa, explicitly approved by the owner. Upstream idea/format attribution and limited source-comparison findings are in provenance.md; no copied upstream source was identified.
