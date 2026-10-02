# Configuration

The sole configuration argument is `nf9-receiver --config FILE`. `--check-config` validates syntax, mappings, limits and readable knownlinks without binding UDP, writing output or locking spool. Errors report line numbers. Format: `key = value`, whitespace, empty lines and whole-line `#` comments. No inline comments or general configuration language.

See config/asstat.conf.example and config/knownlinks.example. All example addresses use the documentation prefix 192.0.2.0/24. Replace them with locally assigned bind/exporter addresses before operation. Ports are unique (1–16 listeners), each mapped to exactly one allowlisted exporter. UDP source ports are unrestricted. No SO_REUSEPORT, protocol autodetection or sFlow. Transport is IPv4; records may be IPv4 or IPv6.

Required keys: netflow9_ports, bind_address (specific unicast IPv4), exporters, samplerate (positive uint64 per exporter), exported_counters (sampled/already_scaled per exporter), knownlinks_file, replace_asn (uint32/none), private_asn_ranges (inclusive uint32 pairs) and exclude_asn (comma-separated uint32). Sampling policy never defaults silently. Unknown/repeated keys, extra/missing mappings, invalid/reversed ranges and conflicting endpoints fail validation.

Template cache identity includes configured source, exporter IP, Source ID and template ID; TTL is from valid template refresh using monotonic time, not data use. Sequence/uptime decrease does not automatically reset cache. After exporter restart, an old schema may persist until refresh or expiry.

Receive queue count and byte limits apply per source, with a shared configuration budget; overflow drops new datagrams. `socket_receive_buffer_bytes=0` keeps the kernel default; positive requests may be capped by the kernel. Kernel drops, receive-queue drops and unknown templates are distinct diagnostics.

`close_delay_seconds`, `max_active_windows`, `max_active_keys` bound global aggregation. `writer_queue_records/bytes` bound optional JSONL output. `windows_output=none` disables detailed JSONL while delivery=true keeps aggregation enabled. `duration=0` runs until signal. `warmup` affects diagnostics only; `report_interval` controls stderr summaries. Diagnostic output is bounded and may skip summaries under pressure.

`output` and non-none `windows_output` must be new files; existing targets are not overwritten. The provided launcher creates a unique report directory per start and overrides --output. It retains at most 32 run directories, which limits count, not a strict byte quota. Operational logs should also have a bounded journald policy.

Delivery keys in the example configure URL, spool path, spool byte/file limits, RAM queue count/bytes, batch count/bytes, HTTP timeout, capped exponential retry delays and total shutdown budget. Only local `http://127.0.0.1:PORT` is supported; HTTP proxy is bypassed. Spool must be receiver-owned and not group/world writable. Keep it separate from VM storage. The public copy rejects the documented default /var/lib/victoriametrics storage path; this is not a generic detector of all possible VM data directories. Do not use another VM storage location as spool.

The sample limits are 1GiB/8192 spool files, 512 records/128KiB per HTTP batch, 3s HTTP timeout, retry 1–60s and 10s delivery shutdown. Retention expiry uses the existing seven-day delivery policy; old timestamps are not moved forward. Corrupt/unfinished/expired/permanent-error files consume spool space and need deliberate operator handling. Do not delete unconfirmed batches automatically. Reducing the batch limit below existing batch sizes can prevent recovery and requires inspection.

CLI overrides supported: --output, --windows-output, --duration, --warmup, --report-interval. Offline decoder nf9-replay is independent of full config; nf9-aggregate-replay accepts --config, a selected --port and a new output JSONL filename. Replay does not implicitly deliver to VM.
