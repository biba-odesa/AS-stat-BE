# Configuration

The sole configuration argument is `nf9-receiver --config FILE`. `--check-config` validates syntax, mappings, limits and readable knownlinks without binding UDP, writing output or locking spool. Errors report line numbers. Format: `key = value`, whitespace, empty lines and whole-line `#` comments. No inline comments or general configuration language.

See config/asstat.conf.example and config/knownlinks.example. All example addresses use the documentation prefix 192.0.2.0/24. Replace them with locally assigned bind/exporter addresses before operation. Ports are unique (1–16 listeners), each mapped to exactly one allowlisted exporter. UDP source ports are unrestricted. No SO_REUSEPORT, protocol autodetection or sFlow. Transport is IPv4; records may be IPv4 or IPv6.

Required keys: netflow9_ports, bind_address (specific unicast IPv4), exporters, samplerate (positive uint64 per exporter), exported_counters (sampled/already_scaled per exporter), knownlinks_file, replace_asn (uint32/none), private_asn_ranges (inclusive uint32 pairs) and exclude_asn (comma-separated uint32). Sampling policy never defaults silently. Unknown/repeated keys, extra/missing mappings, invalid/reversed ranges and conflicting endpoints fail validation.

Template cache identity includes configured source, exporter IP, Source ID and template ID; TTL is from valid template refresh using monotonic time, not data use. Sequence/uptime decrease does not automatically reset cache. After exporter restart, an old schema may persist until refresh or expiry.

Receive queue count and byte limits apply per source, with a shared configuration budget; overflow drops new datagrams. `socket_receive_buffer_bytes=0` keeps the kernel default; see [UDP receive buffer](#udp-receive-buffer) for requests, kernel limits and verification. Kernel drops, receive-queue drops and unknown templates are distinct diagnostics.

`close_delay_seconds`, `max_active_windows`, `max_active_keys` bound global aggregation. `writer_queue_records/bytes` bound optional JSONL output. `windows_output=none` disables detailed JSONL while delivery=true keeps aggregation enabled. `duration=0` runs until signal. `warmup` affects diagnostics only; `report_interval` controls stderr summaries. Diagnostic output is bounded and may skip summaries under pressure.

`output` and non-none `windows_output` must be new files; existing targets are not overwritten. The provided launcher creates a unique report directory per start and overrides --output. It retains at most 32 run directories, which limits count, not a strict byte quota. Operational logs should also have a bounded journald policy.

Delivery keys in the example configure URL, spool path, spool byte/file limits, RAM queue count/bytes, batch count/bytes, HTTP timeout, capped exponential retry delays and total shutdown budget. Only local `http://127.0.0.1:PORT` is supported; HTTP proxy is bypassed. Spool must be receiver-owned and not group/world writable. Keep it separate from VM storage. The public copy rejects the documented default /var/lib/victoriametrics storage path; this is not a generic detector of all possible VM data directories. Do not use another VM storage location as spool.

The sample limits are 1GiB/8192 spool files, 512 records/128KiB per HTTP batch, 3s HTTP timeout, retry 1–60s and 10s delivery shutdown. Retention expiry uses the existing seven-day delivery policy; old timestamps are not moved forward. Corrupt/unfinished/expired/permanent-error files consume spool space and need deliberate operator handling. Do not delete unconfirmed batches automatically. Reducing the batch limit below existing batch sizes can prevent recovery and requires inspection.

CLI overrides supported: --output, --windows-output, --duration, --warmup, --report-interval. Offline decoder nf9-replay is independent of full config; nf9-aggregate-replay accepts --config, a selected --port and a new output JSONL filename. Replay does not implicitly deliver to VM.

## UDP receive buffer

`socket_receive_buffer_bytes` requests a kernel receive buffer for **each UDP listener**, before bind, using `setsockopt(SOL_SOCKET, SO_RCVBUF)`. Default `0` skips the request and keeps the kernel default; positive values are bytes (configuration maximum 16777216). This is separate from the bounded application queue controlled by `queue_memory_bytes`. The receiver reads `getsockopt(SO_RCVBUF)` back and reports that effective value in diagnostics; it does not use SO_RCVBUFFORCE.

For ordinary SO_RCVBUF requests, `net.core.rmem_max` caps the requested amount. Linux doubles the accepted amount for kernel bookkeeping and returns that doubled value from getsockopt. A successful setsockopt call therefore does not prove that the original requested size was accepted without capping. [Linux socket documentation](https://man7.org/linux/man-pages/man7/socket.7.html) describes this convention.

Read-only checks on the collector host (replace ports if needed):

```sh
/usr/sbin/sysctl net.core.rmem_max net.core.rmem_default
ss -u -a -n -m '( sport = :2055 or sport = :2056 or sport = :2057 )'
```

In `skmem`, `rb` is the socket's effective receive-memory limit; `r` is current receive-memory allocation and `d` is the socket drop count. `rb` is not the application's original request or a guaranteed UDP payload capacity. Compare config, rmem_max, diagnostics and ss together, in the receiver's network namespace.

A larger buffer can absorb short bursts; it cannot fix sustained processing overload. If socket drops occur, investigate CPU, scheduling and application queue pressure first. Raising `net.core.rmem_max` is an **optional system tuning step**, not an installation prerequisite. For example, an operator choosing a 1MiB application request can allow it with:

```sh
# Optional: changes the host-wide cap; do not apply without checking its need.
sudo /usr/sbin/sysctl -w net.core.rmem_max=1048576
```

Persist a deliberately chosen cap in a dedicated /etc/sysctl.d file only if needed. Changing the cap alone does not resize existing sockets: the application request must be applied when sockets are recreated. Plan any restart separately, then verify the actual result and drops. AS-stat-BE applies the explicitly configured per-socket request; it does not change host-wide sysctl settings.
