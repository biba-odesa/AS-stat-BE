# Configuration

The sole configuration argument is `nf9-receiver --config FILE`. `--check-config` validates syntax, mappings, limits and readable knownlinks without binding UDP, writing output or locking spool. Errors report line numbers. Format: `key = value`, whitespace, empty lines and whole-line `#` comments. No inline comments or general configuration language.

See config/asstat.conf.example and config/knownlinks.example. All example addresses use the documentation prefix 192.0.2.0/24. Replace them with locally assigned bind/exporter addresses before operation. Ports are unique (1–16 listeners), each mapped to exactly one allowlisted exporter. UDP source ports are unrestricted. No SO_REUSEPORT, protocol autodetection or sFlow. Transport is IPv4; records may be IPv4 or IPv6.

Required keys: netflow9_ports, bind_address (specific unicast IPv4), exporters, samplerate (positive uint64 per exporter), exported_counters (sampled/already_scaled per exporter), knownlinks_file, replace_asn (uint32/none), private_asn_ranges (inclusive uint32 pairs) and exclude_asn (comma-separated uint32). Sampling policy never defaults silently. Unknown/repeated keys, extra/missing mappings, invalid/reversed ranges and conflicting endpoints fail validation.

Template cache identity includes configured source, exporter IP, Source ID and template ID; TTL is from valid template refresh using monotonic time, not data use. Sequence/uptime decrease does not automatically reset cache. After exporter restart, an old schema may persist until refresh or expiry.

Receive queue count and byte limits apply per source, with a shared configuration budget; overflow drops new datagrams. `socket_receive_buffer_bytes=0` keeps the kernel default; see [UDP receive buffer](#udp-receive-buffer) for requests, kernel limits and verification. Kernel drops, receive-queue drops and unknown templates are distinct diagnostics.

`close_delay_seconds`, `max_active_windows`, `max_active_keys` bound global aggregation. `writer_queue_records/bytes` bound optional JSONL output. `windows_output=none` disables detailed JSONL while delivery_enabled=true keeps aggregation enabled. `duration=0` runs until signal. `warmup` affects diagnostics only; `report_interval` controls stderr summaries. Diagnostic output is bounded and may skip summaries under pressure.

`output` and non-none `windows_output` must be new files; existing targets are not overwritten. The provided launcher creates a unique report directory per start and overrides --output. It retains at most 32 run directories, which limits count, not a strict byte quota. Operational logs should also have a bounded journald policy.

Delivery keys in the example configure URL, spool path, spool byte/file limits, RAM queue count/bytes, batch count/bytes, HTTP timeout, capped exponential retry delays and total shutdown budget. Only local `http://127.0.0.1:PORT` is supported; HTTP proxy is bypassed. Spool must be receiver-owned and not group/world writable. Keep it separate from VM storage. The public copy rejects the documented default /var/lib/victoriametrics storage path; this is not a generic detector of all possible VM data directories. Do not use another VM storage location as spool.

The sample limits are 1GiB/8192 spool files, 512 records/128KiB per HTTP batch, 3s HTTP timeout, retry 1–60s and 10s delivery shutdown. With archive sections, retention expiry uses each archive retention_days; old timestamps are not moved forward. Corrupt/unfinished/expired/permanent-error files consume spool space and need deliberate operator handling. Do not delete unconfirmed batches automatically. Reducing the batch limit below existing batch sizes can prevent recovery and requires inspection.

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


## Archive sections

Global settings must precede `[archive NAME]` sections. See the complete checked [example](../config/asstat.conf.example). Each section requires exactly `url`, `interval_seconds`, and `retention_days`. Names must start with an ASCII letter/digit, contain only ASCII letters, digits, underscores or hyphens, and be at most 32 characters. At most 16 archives are allowed. Intervals are 60–86400 seconds, divisible by 60; retention is 1–36500 days. URLs are distinct `http://127.0.0.1:PORT` endpoints; do not mix resolutions in a database. Unknown/repeated keys or sections fail. Fully commented sections are inactive.

| Example archive | Interval | Delivery age | Example VM retention | Port |
| --- | ---: | ---: | --- | ---: |
| minute | 60 s | 1 day | 1d | 8428 |
| week | 300 s | 7 days | 7d | 8429 |
| month | 1800 s | 31 days | 31d | 8430 |
| year | 7200 s | 370 days | 370d | 8431 |
| four_years (commented) | 14400 s | 1461 days | 1461d | 8432 |

These names are examples, not special modes. `interval_seconds` controls actual temporal summation. `retention_days` controls eligible delivery age, not VM deletion. Runtime VM retention is discovered asynchronously: a longer value is allowed, a shorter value blocks that archive's delivery with diagnostics, and temporary unavailability does not prevent collection. Configure VM retention at least as long as delivery age. Do not automatically shorten an existing minute database: historical availability and bootstrap needs must be assessed separately.

`archive_state_directory` contains separate receiver-owned directories and SQLite files per archive. `archive_state_max_bytes` bounds database pages, divided across N archives plus one reference share; allow additional filesystem space for rollback journals, backups and spool. It is not an overall disk quota. `archive_queue_bytes` is divided across archives; `archive_queue_minutes` bounds each channel's count. `archive_max_keys` and `archive_outbox_rows` are divided across archives. Global spool byte/file and delivery queue budgets are also divided. The example uses 2 GiB state, 64 MiB archive queues, eight queued minutes/channel and one million total keys/outbox rows. These are bounded example budgets, not a sizing promise for every load.

Saved URL/interval fingerprints cannot change. Registry validation inspects existing state read-only; config changes require restart. Increasing archive count reduces per-archive shares: check existing page counts, spool batches and memory budgets before adding sections. A database whose allocated page count exceeds its new share may require a larger justified aggregate budget; do not delete or reset state to bypass the check. See [archive administration](archives.md) for initialization, bootstrap and migration.

During catch-up, a full outbox leaves the next immutable minute in durable inbox. `outbox_backpressure` counts observations, not lost minutes. Storage errors, rejected input, queue loss, expiry, corrupt batches, HTTP errors and source drops are distinct diagnostics. Detailed window JSONL is optional; `windows_output=none` keeps aggregation and delivery enabled. Partial/gap windows are not delivered as complete intervals.
