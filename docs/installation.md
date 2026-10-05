# Debian 13 installation

These are installation instructions for a new host, not commands to change an existing pilot. Run administrative commands as root or with sudo. Keep existing configs and spool on upgrades.

## Build and install collector

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends build-essential cmake git python3 libcurl4-openssl-dev ca-certificates curl
git clone https://github.com/biba-odesa/AS-stat-BE.git
cd AS-stat-BE
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
ctest --test-dir build-release --output-on-failure
sudo useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin asstat
sudo install -d -m 0755 /opt/as-stat-be/bin /etc/as-stat-be
sudo install -m 0755 build-release/nf9-receiver build-release/nf9-delivery-worker /opt/as-stat-be/bin/
sudo install -m 0755 scripts/asstat-run.py /opt/as-stat-be/bin/
sudo install -d -o asstat -g asstat -m 0750 /var/spool/asstat /var/lib/as-stat-be /var/lib/as-stat-be/reports
sudo install -o root -g asstat -m 0640 config/asstat.conf.example /etc/as-stat-be/asstat.conf
sudo install -o root -g asstat -m 0640 config/knownlinks.example /etc/as-stat-be/knownlinks
```

If the users/directories already exist, inspect them instead of blindly recreating or recursively changing ownership. nf9-delivery-worker must be beside nf9-receiver: the receiver locates and starts that mandatory helper itself. Diagnostic/file helpers are forked internally; no additional executable is needed. Runtime requires Python 3 and the system libcurl shared-library dependencies (libcurl4t64 on Debian 13). Optional offline binaries are nf9-replay and nf9-aggregate-replay.

Edit /etc/as-stat-be/asstat.conf and knownlinks before starting. Replace documentation IPs, actual interfaces/link IDs and ASN policy deliberately; sampling must reflect the exporter contract. Set delivery=true and windows_output=none for continuous collection. Limit spool to 1073741824 bytes; ensure free space beyond that for filesystem overhead and other services. Do not put spool inside VM storage. No privileged UDP ports are used in the example, so the receiver runs unprivileged.

```sh
sudo -u asstat /opt/as-stat-be/bin/nf9-receiver --config /etc/as-stat-be/asstat.conf --check-config
sudo install -m 0644 deploy/as-stat-be.service /etc/systemd/system/as-stat-be.service
sudo systemctl daemon-reload
sudo systemctl enable --now as-stat-be.service
systemctl status as-stat-be.service
journalctl -u as-stat-be.service -n 30 --no-pager
sudo systemctl stop as-stat-be.service
```

The unit does not depend on VM availability: the spool buffers temporary outages. It has no pilot deadline. KillMode=mixed first signals the receiver, allowing its helpers to drain; after the stop timeout systemd can kill the whole remaining group. The 45s timeout exceeds the example 10s delivery shutdown plus worker/diagnostic cleanup; increase it if you increase the delivery shutdown budget. SIGINT/SIGTERM drain receive queues and close finished windows; unfinished windows are partial and not delivered.

Health is the ready/periodic JSON in journal and final JSON in the unique report directory, not an invented receiver HTTP endpoint. Inspect per-source last receive, decoded counts, socket/queue drops, aggregate losses, spool pending/oldest/retries/disk/HTTP errors. Reports rotate by count (32 directories); configure bounded journald storage for the host and monitor free space. Corrupt/expired spool batches are preserved, not silently discarded.

For an update, build and test the new revision separately, stop only as-stat-be.service, install receiver and mandatory worker together, preserve /etc/as-stat-be and /var/spool/asstat, then start. Existing reports cannot cause O_EXCL restart failure: launcher chooses a new directory every time. Do not start a second sender on the same spool.

## Optional UDP receive-buffer tuning

The default setup does not require changing sysctl. `socket_receive_buffer_bytes` is a per-listener SO_RCVBUF request, subject to net.core.rmem_max; Linux reports a doubled accounting limit. Use the read-only sysctl/ss checks and conditional tuning guidance in [configuration](configuration.md#udp-receive-buffer). Larger buffers help with brief bursts, not sustained overload. The checks use procps (`sysctl`) and iproute2 (`ss`); if absent on a minimal Debian installation, those packages provide the inspection tools. Keep socket drops separate from application queue drops and unknown templates.

## VictoriaMetrics OSS single-node

Official references: [single-node documentation](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/) and [official releases](https://github.com/VictoriaMetrics/VictoriaMetrics/releases). Choose an explicit OSS release and architecture; do not download an enterprise or cluster archive. Match linux-amd64 to x86_64, linux-arm64 to aarch64. Check the selected release's actual asset names and published checksum before running these template commands.

```sh
# Set these to a chosen release and its matching published archive.
VM_VERSION=v1.153.0
VM_ARCH=amd64
VM_ARCHIVE="victoria-metrics-linux-${VM_ARCH}-${VM_VERSION}.tar.gz"
curl -fL "https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/${VM_VERSION}/${VM_ARCHIVE}" -o "$VM_ARCHIVE"
# This release publishes checksums for both archive and extracted binary.
VM_CHECKSUMS="victoria-metrics-linux-${VM_ARCH}-${VM_VERSION}_checksums.txt"
curl -fL "https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/${VM_VERSION}/${VM_CHECKSUMS}" -o "$VM_CHECKSUMS"
grep "  ${VM_ARCHIVE}$" "$VM_CHECKSUMS" | sha256sum -c -
tar -xzf "$VM_ARCHIVE" victoria-metrics-prod
grep "  victoria-metrics-prod$" "$VM_CHECKSUMS" | sha256sum -c -
./victoria-metrics-prod -version
./victoria-metrics-prod -help
sudo useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin victoriametrics
sudo install -m 0755 victoria-metrics-prod /usr/local/bin/victoria-metrics-prod
sudo install -d -o victoriametrics -g victoriametrics -m 0750 /var/lib/victoriametrics
sudo install -m 0644 deploy/victoriametrics.service /etc/systemd/system/victoriametrics.service
sudo systemctl daemon-reload
sudo systemctl enable --now victoriametrics.service
curl --noproxy '*' -fsS http://127.0.0.1:8428/health
curl --noproxy '*' -fsS http://127.0.0.1:8428/metrics
systemctl status victoriametrics.service
```

Verify flag metrics: retentionPeriod=7d, dedup.minScrapeInterval=1s and loopback listen address. `-version` prints binary version without starting a server. The aggregate interval is 60 seconds in AS-stat-BE; retention is seven days in VM; dedup removes identical deliveries, never adds portions. Existing storage is never deleted by these instructions. Set storage/backup/resource limits based on measured load, not a brief pilot extrapolation.

VMUI access without exposing the service publicly:

```sh
ssh -L 8428:127.0.0.1:8428 user@your-server
```

Open http://127.0.0.1:8428/vmui/ in the local browser. Keep the HTTP endpoint private; this example does not implement authentication or remote delivery. See queries.md for sparse-data-safe queries.
