# Debian 13 installation

These are installation instructions for a new host, not commands to change an existing pilot. Run administrative commands as root or with sudo. Keep existing configs and spool on upgrades.

## Build and install collector

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends build-essential cmake git python3 libcurl4-openssl-dev libsqlite3-dev ca-certificates curl
git clone https://github.com/biba-odesa/AS-stat-BE.git
cd AS-stat-BE
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
ctest --test-dir build-release --output-on-failure
sudo useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin asstat
sudo install -d -m 0755 /opt/as-stat-be/bin /etc/as-stat-be
sudo install -m 0755 build-release/nf9-receiver build-release/nf9-delivery-worker build-release/nf9-archive-worker /opt/as-stat-be/bin/
sudo install -m 0755 scripts/service_runtime.py /opt/as-stat-be/bin/
sudo install -d -o asstat -g asstat -m 0750 /var/spool/asstat /var/lib/as-stat-be /var/lib/as-stat-be/reports /var/lib/as-stat-be/archives
sudo install -o root -g asstat -m 0640 config/asstat.conf.example /etc/as-stat-be/asstat.conf
sudo install -o root -g asstat -m 0640 config/knownlinks.example /etc/as-stat-be/knownlinks
```

If the users/directories already exist, inspect them instead of blindly recreating or recursively changing ownership. nf9-delivery-worker and nf9-archive-worker must be beside nf9-receiver: the receiver locates and starts that mandatory helper itself. Diagnostic/file helpers are forked internally; no additional diagnostic executable is needed. Runtime also uses Debian systemd/systemctl, util-linux (`findmnt`), and coreutils (`du`, `stat`, `df`); memory/swap inspection commands use procps (`free`). Runtime requires Python 3 and the system libcurl shared-library dependencies (libcurl4t64 and libsqlite3-0 on Debian 13). Optional offline binaries are nf9-replay and nf9-aggregate-replay.

The permanent launcher creates missing child state/spool directories and initializes only genuinely empty journals at the current UTC minute. Do not initialize existing state again. Direct native launch still needs the commands in [archive administration](archives.md#new-installation-without-history). Additional archives on an existing installation require explicit bootstrap. Keep collection stopped during state migration. Existing root-level legacy spool belongs to its original minute URL: inspect it and drain it with the old URL before changing archive layouts; never move it blindly into a different-resolution directory. Failed/unfinished/expired batches are retained for inspection.

Edit /etc/as-stat-be/asstat.conf and knownlinks before starting. Replace documentation IPs, actual interfaces/link IDs and ASN policy deliberately; sampling must reflect the exporter contract. Set delivery_enabled=true and windows_output=none for continuous collection. For disk-backed mode, limit spool to 1073741824 bytes; ensure free space beyond that for filesystem overhead and other services. Do not put spool inside VM storage. No privileged UDP ports are used in the example, so the receiver runs unprivileged.

### Choose state/spool storage before starting

**Disk-backed default:** keep `archive_state_directory = /var/lib/as-stat-be/archives` and `spool_directory = /var/spool/asstat` from the example. The launcher creates missing archive child directories with service ownership and mode `0750`, preserving existing state. Durable commits survive reboot subject to storage durability; the unclosed minute and RAM before commit remain vulnerable.

**Optional RAM-backed mode:** follow the complete [tmpfs setup](tmpfs.md), including the generated `/etc/fstab` entry for `/var/lib/asstat-ram` (`size=2G,nosuid,nodev,noexec`, actual `asstat` UID/GID), mount verification, configuration paths `/var/lib/asstat-ram/archives` and `/var/lib/asstat-ram/spool`, and both tmpfs drop-ins. Use the smaller example budgets documented there. Install drop-ins before the common start commands below. The launcher refuses an absent or wrong filesystem and automatically initializes genuinely empty state after reboot. Restart retains mounted files. Reboot loses unfinished windows and undelivered batches, possibly multiple intervals during backlog/outage; FULL/fsync does not make RAM persistent. Keep monitoring on disk and inspect RAM, tmpfs fill and swap. Migrate an existing installation only with collection stopped and verified SQLite backups/pending spool copies; never substitute empty state for damaged state.

### Install and control the permanent service

```sh
sudo -u asstat /opt/as-stat-be/bin/nf9-receiver --config /etc/as-stat-be/asstat.conf --check-config
sudo install -m 0644 deploy/asstat-be.service /etc/systemd/system/asstat-be.service
sudo install -d -o asstat -g asstat -m 0750 /var/lib/as-stat-be/monitor
sudo install -m 0644 deploy/asstat-be-monitor.service deploy/asstat-be-monitor.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now asstat-be.service asstat-be-monitor.timer
systemctl status asstat-be.service
journalctl -u asstat-be.service -n 30 --no-pager
sudo systemctl stop asstat-be.service
```

The unit does not depend on VM availability: the spool buffers temporary outages. It has no pilot deadline. `KillMode=mixed` first signals the launcher, which forwards shutdown to the receiver and allows helpers to drain. Its watchdog is 75 seconds; the systemd hard stop is 90 seconds and then removes remaining cgroup processes. The example includes four 10-second delivery shutdown budgets plus worker and receiver/diagnostic cleanup margins. Recalculate it for more archives or larger shutdown budgets. SIGINT/SIGTERM drain receive queues and close finished windows; unfinished windows are partial and not delivered.

Health comes from `/var/lib/as-stat-be/monitor/latest-diagnostic.json`, minute samples, rotating receiver logs and unique final report files; there is no receiver HTTP health endpoint. Journald shows launcher lifecycle and monitor failures. Inspect per-source last receive, decoded counts, socket/queue drops, aggregate losses, spool pending/oldest/retries/disk/HTTP errors. Final reports rotate by count (32 files); configure bounded journald storage for the host and monitor free space. Corrupt/expired spool batches are preserved, not silently discarded.

For an update, build and test the new revision separately, stop only asstat-be.service, install receiver, both mandatory workers and the matching launcher together, preserve /etc/as-stat-be, /var/lib/as-stat-be/archives and /var/spool/asstat, then start. Existing reports cannot cause O_EXCL restart failure: launcher chooses a unique final report filename every time. Do not start a second sender on the same spool.

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
sudo install -d -m 0755 /data/storage
sudo install -d -o victoriametrics -g victoriametrics -m 0750 /data/storage/victoriametrics
for archive in minute week month year; do
  sudo install -d -o victoriametrics -g victoriametrics -m 0750 "/data/storage/victoriametrics/$archive"
  sudo install -m 0644 "deploy/victoriametrics-$archive.service" "/etc/systemd/system/victoriametrics-$archive.service"
done
sudo systemctl daemon-reload
sudo systemctl enable --now victoriametrics-minute.service victoriametrics-week.service victoriametrics-month.service victoriametrics-year.service
curl --noproxy '*' -fsS http://127.0.0.1:8428/health
curl --noproxy '*' -fsS http://127.0.0.1:8428/metrics
systemctl status victoriametrics-minute.service victoriametrics-week.service victoriametrics-month.service victoriametrics-year.service
```

The complete unit examples use one verified `/usr/local/bin/victoria-metrics-prod` binary and separate directories under the consistently lowercase `/data/storage` root:

| Archive | Unit example | Loopback endpoint | Retention | Storage directory |
| --- | --- | --- | --- | --- |
| minute | [victoriametrics-minute.service](../deploy/victoriametrics-minute.service) | 127.0.0.1:8428 | 1d | /data/storage/victoriametrics/minute |
| week | [victoriametrics-week.service](../deploy/victoriametrics-week.service) | 127.0.0.1:8429 | 7d | /data/storage/victoriametrics/week |
| month | [victoriametrics-month.service](../deploy/victoriametrics-month.service) | 127.0.0.1:8430 | 31d | /data/storage/victoriametrics/month |
| year | [victoriametrics-year.service](../deploy/victoriametrics-year.service) | 127.0.0.1:8431 | 370d | /data/storage/victoriametrics/year |

All four run as `victoriametrics`, set `-dedup.minScrapeInterval=1s` and use bounded shutdown/restart settings. Inspect existing directory ownership before installation; do not rename or overwrite a running deployment's paths. The collector and VM accounts have separate state ownership.

Check /health and /flags separately on ports 8428–8431. The examples use retentionPeriod=1d/7d/31d/370d respectively, dedup.minScrapeInterval=1s and loopback listen addresses. `deploy/victoriametrics.service` is a compatibility copy of the minute unit: install either it or victoriametrics-minute.service, never both on port 8428. Four separate storage directories and processes are required; do not change existing database paths or shorten existing retention automatically. `-version` prints binary version without starting a server. The aggregate interval is configured in AS-stat-BE; retention is independently configured in each VM; dedup removes identical deliveries, never adds portions. Existing storage is never deleted by these instructions. Set storage/backup/resource limits based on measured load, not a brief pilot extrapolation.

VMUI access without exposing the service publicly:

```sh
ssh -L 8428:127.0.0.1:8428 user@your-server
```

Open http://127.0.0.1:8428/vmui/ in the local browser. Keep the HTTP endpoint private; this example does not implement authentication or remote delivery. See queries.md for sparse-data-safe queries.


Start the four VM instances before expecting delivery, but do not make the collector depend on their health. A VM outage must leave the receiver accepting data into bounded state/spool. The example parent storage directory and each child must be accessible to victoriametrics; collector state and spool belong to asstat (0750), config files can be root:asstat (0640). Reserve filesystem space beyond configured page/spool limits. Updates must install the three collector binaries from one tested build while its service is stopped, then start with unchanged state and spool. Do not overwrite running binaries piecemeal. Example units are not installed by the test suite.

The permanent launcher initializes only empty archives at the current minute and has no deadline. Monitoring retains bounded on-disk history and follows helpers. After creating the local `victoriametrics` account, optionally run `sudo usermod -a -G victoriametrics asstat` to grant the monitor read access to private VM storage. Without this access, CPU/RSS and archive diagnostics still work, but hourly storage-size samples report a permission error. Do not grant write access to VM data for monitoring. See [optional tmpfs mode](tmpfs.md) for mount guards, reboot losses, migration and budgets. Install the updated launcher together with matching receiver/helpers.

If replacing the previous `as-stat-be.service` example, stop and disable it before starting `asstat-be.service`; never run both receivers or both spool senders. Preserve its state and pending spool during migration.
