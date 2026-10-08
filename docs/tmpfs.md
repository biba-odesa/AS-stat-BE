# Permanent service and optional volatile state

Disk-backed state and spool are the recommended default. `service_runtime.py` provides permanent launch, bounded diagnostics and minute resource monitoring. It has no pilot schedule, deadline or automatic end marker. The receiver, archive worker and delivery worker must be installed together; see [installation](installation.md).

The launcher creates missing service-owned directories with mode `0750`. Only a genuinely empty archive directory with an empty corresponding spool is initialized automatically using native `--init-current`, at the current UTC minute. It never starts historical backfill. The first partial minute and larger windows crossing a startup or collection gap are not emitted as full intervals. Existing state is checked and reused on ordinary restart. Corrupt, incompatible, unknown or incomplete saved state causes startup failure without deletion or automatic reset. Adding an archive to an existing installation still requires the explicit bootstrap procedure in [archives](archives.md).

Reports use unique names and retain at most 32 final reports. Diagnostic logs rotate; minute measurements retain at most 11,000 samples with a 128 MiB SQLite page limit. Keep the monitoring directory on disk if its history must survive reboot. The monitor follows receiver descendants, including archive and delivery workers, tracks run/PID changes, source rates, queues/errors, archive state/spool and filesystem capacity. VM storage traversal is hourly, not once per minute. For local VM storage owned by `victoriametrics`, the monitor can obtain read access through that group (`sudo usermod -a -G victoriametrics asstat` after the group exists); keep VM storage private and do not grant write access merely for monitoring. This is optional and is not a startup dependency. Adjust the group for custom deployments.

## Optional tmpfs mode: explicit loss after reboot

Putting **both SQLite state and spool in tmpfs** removes their block-device writes. It does not remove SQLite transactions or serialization. Keep SQLite `synchronous=FULL` and spool fsync calls enabled. On tmpfs those calls do not turn RAM into persistent storage. An ordinary service restart retains the mounted files and recovers inbox/cursor/outbox/spool. Reboot, unmount or host failure loses **all** state/spool files on that mount: unfinished large windows and pending batches cannot be recovered. Loss can span more than one archive interval when a backlog or endpoint outage has accumulated. Disk-backed mode preserves committed state, but an unclosed minute and RAM data before durable commit can still be lost. HTTP-acknowledged points already stored in VictoriaMetrics remain governed by VM's own disk durability. No history is automatically reconstructed from VM after reboot.

Before opting in, inspect existing `/etc/fstab`, mount configuration, ownership and swap. Do not create duplicate fstab entries. With swap enabled, tmpfs pages may reach swap storage; tmpfs then does not guarantee zero disk I/O or that bytes never reach disk. Disabling system swap is not required by this project.

For a dedicated 2 GiB mount, first obtain the service UID/GID:

```sh
id -u asstat
id -g asstat
sudo install -d -o asstat -g asstat -m 0750 /var/lib/asstat-ram
ASSTAT_UID=$(id -u asstat)
ASSTAT_GID=$(id -g asstat)
printf 'tmpfs /var/lib/asstat-ram tmpfs rw,nosuid,nodev,noexec,size=2G,mode=0750,uid=%s,gid=%s 0 0\n' "$ASSTAT_UID" "$ASSTAT_GID"
sudoedit /etc/fstab
```

Inspect `/etc/fstab` first. Add the exact line printed by `printf` only if this mountpoint has no existing entry; otherwise reconcile that entry. The numeric UID/GID come from the installed `asstat` account and are not portable between hosts. The resulting entry has this form; do not paste placeholders literally:

```fstab
tmpfs /var/lib/asstat-ram tmpfs rw,nosuid,nodev,noexec,size=2G,mode=0750,uid=SERVICE_UID,gid=SERVICE_GID 0 0
```

Mount the configured entry and inspect it:

```sh
sudo systemctl daemon-reload
sudo mount /var/lib/asstat-ram
findmnt -M /var/lib/asstat-ram -o TARGET,SOURCE,FSTYPE,OPTIONS
df -h /var/lib/asstat-ram
stat -c '%U:%G %a' /var/lib/asstat-ram
cat /proc/swaps
```

Change only the relevant global settings in the existing single config:

```ini
archive_state_directory = /var/lib/asstat-ram/archives
spool_directory = /var/lib/asstat-ram/spool
archive_state_max_bytes = 1073741824
spool_max_bytes = 268435456
```

For four archives, native SQLite page caps sum to about 0.8 GiB: the state budget is divided by archive count plus one. Reserving a further 0.8 GiB for worst-case rollback journals and 0.25 GiB spool leaves roughly 0.15 GiB for transient files and metadata in this example. This is a conservative planning budget, not a quota on journal files; monitor mount capacity and reassess when adding archives. Never assume `max_page_count` limits journals/WAL or that separate state/spool limits automatically fit a filesystem.

Install the optional drop-in:

```sh
sudo install -d -m 0755 /etc/systemd/system/asstat-be.service.d
sudo install -m 0644 deploy/asstat-be-tmpfs.conf /etc/systemd/system/asstat-be.service.d/tmpfs.conf
sudo install -d -m 0755 /etc/systemd/system/asstat-be-monitor.service.d
sudo install -m 0644 deploy/asstat-be-monitor-tmpfs.conf /etc/systemd/system/asstat-be-monitor.service.d/tmpfs.conf
sudo systemctl daemon-reload
```

`RequiresMountsFor` orders the service after the mount. Before any directory initialization or UDP launch, the launcher requires an exact, writable tmpfs mountpoint with service ownership and `0750`; absent/wrong/read-only mounts fail startup instead of writing into the underlying disk directory. The executable and all helpers remain on disk, outside the `noexec` tmpfs.

Do **not** switch an active disk-backed installation by just changing paths. Prepare config/units first, stop the receiver and wait for all helpers, back up config/units and make consistent SQLite backups with the SQLite backup API. Compact only backup copies, then check integrity and compare every table/cursor/totals/outbox. Copy every pending spool file to the same archive destination and verify its contents; preserve existing registry/endpoint identity. Keep the original disk state and a rollback plan. Start exactly one receiver against the migrated state and verify cursor progression/delivery before retiring the old service. Record the switch minute as partial. Never copy hot SQLite files or silently reinitialize a damaged archive.

## Control

```sh
systemctl status asstat-be.service asstat-be-monitor.timer
journalctl -u asstat-be.service -u asstat-be-monitor.service --since '1 hour ago'
cat /var/lib/as-stat-be/monitor/latest-sample.json
df -h /var/lib/asstat-ram
free -h
grep -E '^(MemAvailable|Shmem):' /proc/meminfo
cat /proc/swaps
systemctl show asstat-be.service -p MemoryCurrent -p CPUUsageNSec
sudo systemctl stop asstat-be.service
sudo systemctl start asstat-be.service
```

The 90-second systemd stop budget exceeds the launcher's 75-second drain watchdog and the example delivery shutdown budgets. `KillMode=mixed` signals the wrapper first, which asks the receiver to drain, and removes remaining cgroup helpers at the hard deadline. Reassess both limits if archive count or delivery shutdown timeout increases. Unit restart policy is `on-failure` with a five-second delay; a deliberate stop does not automatically restart it. Enabled service/timer resume on reboot, initialize a cleared tmpfs and preserve disk monitoring history. No end-date timer is installed.

`MemoryCurrent` covers the service cgroup and its helpers; use mount usage and host memory alongside it rather than adding potentially overlapping values. tmpfs has a capacity limit, not preallocated RAM. Check both available memory and filesystem capacity. Swap, if enabled, may introduce disk I/O; inspect it without globally disabling it.
