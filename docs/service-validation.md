# Permanent deployment validation

The permanent launcher, monitoring units and optional tmpfs guards are covered by synthetic checks. No example units are installed or started by these checks, and no operational configuration or service needs to change.

## Checks for this revision

```sh
ctest --test-dir build-release --output-on-failure -R 'service-runtime|archive-config|archive-journal|archive-boundary|app-config|config-integration|archive-recovery-tools'
git diff --check
```

All seven selected CTest entries passed. The runtime suite checks missing/wrong/read-only mounts, safe directory creation and content preservation, and symlink rejection. Its optional real-tmpfs bootstrap/restart/corruption integration case is skipped unless `ASSTAT_TEST_TMPFS` explicitly names an isolated test mount. That optional case was not rerun for this documentation publication; production tmpfs is not a test directory.

The example configuration passed `nf9-receiver --config EXAMPLE --check-config` using a temporary copy with only the knownlinks path redirected to the included synthetic example. No UDP listener was started. README, Debian and tmpfs shell blocks passed `sh -n`.

`systemd-analyze verify` checked the permanent collector, monitor service/timer, both combined tmpfs drop-ins and all four VM units plus the compatibility minute unit. The documented VM binary is intentionally not installed at `/usr/local/bin` on the validation host: verification used temporary unit copies substituting `/usr/bin/true` for that executable path only. No VM was executed. Separate assertions verified all four original storage paths, loopback ports, retention values, dedup setting and service user. The collector and monitor examples themselves required no executable-path substitution. This validates syntax and consistency, not target-host installation, mount availability or VM durability.

Disk-backed and tmpfs procedures share the same receiver/helpers and archive configuration. The launcher uses unique final report names, bounded logs/history, a 75-second drain watchdog and a 90-second unit stop budget. The optional RAM guard runs before initialization; existing state is checked rather than silently reset. Loss boundaries and migration precautions are documented in [tmpfs deployment](tmpfs.md).

Publication review excludes operational configuration, captures, metrics, SQLite/reference/spool, logs and backups. License, attribution and the separate web repository link are preserved. No live test, service restart or historical migration is part of this publication check.
