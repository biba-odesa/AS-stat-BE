# Archive initialization, bootstrap and backfill

Run worker administration as the receiver owner, with the receiver stopped when operating on an existing journal. Never reset existing state, alter its cursor with ad-hoc SQL, or delete unacknowledged spool. Keep state, configuration and immutable migration work together in backups. Endpoint/interval changes in saved state are rejected.

## New installation without history

Create the state root and a 0750 receiver-owned child directory for every configured archive. Choose one actual UTC minute boundary, then initialize only absent databases:

```sh
FIRST_MINUTE=$(python3 -c 'import time; print(int(time.time())//60*60)')
for archive in minute week month year; do
  sudo -u asstat /opt/as-stat-be/bin/nf9-archive-worker --init-current /etc/as-stat-be/asstat.conf "$archive" "$FIRST_MINUTE"
done
```

The names above match the example only. Do not rerun initialization over existing databases: it deliberately fails. Startup makes the interrupted starting minute partial and proceeds from preserved state. This current-only procedure does not backfill an unavailable prefix.

## Additional archive, including four_years

1. Provide its separate local VM endpoint, storage and retention at least 1461d with dedup=1s for the commented example. Uncomment its section only when that endpoint is intended. Review aggregate budgets: adding an archive reduces shares. Run --check-config as the receiver user. Runtime reload is not supported.
2. For current-only operation, stop the receiver, create only the new 0750 directory, and run --init-current for only the new archive with the current minute boundary. Restart; existing archives/windows are not reinitialized. This is the simple default when historical filling is not required.
3. For optional historical bootstrap instead, choose a retained source-minute start and a new migration work directory. Create only the new archive directory and run:

```sh
sudo -u asstat /opt/as-stat-be/bin/nf9-archive-worker --bootstrap /etc/as-stat-be/asstat.conf four_years FIRST_MINUTE_EPOCH
```

FIRST_MINUTE_EPOCH is a real minute-aligned UTC epoch value. Start/restart the receiver: the new journal records its own finite history/live boundary; live minutes durably wait behind history. Existing journals retain their cursors and accumulated windows. No endpoint may receive separate competing backfill and live sums for the same timestamp.

## Optional migration tool and limitations

The standard-library tool uses bounded per-minute deduplicated raw export, never a filled graphical selector or reduce_mem_usage=1. It stores an immutable contract, jobs, expected sums and progress in a separate SQLite reference. Native journal workers do the actual aggregation/delivery. Historical output stays behind a validation gate until compared against the independent reference and existing destination. Conflicting destination values abort; retries do not POST just to check visibility. One flock owner per work directory prevents concurrent migration.

Create a coverage JSON with `complete_intervals` containing proven complete `[start_epoch,end_epoch)` collection periods. Unknown coverage and edge windows are partial; absence of ASN rows is not itself incomplete coverage. Run as the receiver owner:

```sh
python3 scripts/archive_backfill.py --config /etc/as-stat-be/asstat.conf \
  --archives four_years --source-url http://127.0.0.1:8428 \
  --start FIRST_MINUTE_EPOCH --coverage COVERAGE_JSON --work NEW_REFERENCE_DIRECTORY
```

Do not replace these placeholders with guessed coverage. Resume with the same config/contract/start/work directory and preserved boundary. Observe actual cursor movement, inbox growth, page budget and independent verification; an idle timeout is not proof of network loss. Historical/live boundary-crossing windows are handled by the same ordered journal, not independently imported halves. Natural full live windows need separate read-only verification with archive_live_check.py; check its bounded deadline and result statuses. The helper selects the first full interval after the supplied boundary, allows four hours after the latest window end, and refuses a schedule more than eight hours ahead. For longer intervals, run a deliberate bounded raw comparison near closure rather than assuming this helper can wait indefinitely.

History is limited to retained raw minute points. Four years of arbitrary detailed history cannot be reconstructed from a short minute retention or larger aggregate sums. Source retention discovery currently requires the source flags to express integer days; alternate flag representations are rejected. Values read from VM reflect its numeric representation, not necessarily original uint128 values. Do not shorten source retention until a deliberately attempted migration is verified. Long backfills can accumulate live inbox faster than their page budget; monitor free space and budgets including transaction duplication and rollback journals. Real long-duration historical transfer is not claimed universally completed or benchmarked by this release. Synthetic interrupted-resume/gate tests are distinct from production-scale validation.

## Cancellation and exceptional recovery

Placing `cancelled.json` in a migration work directory prevents further historical tool processing while preserving reference/jobs. Stop the migration process first. The offline --abandon-history CONFIG ARCHIVE SAVED_BOUNDARY operation records an audited historical gap to the immutable saved boundary, without directly changing the cursor. It refuses pending historical inbox entries; inspect them rather than dropping them. Subsequent normal ingest passes the gap as partial coverage, preserving live inbox and existing outbox. Prepared historical batches remain gated until independently validated; cancellation does not authorize conflicting values. This is an administrative operation requiring deliberate inspection, not an automatic timeout policy.

`archive_recover_minutes.py` is an exceptional repair tool for only absent, unprocessed minutes. It requires preserved pre-restart diagnostics and either identical immutable peer inbox or confirmed raw-minute completeness/delivery evidence. It records input hashes/acknowledgements before stage, rejects conflicts and does not resample. It is not general reconstruction from arbitrary storage and is disabled by the cancellation marker. No operational datasets or migration state ship with this project.
