# Archive architecture

UDP receive and decode stay separated by bounded per-source queues. Decoder counters remain raw. Independent flow-side accounting applies the configured sampling and ASN policy once, then merges all sources into final global UTC minutes keyed by link_id/ASN/direction/family. Source IDs are diagnostic identities, not statistics labels; similar records from different Source IDs are not deduplicated.

Each configured archive receives those same immutable final minutes directly. There is no cascade from one database to another. A dedicated worker owns each journal and sender; a failed endpoint does not stall other archives or UDP. Filesystem work and HTTP are outside the receive thread. Archive channels are bounded by both payload bytes and minute count. Overflow is explicitly diagnosed as loss; continuous UDP collection is not a durable packet queue.

## State and commit boundaries

Each archive's SQLite database contains immutable minute inbox, processing cursor, bounded recent canonical receipts, current-window totals, partial/gap coverage, immutable closed outbox and historical validation gates. A transaction atomically adds a minute, advances its cursor and closes any completed interval. Reprocessing recent identical input is a no-op; conflicting input fails. Cursor and inbox consumption are committed together. Checked uint128 arithmetic prevents silent overflow. SQLite uses DELETE journaling and synchronous FULL; database and journal live outside the removable delivery spool. Exclusive locks prevent concurrent owners. State fingerprints prevent accidental reuse at a different resolution.

A closed outbox is handed off in bounded pieces to its own spool. The spool file is written, fsynced, atomically published and its directory fsynced before the corresponding output rows are acknowledged/deleted from SQLite. A crash between publication and acknowledgement can repeat identical points, never different partial sums. HTTP delivery retries exactly the same immutable labels, timestamp and value. Only an API acknowledgement permits spool removal, followed by directory fsync. An ambiguous response retains the party. Permanent errors are retained and backed off, not treated as success or silently discarded. Corrupt, unfinished and expired files require deliberate inspection.

These guarantees assume the filesystem and storage honor fsync. RAM receive/aggregation/channel data before durable journal commit and an unclosed base minute can be lost on a receiver crash. HTTP ACK is API acceptance, not proof of VictoriaMetrics crash durability. VM deduplication (1s) handles identical retries; it does not sum pieces. SQLite durability on one disk is not replication or a backup. Stop timeout bounds drain; already durable pending data survives for restart. Local HTTP explicitly bypasses external proxies.

## Windows and gaps

An interval is `[floor(minute/I)*I, floor(minute/I)*I+I)` relative to Unix epoch. Only complete closed intervals are sent. Missing ASN keys do not create zeros and do not make a minute partial. Known partial minutes, administrative gaps and detected receive interruptions make intersecting coarse windows partial. No traffic cannot be inferred from absent UDP or sampled records. Timed closure works without new packets. Late data after base-minute closure is diagnosed and never shifted into a later minute. The system cannot identify every otherwise undetected collection outage.

Startup/restart retains accumulated coarse windows and excludes known interruption coverage. Two partial fragments of the same restart minute remain partial, never a manufactured full minute. Sampling/options and policy are not reapplied during archive replay. A large database file may retain free pages after catch-up; page reuse is distinct from physical file shrinkage.

## Delivery and diagnostics

Each endpoint stores the same gauge `asstat_traffic_bytes` and labels link_id/asn/direction/ip_version. Values are byte sums for that endpoint's interval, timestamps its UTC start in milliseconds. Resolution is selected by endpoint, not an extra label. Do not combine endpoints as if they contained unique disjoint traffic. VM numeric representation cannot exactly preserve every uint128; independent checks compare actual raw stored values and report rounding rather than assuming integer precision.

Diagnostics separate socket drops, channel losses, unknown templates, journal/stage errors, outbox backpressure, HTTP/disk errors, spool age/bytes/batches and expiry. No routine raw export runs every minute. Bounded snapshots may be skipped under diagnostic pressure. See [configuration](configuration.md), [installation](installation.md), [archive administration](archives.md) and [queries](queries.md).
