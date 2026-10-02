# Minute queries

Metric: asstat_traffic_bytes, complete-minute byte volume with minute-start timestamps. A window at A represents [A,A+60s), not an observation of a cumulative counter. Sampling/ASN policy is already applied; do not multiply again.

Use actual raw points as the reference, via /api/v1/export with match[]=asstat_traffic_bytes and explicit start/end. Do not use reduce_mem_usage=1: it disables deduplication of recent export data. Export end is inclusive; for [A,B) use B-0.001 seconds. Chunk large exports rather than requesting all history.

A graphical selector can repeat its previous sample between sparse points. To show only actual minute starts, query_range with UTC minute-aligned start/end, step=60 and:

```promql
sum_over_time(asstat_traffic_bytes{asn="64496",direction="in",ip_version="4"}[1ms])
```

Each link remains a separate series. Change direction/family to obtain the other panels. The 1ms window at the exact sample timestamp prevents lookback substitution. Missing minutes remain absent.

For interval [A,B), let D=B-A seconds and evaluate at B-0.001. Example D=3600:

```promql
topk(10, sum by (asn) (
  sum_over_time(asstat_traffic_bytes{link_id="link-a"}[3600s])
))
```

For a group:

```promql
topk(10, sum by (asn) (
  sum_over_time(asstat_traffic_bytes{link_id=~"link-(a|b)"}[3600s])
))
```

Add direction/ip_version filters when separate rankings are required. A group sum may count the same traffic on multiple links; it is not unique network volume. Results are volume in bytes, not instantaneous rank by a sparse graphical sample.

Minute average bit/s:

```promql
sum_over_time(asstat_traffic_bytes{asn="64496",direction="in",ip_version="4"}[1ms]) * 8 / 60
```

Do not use rate()/increase() on this gauge. Do not sum filled graphical points to calculate volume. Only full closed minutes are stored: no implicit zero series and no partial-minute normalization. These window formulations were checked against independent calculations during the project's local validation; no operational datasets are published.
