# ice-keeper

[Apache Iceberg](https://iceberg.apache.org/) tables require regular maintenance. This may be unexpected for many people that are new to Iceberg-based data architecture.

There are three good reasons:

Iceberg unlocks background updates. Iceberg solves the problem of coordinating multiple writers safely. That enables problems to be broken down into simpler and more reliable pieces. Before, writers had to balance making data available quickly (frequent writes) with the performance problems of small files, and would ideally also cluster data for downstream consumption. With Iceberg, a streaming writer can make data available quickly and a background maintenance task can cluster and compact for long-term performance.

Iceberg uses an optimistic approach. Writers create parallel snapshots of a table and use an atomic swap to switch between them. Old snapshots must be kept around until readers are no longer using them. The downside of this model is that snapshots need to be cleaned up later, or else old data files might accumulate indefinitely.

In short, table maintenance is unavoidable in modern formats and, in many cases, breaking work down into separate writes and data maintenance is a better operational pattern.

These are the most common operations that are needed to keep tables performant and cost-effective with minimal effort:

- Data compaction asynchronously rewrites data files to fix the small files problem, but can also cluster data to improve query performance and remove rows that have been soft-deleted.
- Snapshot expiration removes old snapshots and deletes data files that are no longer needed.
- Orphan file cleanup identifies and deletes data files that were written but never committed because of job failures.

ice-keeper is a service that automates Iceberg table maintenance. ice-keeper is typically scheduled to run every night in Airflow, but it can be scheduled in your favorite scheduler (e.g., Airflow, Dagster, cron, or any other orchestration tool).

ice-keeper can:
- expire old snapshots
- find and remove orphan files (not tracked by Iceberg), leveraging a storage inventory report to greatly speed up the process
- remove empty folders left behind after orphan file cleanup
- run an optimization on unhealthy partitions to improve search performance
- apply lifecycle policies to automatically delete old data based on a configurable retention period


## Overview

Table owners control ice-keeper entirely through Iceberg table properties. Here is a summary of the key capabilities:

- **Snapshot expiration** — Enabled by default. ice-keeper expires old snapshots to keep metadata small and reclaim storage. You control the retention window (in days) and the minimum number of snapshots to keep.
- **Orphan file removal** — Enabled by default. ice-keeper identifies and deletes data files that were written but never committed (e.g., due to job failures). It leverages a storage inventory report to greatly speed up this process. Empty folders left behind are also cleaned up.
- **Optimization** — Opt-in. You choose whether to optimize your table and which strategy to use: `binpack` (compact small files), `sort` (compact and sort by specified columns), or `zorder` (compact and Z-order by specified columns). You control the diagnostic window by specifying how many recent partitions to skip (`min-partition-to-optimize`) and how far back to look (`max-partition-to-optimize`). Within this window, ice-keeper evaluates the health of each partition and only optimizes those that actually need it — not every partition in the window is rewritten. The target file size can be set explicitly or left on automatic (recommended), which sizes files per partition for optimal performance.
- **Lifecycle management** — Opt-in. ice-keeper can automatically delete rows older than a configurable retention period based on an ingestion time column, helping manage storage costs for tables with time-bound data.
- **Manifest rewriting** — Opt-in. ice-keeper can rewrite manifest files to improve query planning performance.
- **Partition widening** — Opt-in. ice-keeper can widen partitions (e.g., from daily to monthly) for older data to reduce the number of small partitions.

## Configuring ice-keeper via Table Properties

Table owners can control what ice-keeper will do to their table. They can opt-in, opt-out and generally configure all aspect of the automated table maintenance. ice-keeper settings are managed using Iceberg table properties.

Table owners can set table properties using the sql call
```sql
alter table my_catalog.my_schema.my_tableset set tblproperties (
    'ice-keeper.notification-email'='my-email@domain.gc.ca'
)
```

See the Iceberg [documentation](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table-set-tblproperties) for more details.  The table below lists all configurations available for ice-keeper.


| Table Property                                 | Default Value      | Description                |
| ---------------------------------------------- | ------------------ | -------------------------- |
| ice-keeper.notification-email                  | None               | Specifies an email address to receive notifications in case of failures. This property ensures alerts are sent to the configured email when maintenance actions encounter issues or errors.
| ice-keeper.should-expire-snapshots             | true               | Determines if a table should participate in ice-keeper's snapshot expiration process.
| ice-keeper.retention-days-snapshots            | 7                  | Defines the number of days for which snapshots should be retained.
| history.expire.max-snapshot-age-ms             | 604800000 (7 days) | This is a native Iceberg configuration where millisecond resolution is used. However, ice-keeper rounds it down to the nearest day. It is recommended to use `ice-keeper.retention-days-snapshots` instead of this property for easier management.
| ice-keeper.retention-num-snapshots             | 1                  | Defines the minimum number of snapshots to retain. If `history.expire.min-snapshots-to-keep` is also set, `ice-keeper.retention-num-snapshots` takes precedence.
| history.expire.min-snapshots-to-keep           | (Iceberg default)  | This is a native Iceberg configuration. If `ice-keeper.retention-num-snapshots` is also set, the ice-keeper property takes precedence.
| ice-keeper.should-remove-orphan-files          | true               | Determines if a table should undergo ice-keeper's orphan file removal process.
| ice-keeper.retention-days-orphan-files         | 5                  | Indicates that orphan files less than a specified number of days old should not be deleted.
| ice-keeper.should-optimize                     | false              | Indicates if a table should be optimized using binpack, sort, or Z-order strategies.
| ice-keeper.min-partition-to-optimize            | 1d                 | Minimum partition-age offset to include in optimization. Format: `<int><unit>` where unit is `h` (hour), `d` (day), `m` (month), or `y` (year), case-insensitive (e.g., `1d`, `1D`, `3M`, `3m` all work). `min` and `max` must use the same unit.
| ice-keeper.max-partition-to-optimize            | 7d                 | Maximum partition-age offset to include in optimization, using the same format and unit as `ice-keeper.min-partition-to-optimize`. The window is inclusive on both ends (`>= current - max` and `<= current - min`). Must be greater than or equal to `min`.
| ice-keeper.optimization-strategy               | None               | Defines the optimization strategy. Set to `binpack` for file compaction without reordering. For sort-based optimization, specify a comma-separated list of sort columns (e.g., `id desc, action asc`). For Z-order sorting, use `zorder(col1, col2)` (e.g., `zorder(src_ip, dst_ip)`). Sort and zorder values are passed directly to the `rewrite_data_files` procedure's `sort_order` parameter; for binpack, no `sort_order` is used.
| ice-keeper.optimize-partition-depth            | -1 (dynamic grouping) | Controls how many partition levels are used when grouping partitions for optimization. The default value of `-1` enables dynamic grouping, which automatically bundles sub-partitions into groups up to `ice-keeper.optimization-grouping-size-bytes` for a single `rewrite_data_files` call. For example, given a table partitioned by `days(event_time), event_type`: at **depth=1**, ice-keeper diagnoses individual sub-partitions but groups the optimization by the first level only, issuing compaction jobs with a WHERE clause like `days(event_time) = '2024-01-01'` (all event types in that day are rewritten together). At **depth=2**, each sub-partition is optimized independently with a WHERE clause like `days(event_time) = '2024-01-01' AND event_type = 'type1'`. Higher depth gives finer control but produces more optimization calls. For binpack this is often unnecessary since Iceberg already skips files that don't need compaction, but for sort (where `rewrite-all=true`), finer granularity avoids re-sorting partitions that are already sorted. Must be `-1` or a positive integer (1, 2, 3, ...); any other value (e.g. 0) raises an error.
| ice-keeper.optimization-grouping-size-bytes    | 17179869184 (16 GB) | When `ice-keeper.optimize-partition-depth` is set to `-1` (dynamic grouping), this controls the maximum combined size of sub-partitions that are grouped into a single `rewrite_data_files` call. Sub-partitions within the same partition age are accumulated until this threshold is reached, then a new group is started.
| ice-keeper.binpack-min-input-files             | 5                  | Minimum number of files targeted for rewrite in a partition before binpacking is triggered. A partition's `should_optimize` flag is set when `num_files_targetted_for_rewrite` exceeds this threshold (or when delete files are present).
| ice-keeper.sort-corr-threshold                 | -1 (disabled)      | **For debugging/testing only — should not be set by users in normal operation.** When set to a value >= 0, overrides the default correlation threshold used by all optimization strategies (sort: 0.97, binpack: 1.00, zorder: dynamic curve based on file count). A partition's `should_sort` flag is set when `corr < corr_threshold`. Set to `2.0` in integration tests to force `should_sort = true` with minimal data since `corr` maxes out at 1.0.
| ice-keeper.optimization-target-file-size-bytes  | -1 (auto)          | Specifies the target size for files when executing the optimization process through `rewrite_data_files`. The default value of `-1` enables automatic target file sizing per partition based on partition size (ranges from 16 MB to 1 GB). Set to a specific byte value (e.g., `536870912` for 512 MB) to use a fixed target size for all partitions. Automatic sizing **requires** `ice-keeper.optimize-partition-depth` to equal the number of partition levels in the table (so that each sub-partition is optimized independently) or to be `-1` (dynamic grouping, which already analyses each sub-partition individually). Note: the native Iceberg property `write.target-file-size-bytes` is **not** used by ice-keeper; if you want ice-keeper to optimize with the same target size as your write configuration, you must explicitly set this property.
| ice-keeper.should-rewrite-manifest             | false              | Determines whether ice-keeper should execute the `rewrite_manifest` procedure.
| ice-keeper.should-apply-lifecycle              | false              | Specifies whether `ice-keeper` should automatically delete rows with older data based on the configured retention policy defined by `ice-keeper.lifecycle-max-days`.
| ice-keeper.lifecycle-max-days                  | 330                | Defines the maximum number of days to retain data. Rows with a value in the specified ingestion time column older than `ice-keeper.lifecycle-max-days` will be deleted during lifecycle management.
| ice-keeper.lifecycle-ingestion-time-column     | None               | **Required when `ice-keeper.should-apply-lifecycle` is `true`.** Specifies the column to be used as the ingestion timestamp for lifecycle operations. The column must exist in the table schema. For example, when set to `ingestion_time`, ice-keeper deletes rows older than the retention period using a condition like `DELETE FROM table WHERE ingestion_time < current_date() - INTERVAL '330' DAY`. If left empty or set to a non-existent column, ice-keeper raises an error at runtime.
| ice-keeper.widening.rule.src.partition              | None          | The name of the source partition to be widened (e.g., `partition.timestamp_day`).
| ice-keeper.widening.rule.dst.partition              | None          | The name of the destination (widened) partition (e.g., `partition.timestamp_month`).
| ice-keeper.widening.rule.min-partition-to-widen     | 1M            | Minimum partition-age offset to start widening, using the same format as `ice-keeper.min-partition-to-optimize` (e.g., `1d`, `1M`). Partitions younger than this offset are skipped. The window is evaluated relative to `current_date()` / `current_timestamp()`, not the most recent partition.
| ice-keeper.widening.rule.max-partition-to-widen     | 2M            | Maximum partition-age offset to consider for widening, using the same format as `ice-keeper.max-partition-to-optimize` (e.g., `2M`, `1y`). The window is evaluated relative to `current_date()` / `current_timestamp()`, not the most recent partition.
| ice-keeper.widening.rule.select.criteria            | None          | Specifies the criteria for selecting rows used when widening.   (e.g., `partition.category in ('leading', 'lagging')`
| ice-keeper.widening.rule.required_partition_columns | None          | A list of column names that must not contain NULL values before the partition can be widened. Ensures data integrity. (e.g., `partition._lag`).
------

### Automatic Target File Sizing

When `ice-keeper.optimization-target-file-size-bytes` is set to `-1` (the default), ice-keeper automatically selects a target file size per partition based on the total data size of that partition. The target file size scales so that each partition ends up with at most N files of N MB each.

| Partition size         | Target file size | Max files per partition |
| ---------------------- | ---------------- | ---------------------- |
| < 256 MB               | 16 MB            | 16                     |
| < 1 GB                 | 32 MB            | 32                     |
| < 4 GB                 | 64 MB            | 64                     |
| < 16 GB                | 128 MB           | 128                    |
| < 64 GB                | 256 MB           | 256                    |
| < 256 GB               | 512 MB           | 512                    |
| ≥ 256 GB               | 1 GB             | —                      |

This requires `ice-keeper.optimize-partition-depth` to equal the number of partition levels in the table, or to be set to `-1` (dynamic grouping), so that each sub-partition is sized independently.

