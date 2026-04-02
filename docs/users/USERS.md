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

ice-keeper is a service that automates Iceberg table maintenance. ice-keeper is scheduled to run every night in Airflow.

ice-keeper can:
- expire old snapshots
- find and remove orphan files (not tracked by Iceberg)
- run an optimization on unhealthy partitions to improve search performance


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
| ice-keeper.retention-num-snapshots             | 60                 | Defines the minimum number of snapshots to retain.
| history.expire.min-snapshots-to-keep           | 60                 | This is a native Iceberg configuration, but it is advisable to use `ice-keeper.retention-num-snapshots` instead.
| ice-keeper.should-remove-orphan-files          | true               | Determines if a table should undergo ice-keeper's orphan file removal process.
| ice-keeper.retention-days-orphan-files         | 5                  | Indicates that orphan files less than a specified number of days old should not be deleted.
| ice-keeper.should-optimize                     | false              | Indicates if a table should be optimized using binpack, sort, or Z-order strategies.
| ice-keeper.min-age-to-optimize                 | -1 (deprecated)    | **Deprecated.** Use `ice-keeper.min-partition-to-optimize` instead. Number of recent partitions to ignore by the optimization. For example you might want to ignore the current hour or the current day. The current age is given index of 1, by setting `ice-keeper.min-age-to-optimize=2` you can skip the current hour or day.
| ice-keeper.max-age-to-optimize                 | -1 (deprecated)    | **Deprecated.** Use `ice-keeper.max-partition-to-optimize` instead. Number of partitions to consider optimizing. Note this will be the number of months, days or hours depending on the base time partition.
| ice-keeper.min-partition-to-optimize            | 1d                 | Minimum partition time offset (relative to the most recent partition) to start optimizing. Specified as a duration string with a numeric value and unit suffix: `d` (day), `h` (hour), `m` (month), `y` (year). For example, `1d` means skip the most recent day, `0m` means include the current month. The reference point is the most recent partition rounded down to the start of the unit (for example, using `date_trunc`), and offsets are applied by adding or subtracting whole units from that point.
| ice-keeper.max-partition-to-optimize            | 7d                 | Maximum partition time offset (relative to the most recent partition) to consider for optimization. Uses the same duration format and reference point semantics as `ice-keeper.min-partition-to-optimize`. For example, `7d` means consider partitions up to 7 days before the reference point.
| ice-keeper.optimization-strategy               | None               | Defines the optimization strategy to be used during the optimization process. It can include binpack, sort, or Z-order sorting. For example, a simple sort may be specified as: `id desc, action asc`, whereas a Z-order sort could be specified as `zorder(src_ip, dst_ip)`. These values are passed directly to the `rewrite_data_files` procedure’s `sort_order` parameter.
| ice-keeper.optimize-partition-depth            | 1                  | By default ice-keeper will analyse and optimize partitions at the first level (in a multi-leveled partitioned table). For example if the table is partitioned by `days(event_time), event_type` ice-keeper will diagnose each individual partitions during the diagnosis phase. Then if it detects any of these partitions need to be optimized it will issue an optimization for the base partition (level 1). When optimizing it will search for partitions to optimize and issue binpack/sort compaction jobs for each individual partitions e.g.: days(event_time) = '2024-01-01' and event_type = 'type1'. You can use this configuration to avoid re-optimizing partitions that do not need it. However, in the case of binpack it might not be necessary since Iceberg does a good job of figuring out if a partition's file need to be binpack. In the case of sort, ice-keeper sets rewrite-all=true so for these types of optimization you might want to consider using finer granularity. Set to `-1` to enable dynamic grouping, which automatically bundles sub-partitions into groups up to `ice-keeper.optimization-grouping-size-bytes` for a single `rewrite_data_files` call. Setting a value of 0 defaults to level=1.
| ice-keeper.optimization-grouping-size-bytes    | 16777216 (16 MB)   | When `ice-keeper.optimize-partition-depth` is set to `-1` (dynamic grouping), this controls the maximum combined size of sub-partitions that are grouped into a single `rewrite_data_files` call. Sub-partitions within the same partition age are accumulated until this threshold is reached, then a new group is started.
| ice-keeper.binpack-min-input-files             | 5                  | Minimum number of files targeted for rewrite in a partition before binpacking is triggered. A partition's `should_binpack` flag is set when `num_files_targetted_for_rewrite` exceeds this threshold (or when delete files are present).
| ice-keeper.optimization-target-file-size-bytes  | 536870912 (512 MB) | Specifies the target size for files when executing the optimization process through `rewrite_data_files`. Set to `-1` to enable automatic target file sizing per partition based on partition size (ranges from 16 MB to 1 GB). **Requires** `ice-keeper.optimize-partition-depth` to equal the number of partition levels in the table so that each sub-partition is optimized independently.
| write.target-file-size-bytes                   | 536870912 (512 MB)  | This is a native Iceberg property, but it can be overridden through the ice-keeper property `ice-keeper.optimization-target-file-size-bytes`.   
| ice-keeper.should-rewrite-manifest             | false              | Determines whether ice-keeper should execute the `rewrite_manifest` procedure.
| ice-keeper.should-apply-lifecycle              | false              | Specifies whether `ice-keeper` should automatically delete rows with older data based on the configured retention policy defined by `ice-keeper.lifecycle-max-days`.
| ice-keeper.lifecycle-max-days                  | 330                | Defines the maximum number of days to retain data. Rows with a value in the specified ingestion time column older than `ice-keeper.lifecycle-max-days` will be deleted during lifecycle management.
| ice-keeper.lifecycle-ingestion-time-column     | None               | Specifies the column to be used as the ingestion timestamp for lifecycle operations. For example, when set to `ingestion_time`, ice-keeper deletes rows older than the retention period using a condition like `DELETE FROM table WHERE ingestion_time < current_date() - INTERVAL '330' DAY`.
| ice-keeper.widening.rule.src.partition              | None          | The name of the source partition to be widened (e.g., `partition.timestamp_day`).
| ice-keeper.widening.rule.dst.partition              | None          | The name of the destination (widened) partition (e.g., `partition.timestamp_month`).
| ice-keeper.widening.rule.min.age.to.widen           | -1             | **Deprecated.** Use `ice-keeper.widening.rule.min-partition-to-widen` instead. The minimum age (in units of the destination partition) for data files to qualify for widening. 
| ice-keeper.widening.rule.min-partition-to-widen     | 1M            | Minimum partition time offset (relative to the most recent partition) to start widening. Uses the same duration format as `ice-keeper.min-partition-to-optimize` (e.g., `1d`, `0m`, `2m`).
| ice-keeper.widening.rule.max-partition-to-widen     | 2M            | Maximum partition time offset (relative to the most recent partition) to consider for widening. Uses the same duration format as `ice-keeper.min-partition-to-optimize` (e.g., `7d`, `12m`).
| ice-keeper.widening.rule.select.criteria            | None          | Specifies the criteria for selecting rows used when widening.   (e.g., `partition.category in ('leading', 'lagging')`
| ice-keeper.widening.rule.required_partition_columns | None          | A list of column names that must not contain NULL values before the partition can be widened. Ensures data integrity. (e.g., `partition._lag`).
------

