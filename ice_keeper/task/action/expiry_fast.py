import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from pyiceberg.table.refs import SnapshotRefType
from typing_extensions import override

from ice_keeper import Action, TimeProvider, escape_identifier
from ice_keeper.catalog import load_table
from ice_keeper.stm import STL
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class ExpireFastSnapshotsStrategy(ActionStrategy):
    """Fast snapshot trimming using Iceberg Java API by default.

    The default runtime path calls Iceberg's Java API via Spark JVM to expire
    snapshots with explicit retention controls. The original PyIceberg-based
    implementation is retained as a fallback path.

    With the default configuration, this strategy removes old snapshot
    references from table metadata without deleting underlying data files.
    Untracked files can later be cleaned up by running orphan-file maintenance.
    """

    # Default runtime mode for expire_fast.
    USE_JAVA_API = True
    # Java API allows explicitly controlling whether expired data files are deleted.
    JAVA_DELETE_EXPIRED_FILES = False

    @override
    @classmethod
    def get_action(cls) -> Action:
        return Action.EXPIRE_FAST_SNAPSHOTS

    @override
    def task_description(self, full_name: str) -> str:
        return f"Fast-expiring snapshots from table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determine whether the snapshot expiration task should be executed.

        The task is executed if:
        1. `mnt_props.should_expire_snapshots` is enabled (indicating that snapshot expiration is allowed).
        2. The table has been recently modified, as determined by `_is_table_recently_modified`.

        If either condition is not met:
        - Logs relevant information about why the action is being skipped.
        - Disables journaling to prevent unnecessary log entries.

        Returns:
            bool: True if the snapshot expiration action should be performed, False otherwise.
        """
        should_execute = False
        if self.mnt_props.should_expire_snapshots:
            dirty = self._is_table_recently_modified()
            if dirty:
                should_execute = True
            else:
                logger.debug("No recent changes found, skipping fast snapshot expiration for %s", self.mnt_props.full_name)
        else:
            logger.debug("Snapshot expiration is disabled, skipping %s", self.mnt_props.full_name)

        if not should_execute:
            self.disable_journaling()

        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Build a journal-friendly pseudo statement for this non-SQL action.

        The generic action framework expects every strategy to provide a
        statement string. That value is persisted in the journal (`sql_stm`)
        before execution so operators can audit what parameters were used.

        For SQL-based strategies this is executable SQL. For `expire_fast`,
        execution happens via PyIceberg APIs in `execute_statement`, so we
        return a descriptive call-like string instead of SQL.

        This keeps the framework behavior consistent while making journal
        records self-explanatory (`table`, `older_than`, `retain_last`, and
        metadata-only intent).
        """
        n_days = self.mnt_props.retention_days_snapshots
        older_than = TimeProvider.current_datetime() - timedelta(days=n_days)
        retain_last = max(1, self.mnt_props.retention_num_snapshots)
        if self.USE_JAVA_API:
            return (
                "java.expire_snapshots("
                f"table='{self.mnt_props.full_name}', "
                f"older_than='{older_than.isoformat()}', "
                f"retain_last={retain_last}, "
                f"delete_files={str(self.JAVA_DELETE_EXPIRED_FILES).lower()}"
                ")"
            )

        return (
            "pyiceberg.expire_snapshots("
            f"table='{self.mnt_props.full_name}', "
            f"older_than='{older_than.isoformat()}', "
            f"retain_last={retain_last}, "
            "delete_files=false"
            ")"
        )

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Execute fast snapshot expiration.

        By default, this strategy uses the Java Iceberg API via Py4J because it
        supports explicit control over deleting expired data files and keeps
        retention handling in the native API path.

        The original PyIceberg implementation is kept as a fallback helper to
        simplify rollback if needed.
        """
        if self.USE_JAVA_API:
            return self._execute_statement_with_java_api(sub_executor, sql_stm)
        return self._execute_statement_with_pyiceberg(sub_executor, sql_stm)

    def _execute_statement_with_java_api(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Expire snapshots using Iceberg's Java API through Spark JVM."""
        _ = sub_executor
        _ = sql_stm

        n_days = self.mnt_props.retention_days_snapshots
        older_than = TimeProvider.current_datetime() - timedelta(days=n_days)
        retain_last = max(1, self.mnt_props.retention_num_snapshots)

        identifier = (
            f"{escape_identifier(self.mnt_props.catalog)}."
            f"{escape_identifier(self.mnt_props.schema)}."
            f"{escape_identifier(self.mnt_props.table_name)}"
        )

        spark = STL.get()
        jvm = spark._jvm  # noqa: SLF001
        jspark_session = spark._jsparkSession  # noqa: SLF001
        if jvm is None or jspark_session is None:
            msg = "Spark JVM bridge is unavailable; cannot run Java expireSnapshots API"
            raise RuntimeError(msg)
        jtable = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jspark_session, identifier)

        # Java API keeps branch/tag protections and retention logic within the engine.
        jtable.expireSnapshots().expireOlderThan(int(older_than.timestamp() * 1000)).retainLast(
            int(retain_last)
        ).cleanExpiredFiles(self.JAVA_DELETE_EXPIRED_FILES).commit()

        logger.info(
            "Fast-expired snapshots from %s using Java API (delete_files=%s)",
            self.mnt_props.full_name,
            self.JAVA_DELETE_EXPIRED_FILES,
        )
        return {}

    def _execute_statement_with_pyiceberg(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Original PyIceberg implementation kept as fallback.

        The action is intentionally split into clear phases:

        1) Build retention boundaries from maintenance properties.
           - `older_than`: snapshots older than this UTC timestamp are eligible.
           - `retain_last`: minimum number of newest snapshots to keep.

        2) Load table metadata and compute protected snapshot ids.
           Branch/tag heads are always excluded from expiration.

        3) Emulate `retain_last` behavior.
           PyIceberg does not provide a direct `retain_last` option, so we keep
           the newest N snapshots by timestamp and remove them from candidates.

        4) Build final expiration candidates.
           A snapshot is expired only if it is:
           - older than `older_than`
           - not a protected branch/tag ref
           - not one of the newest `retain_last` snapshots

        5) Commit metadata changes.
           The commit trims snapshot metadata only. It does not delete data
           files; periodic orphan cleanup handles untracked files.

        The `sql_stm` argument is used by the task framework for journaling.
        Here we ignore it at runtime because this action executes through
        PyIceberg APIs instead of SQL procedures.
        """
        _ = sub_executor
        _ = sql_stm

        # Build expiration boundaries from table-level retention settings.
        n_days = self.mnt_props.retention_days_snapshots
        older_than = TimeProvider.current_datetime() - timedelta(days=n_days)
        retain_last = max(1, self.mnt_props.retention_num_snapshots)

        # Read current snapshot and ref metadata.
        table = load_table(self.mnt_props.catalog, self.mnt_props.schema, self.mnt_props.table_name)
        snapshots = list(table.metadata.snapshots)

        # Branch/tag heads are protected and cannot be expired.
        # The Spark `expire_snapshots` procedure applies this protection
        # internally. Here we call PyIceberg directly, so we enforce the same
        # safety rule explicitly before selecting candidate snapshot IDs.
        protected_ids = {
            ref.snapshot_id
            for ref in table.metadata.refs.values()
            if ref.snapshot_ref_type in (SnapshotRefType.BRANCH, SnapshotRefType.TAG)
        }

        # Emulate retain_last by pinning the newest N snapshots.
        retained_ids = {
            snapshot.snapshot_id for snapshot in sorted(snapshots, key=lambda item: item.timestamp_ms, reverse=True)[:retain_last]
        }

        # PyIceberg does not expose a direct retain_last/keep_last option.
        # We therefore build snapshot IDs explicitly so we can enforce both:
        # 1) older_than cutoff and 2) keep newest N snapshots.
        # Expire only snapshots that satisfy all eligibility conditions.
        snapshot_ids_to_expire = [
            snapshot.snapshot_id
            for snapshot in snapshots
            if datetime.fromtimestamp(snapshot.timestamp_ms / 1000, tz=timezone.utc) < older_than
            and snapshot.snapshot_id not in protected_ids
            and snapshot.snapshot_id not in retained_ids
        ]

        if snapshot_ids_to_expire:
            # Commit metadata update; data files are intentionally left intact.
            table.maintenance.expire_snapshots().by_ids(snapshot_ids_to_expire).commit()
            logger.info(
                "Fast-expired %d snapshots from %s using PyIceberg metadata-only expiration",
                len(snapshot_ids_to_expire),
                self.mnt_props.full_name,
            )
        else:
            logger.debug("No snapshots eligible for fast expiration on %s", self.mnt_props.full_name)

        # This operation updates metadata only and intentionally does not delete data files.
        return {}
