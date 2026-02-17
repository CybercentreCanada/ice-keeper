import dataclasses
import logging
import time
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Row

from ice_keeper import Action, Status, TimeProvider, get_user_name

from .schedule_entry import MaintenanceScheduleEntry

logger = logging.getLogger("ice-keeper")


MAX_STATUS_LENGTH = 1024 * 1024
"""Keep the length of stacktraces errors to a maximum of 1MB"""


@dataclass
class JournalEntry:
    full_name: str = ""
    catalog: str = ""
    schema: str = ""
    table_name: str = ""
    start_time: datetime = field(default_factory=TimeProvider.current_datetime)
    end_time: datetime = field(default_factory=TimeProvider.current_datetime)
    exec_time_seconds: float = 0.0
    sql_stm: str = ""
    status: str = ""
    status_details: str = ""
    executed_by: str = ""

    action: str = ""
    # rewrite_data_files
    rewritten_data_files_count: int = 0
    added_data_files_count: int = 0
    rewritten_bytes_count: int = 0
    failed_data_files_count: int = 0
    removed_delete_files_count: int = 0
    # expire_snapshots
    deleted_data_files_count: int = 0
    deleted_position_delete_files_count: int = 0
    deleted_equality_delete_files_count: int = 0
    deleted_manifest_files_count: int = 0
    deleted_manifest_lists_count: int = 0
    deleted_statistics_files_count: int = 0
    # rewrite_manifests
    rewritten_manifests_count: int = 0
    added_manifests_count: int = 0
    # orphan files
    num_orphan_files_deleted: int = 0
    # lifecycle
    lifecycle_deleted_data_files: int = 0
    lifecycle_deleted_records: int = 0
    lifecycle_changed_partition_count: int = 0

    @classmethod
    def get_ddl(cls) -> str:
        return """
            full_name STRING,
            catalog STRING,
            schema STRING,
            table_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            exec_time_seconds DOUBLE,
            sql_stm STRING,
            status STRING,
            status_details STRING,
            executed_by STRING,
            action STRING,
            rewritten_data_files_count BIGINT,
            added_data_files_count BIGINT,
            rewritten_bytes_count BIGINT,
            failed_data_files_count BIGINT,
            removed_delete_files_count BIGINT,
            deleted_data_files_count BIGINT,
            deleted_position_delete_files_count BIGINT,
            deleted_equality_delete_files_count BIGINT,
            deleted_manifest_files_count BIGINT,
            deleted_manifest_lists_count BIGINT,
            deleted_statistics_files_count BIGINT,
            rewritten_manifests_count BIGINT,
            added_manifests_count BIGINT,
            num_orphan_files_deleted BIGINT,
            lifecycle_deleted_data_files BIGINT,
            lifecycle_deleted_records BIGINT,
            lifecycle_changed_partition_count BIGINT
            """

    def is_success(self) -> bool:
        return self.status == Status.SUCCESS.value

    def is_failed(self) -> bool:
        return self.status == Status.FAILED.value

    def is_warning(self) -> bool:
        return self.status == Status.WARNING.value

    def set_status(self, status: Status, status_details: str) -> None:
        # The status fields is represented as a string in the Iceberg table.
        self.status = status.value
        self.status_details = self._truncate_middle_if_too_large(status_details)

    def to_row(self) -> Row:
        return Row(**dataclasses.asdict(self))

    @classmethod
    def from_row(cls, row: Row) -> "JournalEntry":
        return cls(**row.asDict())

    def apply_dict(self, **kwargs: dict[str, Any]) -> None:
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                msg = f"The provided key [{key}] does not match any of the JouralEntry fields {fields(JournalEntry)}"
                logger.error(msg)

    @classmethod
    def make_journal_entry(cls, action: Action, mnt_props: MaintenanceScheduleEntry) -> "JournalEntry":
        current_time = time.time()
        return JournalEntry(
            action=action.value,
            full_name=mnt_props.full_name,
            catalog=mnt_props.catalog,
            schema=mnt_props.schema,
            table_name=mnt_props.table_name,
            sql_stm="",
            executed_by=get_user_name(),
            start_time=datetime.fromtimestamp(current_time, tz=timezone.utc),
            end_time=datetime.fromtimestamp(current_time, tz=timezone.utc),
            exec_time_seconds=0.0,
            status=Status.SUCCESS.value,
        )

    @classmethod
    def _truncate_middle_if_too_large(cls, status: str) -> str:
        length = len(status)
        if length <= MAX_STATUS_LENGTH:
            return status  # No truncation needed if the string is within the limit

        # Calculate the amount to keep from the start and end
        keep_half = (MAX_STATUS_LENGTH) // 2

        # Construct the new truncated string
        return status[:keep_half] + "...truncated status..." + status[-keep_half:]
