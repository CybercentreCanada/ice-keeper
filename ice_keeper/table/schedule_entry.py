"""Module for managing Iceberg table maintenance schedules.

Including properties like optimizations, lifecycle policies, snapshot expiration, orphan file management, and partitioning.
Provides the `MaintenanceScheduleEntry` as a key abstraction.
"""

import logging
from functools import cached_property
from typing import Any

from pydantic import BaseModel, Field
from pyiceberg.table import Table
from pyspark.sql import Row

from ice_keeper import IceKeeperTblProperty, get_user_name
from ice_keeper.spec import OptimizationSpec
from ice_keeper.spec.partition_spec import PartitionSpecifications

logger = logging.getLogger("ice-keeper")


DEFAULTS = {
    "partition_by": "",
    "optimize_partition_depth": 1,
    "optimization_strategy": "",
    "should_optimize": False,
    "min_age_to_optimize": -1,  # Deprecated configuration, default to -1 indicating it is not set and we should be using min_partition_to_optimize instead.
    "max_age_to_optimize": -1,
    "min_partition_to_optimize": "1d",
    "max_partition_to_optimize": "7d",
    "target_file_size_bytes": 536870912,
    "should_expire_snapshots": True,
    "retention_days_snapshots": 7,
    "should_remove_orphan_files": True,
    "retention_days_orphan_files": 5,
    "last_updated_by": "",
    "retention_num_snapshots": 1,
    "should_rewrite_manifest": False,
    "notification_email": "",
    "should_apply_lifecycle": False,
    "lifecycle_max_days": 330,
    "lifecycle_ingestion_time_column": "",
    "optimization_grouping_size_bytes": 33554432,
    "binpack_min_input_files": 5,
    "widening_rule_select_criteria": "",
    "widening_rule_required_partition_columns": "",
    "widening_rule_src_partition": "",
    "widening_rule_dst_partition": "",
    "widening_rule_min_age_to_widen": -1,
    "widening_rule_min_partition_to_widen": "1M",
    "widening_rule_max_partition_to_widen": "2M",
    "table_location": "",
}


class MaintenanceScheduleRecord(BaseModel):
    """Represents a maintenance schedule record in the database.

    Stores table metadata,
    optimization configurations, lifecycle management settings, and widening rules. Also includes
    methods for converting to/from PySpark Rows and MaintenanceScheduleEntry.
    """

    full_name: str
    catalog: str
    # Use an alias to avoid conflict with .schema()
    schema_: str = Field(alias="schema")
    table_name: str
    partition_by: str | None = None
    optimize_partition_depth: int | None = None
    optimization_strategy: str | None = None
    should_optimize: bool | None = None
    min_age_to_optimize: int | None = None
    max_age_to_optimize: int | None = None
    min_partition_to_optimize: str | None = None
    max_partition_to_optimize: str | None = None
    target_file_size_bytes: int | None = None
    should_expire_snapshots: bool | None = None
    retention_days_snapshots: int | None = None
    should_remove_orphan_files: bool | None = None
    retention_days_orphan_files: int | None = None
    last_updated_by: str | None = None
    retention_num_snapshots: int | None = None
    should_rewrite_manifest: bool | None = None
    notification_email: str | None = None
    should_apply_lifecycle: bool | None = None
    lifecycle_max_days: int | None = None
    lifecycle_ingestion_time_column: str | None = None
    optimization_grouping_size_bytes: int | None = None
    binpack_min_input_files: int | None = None
    widening_rule_select_criteria: str | None = None
    widening_rule_required_partition_columns: str | None = None
    widening_rule_src_partition: str | None = None
    widening_rule_dst_partition: str | None = None
    widening_rule_min_age_to_widen: int | None = None
    widening_rule_min_partition_to_widen: str | None = None
    widening_rule_max_partition_to_widen: str | None = None
    table_location: str | None = None

    def to_entry(self) -> "MaintenanceScheduleEntry":
        return MaintenanceScheduleEntry(self)

    @classmethod
    def get_ddl(cls) -> str:
        return """
            full_name STRING NOT NULL,
            catalog STRING NOT NULL,
            schema STRING NOT NULL,
            table_name STRING NOT NULL,
            partition_by STRING,
            optimize_partition_depth INT,
            optimization_strategy STRING,
            should_optimize BOOLEAN,
            min_age_to_optimize INT,
            max_age_to_optimize INT,
            min_partition_to_optimize STRING,
            max_partition_to_optimize STRING,
            target_file_size_bytes BIGINT,
            should_expire_snapshots BOOLEAN,
            retention_days_snapshots INT,
            should_remove_orphan_files BOOLEAN,
            retention_days_orphan_files INT,
            last_updated_by STRING,
            retention_num_snapshots INT,
            should_rewrite_manifest BOOLEAN,
            notification_email STRING,
            should_apply_lifecycle BOOLEAN,
            lifecycle_max_days INT,
            lifecycle_ingestion_time_column STRING,
            optimization_grouping_size_bytes BIGINT,
            binpack_min_input_files INT,
            widening_rule_select_criteria STRING,
            widening_rule_required_partition_columns STRING,
            widening_rule_src_partition STRING,
            widening_rule_dst_partition STRING,
            widening_rule_min_age_to_widen INT,
            widening_rule_min_partition_to_widen STRING,
            widening_rule_max_partition_to_widen STRING,
            table_location STRING
            """

    def same_config_as(self, other: "MaintenanceScheduleRecord") -> bool:
        """Compares the configuration of two `MaintenanceScheduleRecord` objects, ignoring metadata fields.

        This method compares the value that are actually stored in the table.

        Args:
            other (MaintenanceScheduleRecord): Another maintenance schedule entry to compare.

        Returns:
            bool: True if the configurations are identical, False otherwise.
        """
        # The last_updated_by field is ignored.
        # Compare everything except last_updated_by
        return self.model_dump(exclude={"last_updated_by"}) == other.model_dump(exclude={"last_updated_by"})

    def get(self, field: str) -> Any:  # noqa: ANN401
        """Get field value, returning default if None.

        Args:
            field: Name of the field to retrieve

        Returns:
            Field value if not None, otherwise default from DEFAULTS dict
        """
        value = getattr(self, field)
        return value if value is not None else DEFAULTS.get(field)

    @classmethod
    def from_row(cls, row: Row) -> "MaintenanceScheduleRecord":
        """Creates a `MaintenanceScheduleRecord` instance from a PySpark `Row`.

        Args:
            row (Row): A PySpark row containing maintenance schedule data.

        Returns:
            MaintenanceScheduleRecord: An instance created from row data.
        """
        return cls(**row.asDict())

    def to_row(self) -> Row:
        """Converts the `MaintenanceScheduleRecord` instance into a PySpark `Row`.

        Returns:
            Row: A PySpark row representation of the instance.
        """
        return Row(**self.model_dump(by_alias=True))

    @classmethod
    def from_iceberg_table(cls, table: Table) -> "MaintenanceScheduleRecord":
        """Creates a `MaintenanceScheduleRecord` instance from an Iceberg `Table` object.

        This method extracts table properties, partition specifications, and additional
        information from the Iceberg table metadata, combines it with any hardcoded
        widening rules based on the table's catalog and schema, converts the data into
        a PySpark `Row`, and then initializes an instance of the class.

        Args:
            table (Table): The Iceberg `Table` object containing metadata about a table.

        Returns:
            MaintenanceScheduleRecord: A new instance with the corresponding settings
            from the Iceberg table metadata.
        """
        tblproperties = table.metadata.properties
        table_location = table.location()
        columns = cls._convert_to_columns(tblproperties)
        partition_by_json = PartitionSpecifications.serialize_partition_by(table)
        catalog = table.catalog.name
        schema = table.name()[0]
        table_name = table.name()[-1]
        full_name = f"{catalog}.{schema}.{table_name}"
        scope: dict[str, Any] = {
            "full_name": full_name,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "partition_by": partition_by_json,
            "table_location": table_location,
        }
        data: dict[str, Any] = scope | columns
        row = Row(**data)
        return cls.from_row(row)

    @classmethod
    def _get_boolean(cls, tblproperties: dict[str, str], key: str) -> bool | None:
        value_str = tblproperties.get(key)
        if value_str is None:
            return None
        return value_str.lower() in ("true", "1", "yes")

    @classmethod
    def _get_int(cls, tblproperties: dict[str, str], key: str) -> int | None:
        value: int | None = None
        value_str = tblproperties.get(key)
        if value_str:
            try:
                value = int(value_str)
            except Exception:
                logger.exception("Failed to parse tblproperty: {tblproperty}, in table {full_name}", stack_info=True)
        return value

    @classmethod
    def _get_days_from_ms(cls, tblproperties: dict[str, str], key: str) -> int | None:
        value: int | None = None
        value_str = tblproperties.get(key)
        if value_str:
            try:
                value = int(int(value_str) / 1000 / 60 / 60 / 24)
            except Exception:
                logger.exception("Failed to parse tblproperty: {tblproperty}, in table {full_name}", stack_info=True)
        return value

    @classmethod
    def _get_string(cls, tblproperties: dict[str, str], key: str) -> str | None:
        return tblproperties.get(key)

    @classmethod
    def _convert_to_columns(cls, tblproperties: dict[str, str]) -> dict[str, Any]:
        """Converts Iceberg table properties to a dictionary suitable for creating a maintenance schedule entry.

        Args:
            tblproperties (dict[str, str]): Key-value pairs representing Iceberg table properties as found in a pyspark row.

        Returns:
            dict[str, Any]: Parsed and validated properties.
        """
        parsed: dict[str, Any] = {}

        parsed["should_expire_snapshots"] = cls._get_boolean(tblproperties, IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS)

        if tblproperties.get(IceKeeperTblProperty.RETENTION_DAYS_SNAPSHOTS):
            parsed["retention_days_snapshots"] = cls._get_int(tblproperties, IceKeeperTblProperty.RETENTION_DAYS_SNAPSHOTS)
        elif tblproperties.get(IceKeeperTblProperty.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS):
            parsed["retention_days_snapshots"] = cls._get_days_from_ms(
                tblproperties,
                IceKeeperTblProperty.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS,
            )

        parsed["should_remove_orphan_files"] = cls._get_boolean(tblproperties, IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES)
        parsed["retention_days_orphan_files"] = cls._get_int(tblproperties, IceKeeperTblProperty.RETENTION_DAYS_ORPHAN_FILES)

        parsed["should_optimize"] = cls._get_boolean(tblproperties, IceKeeperTblProperty.SHOULD_OPTIMIZE)
        parsed["optimization_strategy"] = cls._get_string(tblproperties, IceKeeperTblProperty.OPTIMIZATION_STRATEGY)
        parsed["min_age_to_optimize"] = cls._get_int(tblproperties, IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE)
        parsed["max_age_to_optimize"] = cls._get_int(tblproperties, IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE)
        parsed["min_partition_to_optimize"] = cls._get_string(tblproperties, IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE)
        parsed["max_partition_to_optimize"] = cls._get_string(tblproperties, IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE)
        if tblproperties.get(IceKeeperTblProperty.OPTIMIZATION_TARGET_FILE_SIZE_BYTES):
            parsed["target_file_size_bytes"] = cls._get_int(
                tblproperties, IceKeeperTblProperty.OPTIMIZATION_TARGET_FILE_SIZE_BYTES
            )
        elif tblproperties.get(IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES):
            parsed["target_file_size_bytes"] = cls._get_int(tblproperties, IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES)

        parsed["optimize_partition_depth"] = cls._get_int(tblproperties, IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH)
        parsed["optimization_grouping_size_bytes"] = cls._get_int(
            tblproperties, IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES
        )
        parsed["binpack_min_input_files"] = cls._get_int(tblproperties, IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES)

        parsed["retention_num_snapshots"] = cls._get_int(tblproperties, IceKeeperTblProperty.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP)
        if tblproperties.get(IceKeeperTblProperty.RETENTION_NUM_SNAPSHOTS):
            parsed["retention_num_snapshots"] = cls._get_int(tblproperties, IceKeeperTblProperty.RETENTION_NUM_SNAPSHOTS)

        parsed["should_rewrite_manifest"] = cls._get_boolean(
            tblproperties,
            IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST,
        )
        parsed["notification_email"] = cls._get_string(tblproperties, IceKeeperTblProperty.NOTIFICATION_EMAIL)

        parsed["should_apply_lifecycle"] = cls._get_boolean(
            tblproperties,
            IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE,
        )
        parsed["lifecycle_max_days"] = cls._get_int(tblproperties, IceKeeperTblProperty.LIFECYCLE_MAX_DAYS)

        parsed["lifecycle_ingestion_time_column"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN,
        )

        parsed["last_updated_by"] = get_user_name()

        parsed["widening_rule_select_criteria"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA,
        )
        parsed["widening_rule_required_partition_columns"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS,
        )
        parsed["widening_rule_src_partition"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION,
        )
        parsed["widening_rule_dst_partition"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION,
        )

        parsed["widening_rule_min_age_to_widen"] = cls._get_int(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN,
        )

        parsed["widening_rule_min_partition_to_widen"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_MIN_PARTITION_TO_WIDEN,
        )

        parsed["widening_rule_max_partition_to_widen"] = cls._get_string(
            tblproperties,
            IceKeeperTblProperty.WIDENING_RULE_MAX_PARTITION_TO_WIDEN,
        )
        return parsed


class MaintenanceScheduleEntry:
    """Represents a maintenance schedule entry for an Iceberg table.

    Stores table metadata,
    optimization configurations, lifecycle management settings, and widening rules. Also includes
    methods for converting to/from PySpark Rows and MaintenanceScheduleEntry.
    """

    def __init__(self, record: MaintenanceScheduleRecord) -> None:
        self._record = record

    @property
    def record(self) -> MaintenanceScheduleRecord:
        """Access to underlying storage record."""
        return self._record

    @property
    def full_name(self) -> str:
        return self._record.full_name

    @property
    def catalog(self) -> str:
        return self._record.catalog

    @property
    def schema(self) -> str:
        return self._record.schema_

    @property
    def table_name(self) -> str:
        return self._record.table_name

    @property
    def partition_by(self) -> str:
        return self._record.get("partition_by")

    @property
    def optimize_partition_depth(self) -> int:
        return self._record.get("optimize_partition_depth")

    @property
    def optimization_grouping_size_bytes(self) -> int:
        return self._record.get("optimization_grouping_size_bytes")

    @property
    def binpack_min_input_files(self) -> int:
        return self._record.get("binpack_min_input_files")

    @property
    def optimization_strategy(self) -> str:
        return self._record.get("optimization_strategy")

    @property
    def should_optimize(self) -> bool:
        return self._record.get("should_optimize")

    @property
    def min_age_to_optimize(self) -> int:
        return self._record.get("min_age_to_optimize")

    @property
    def max_age_to_optimize(self) -> int:
        return self._record.get("max_age_to_optimize")

    @property
    def min_partition_to_optimize(self) -> str:
        return self._record.get("min_partition_to_optimize")

    @property
    def max_partition_to_optimize(self) -> str:
        return self._record.get("max_partition_to_optimize")

    @property
    def target_file_size_bytes(self) -> int:
        return self._record.get("target_file_size_bytes")

    @property
    def should_expire_snapshots(self) -> bool:
        return self._record.get("should_expire_snapshots")

    @property
    def retention_days_snapshots(self) -> int:
        return self._record.get("retention_days_snapshots")

    @property
    def should_remove_orphan_files(self) -> bool:
        return self._record.get("should_remove_orphan_files")

    @property
    def retention_days_orphan_files(self) -> int:
        return self._record.get("retention_days_orphan_files")

    @property
    def last_updated_by(self) -> str:
        return self._record.get("last_updated_by")

    @property
    def retention_num_snapshots(self) -> int:
        return self._record.get("retention_num_snapshots")

    @property
    def should_rewrite_manifest(self) -> bool:
        return self._record.get("should_rewrite_manifest")

    @property
    def notification_email(self) -> str:
        return self._record.get("notification_email")

    @property
    def should_apply_lifecycle(self) -> bool:
        return self._record.get("should_apply_lifecycle")

    @property
    def lifecycle_max_days(self) -> int:
        return self._record.get("lifecycle_max_days")

    @property
    def lifecycle_ingestion_time_column(self) -> str:
        return self._record.get("lifecycle_ingestion_time_column")

    @property
    def widening_rule_select_criteria(self) -> str:
        return self._record.get("widening_rule_select_criteria")

    @property
    def widening_rule_required_partition_columns(self) -> str:
        return self._record.get("widening_rule_required_partition_columns")

    @property
    def widening_rule_src_partition(self) -> str:
        return self._record.get("widening_rule_src_partition")

    @property
    def widening_rule_dst_partition(self) -> str:
        return self._record.get("widening_rule_dst_partition")

    @property
    def widening_rule_min_age_to_widen(self) -> int:
        return self._record.get("widening_rule_min_age_to_widen")

    @property
    def widening_rule_min_partition_to_widen(self) -> str:
        return self._record.get("widening_rule_min_partition_to_widen")

    @property
    def widening_rule_max_partition_to_widen(self) -> str:
        return self._record.get("widening_rule_max_partition_to_widen")

    def get_widening_rule_required_partition_columns(self) -> list[str]:
        """Parses the `widening_rule_required_partition_columns` attribute into a list of column names.

        Returns:
            list[str]: A list of column names that are required in the partition widening rule.
        """
        if self.widening_rule_required_partition_columns:
            columns = self.widening_rule_required_partition_columns.split(",")
            return [column.strip() for column in columns]
        return []

    @property
    def table_location(self) -> str:
        return self._record.get("table_location")

    @cached_property
    def partition_specs(self) -> PartitionSpecifications:
        """Retrieves the PartitionSpecifications from the Iceberg table's metadata."""
        return PartitionSpecifications.deserialize_partition_by(self.catalog, self.schema, self.table_name)

    @cached_property
    def optimization_spec(self) -> OptimizationSpec:
        """Converts the optimization_strategy string into an `OptimizationSpec` object."""
        return OptimizationSpec.from_string(self.optimization_strategy)
