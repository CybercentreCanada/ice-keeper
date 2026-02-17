import pytest
from pyspark.sql import Row

from ice_keeper.table.schedule_entry import MaintenanceScheduleRecord
from ice_keeper.task.action.config_auditor import ConfigAuditorStrategy
from tests.test_common import (
    ONE_EXPECTED,
    ZERO_EXPECTED,
)


@pytest.fixture
def config_auditor() -> ConfigAuditorStrategy:
    row = Row(catalog="x", schema="y", table_name="y", full_name="x.y.z", table_location="")
    record = MaintenanceScheduleRecord.from_row(row)
    mnt_props = record.to_entry()
    return ConfigAuditorStrategy(mnt_props)


def test_all_valid_configs(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {
        "owner": "",
        "write.merge.mode": "",
        "groupOwner": "",
        "ice-keeper.should-optimize": "",
        "write.delete.mode": "",
        "write.parquet.compression-codec": "",
        "write.delete.isolation-level": "",
        "write.metadata.delete-after-commit.enabled": "",
        "ice-keeper.should-remove-orphan-files": "",
        "write.metadata.compression-codec": "",
        "write.merge.isolation-level": "",
        "created-at": "",
        "comment": "",
        "ice-keeper.should-expire-snapshots": "",
        "write.update.mode": "",
        "write.update.isolation-level": "",
        "write.target-file-size-bytes": "",
        "ice-keeper.optimization-strategy": "",
        "ice-keeper.notification-email": "",
        "ice-keeper.retention-num-snapshots": "",
        "history.expire.min-snapshots-to-keep": "",
        "commit.manifest-merge.enabled": "",
        "write.metadata.previous-versions-max": "",
        "last-iceberg-maintenance": "",
        "commit.retry.num-retries": "",
        "write.distribution-mode": "",
        "ice-keeper.min-age-to-optimize": "",
        "ice-keeper.max-age-to-optimize": "",
        "commit.manifest.min-count-to-merge": "",
        "read.split.target-size": "",
        "read.parquet.vectorization.enabled": "",
        "ice-keeper.should-apply-lifecycle": "",
        "ice-keeper.should-rewrite-manifest": "",
        "cccs-retention-script.max_retention_days": "",
        "ice-keeper.optimization-target-file-size-bytes": "",
        "write.parquet.row-group-size-bytes": "",
        "write.wap.enabled": "",
        "read.split-size": "",
        "write.spark.accept-any-schema": "",
        "commit.retry.max-wait-ms": "",
        "write.metadata.metrics.default": "",
        "commit.retry.min-wait-ms": "",
        "ice-keeper.optimize-partition-depth": "",
        "history.expire.max-snapshot-age-ms": "",
        "ice-keeper.lifecycle-ingestion-time-column": "",
        "write.format.default": "",
        "owners": "",
        "tags": "",
        "ice-keeper.lifecycle-max-days": "",
        "last_partition_size": "",
        "update.merge.isolation-level": "",
        "delete.merge.isolation-level": "",
    }
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ZERO_EXPECTED


def test_typo_in_ice_keeper_prefix(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"ice_keeper.should-apply-lifecycle": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED


def test_typo_in_ice_keeper_prefix2(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"icekeeper.should-apply-lifecycle": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED


def test_typo_in_suffix(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"ice-keeper.should_apply_lifecycle": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED


def test_typo_in_both(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"ice_keeper.should_apply_lifecycle": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED


def test_typo_iceberg_prop_reused_by_icekeeper(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"write_target_file_size_bytes": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED


def test_typo_iceberg_prop_reused_by_icekeeper2(config_auditor: ConfigAuditorStrategy) -> None:
    properties = {"write_target_file_size_bytes": "false"}
    warnings = config_auditor._check_config(properties)
    assert len(warnings) == ONE_EXPECTED
