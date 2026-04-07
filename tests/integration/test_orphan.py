from datetime import datetime, timedelta, timezone

import pytest

from ice_keeper import Action, IceKeeperTblProperty, Status, TimeProvider
from ice_keeper.config import Config
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from tests.test_common import (
    SCOPE_TABLE,
    inv_schema,
    load_test_table,
    set_tblproperty,
)
from tests.utils import compare_multiline_strings, create_empty_test_table, run_action_and_collect_journal


def _empty_inventory_report() -> None:
    data: list[tuple[str, str]] = []
    df = STL.get().createDataFrame(data, schema=inv_schema)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"


def _inventory_report_single_data_file(age_of_date_files: int) -> None:
    TimeProvider.set(datetime(2023, 3, 3, 15, 0, 0, tzinfo=timezone.utc))
    table = load_test_table()
    table_location = table.location()

    loaded_at = TimeProvider.current_date() - timedelta(days=10)
    last_modified = TimeProvider.current_datetime() - timedelta(days=age_of_date_files)
    file_path = f"{table_location}/data/c.parquet"
    # file_path in the inventory does not include the scheme
    file_path = file_path.replace("file:///", "")
    data = [(loaded_at, last_modified, "", "", file_path, 1)]
    df = STL.get().createDataFrame(data, schema=inv_schema)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"


@pytest.mark.integration
def test_remove_orphan_files_with_no_inventory_report_should_fallback_to_directory_listing(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime(2023, 3, 3, 15, 0, 0, tzinfo=timezone.utc))
    # orphan task defaults to 500 days before current date
    expected_older_than = TimeProvider.current_date() - timedelta(days=500)

    create_empty_test_table(executor)

    prev_storage_inventory_report_table_name = Config.instance().storage_inventory_report_table_name
    try:
        Config.instance().storage_inventory_report_table_name = None
        rows = run_action_and_collect_journal(executor, Action.REMOVE_ORPHAN_FILES, scope=SCOPE_TABLE)
        # Then we should have the corresponding log.
        assert len(rows) == 1, "should have 1 log"
        status = rows[0].status
        assert status == Status.SUCCESS.value, "Should succeed"
        actual_output = rows[0].sql_stm
        expected_output = f"""
            call local.system.remove_orphan_files(
                table => 'test.test'
                , older_than => timestamp '{expected_older_than}'
                , dry_run => false)
        """
        diff, details = compare_multiline_strings(expected_output, actual_output)
        if diff:
            msg = f"Test test_remove_orphan_files_with_no_inventory_report_should_fallback_to_directory_listing failed. The actual output was {actual_output}.\nDifferences are {details}"
            raise Exception(msg)
    finally:
        Config.instance().storage_inventory_report_table_name = prev_storage_inventory_report_table_name


@pytest.mark.integration
def test_remove_orphan_files_no_file_paths_found_in_inventory(executor: TaskExecutor) -> None:
    create_empty_test_table(executor)

    _inventory_report_single_data_file(1)

    rows = run_action_and_collect_journal(executor, Action.REMOVE_ORPHAN_FILES, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status_details == ""
    assert status == Status.SUCCESS.value, "Should succeed"
    actual_output = rows[0].sql_stm

    expected_output = """
        call local.system.remove_orphan_files(
               table => 'test.test'
               , older_than => timestamp '2023-02-15'
               , dry_run => false)
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"Test test_remove_orphan_files_with_no_inventory_report_should_fallback_to_directory_listing failed. The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)


@pytest.mark.integration
def test_remove_orphan_files_with_file_paths_found_in_inventory(executor: TaskExecutor) -> None:
    create_empty_test_table(executor)

    _inventory_report_single_data_file(100)

    rows = run_action_and_collect_journal(executor, Action.REMOVE_ORPHAN_FILES, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status_details == ""
    assert status == Status.SUCCESS.value, "Should succeed"
    actual_output = rows[0].sql_stm

    # procedure uses a file_list_view
    expected_output = """
        call local.system.remove_orphan_files(
               table => 'test.test'
               , older_than => timestamp '2023-02-15'
               , file_list_view => 'file_list_view'
               , dry_run => false)
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"Test test_remove_orphan_files_with_file_paths_found_in_inventory failed. The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)


@pytest.mark.integration
def test_remove_orphan_files_explicit_disabled(executor: TaskExecutor) -> None:
    create_empty_test_table(executor)
    set_tblproperty(IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES, "false")
    _inventory_report_single_data_file(100)

    rows = run_action_and_collect_journal(executor, Action.REMOVE_ORPHAN_FILES, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 0, "should have no logs"
