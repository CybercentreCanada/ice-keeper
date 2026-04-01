import datetime
from dataclasses import dataclass

import humanfriendly
import pytest

from ice_keeper.ice_keeper import OptimizationStrategy
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import PartitionSummary
from ice_keeper.task.action.optimization.optimization import SubOptimizationStrategy
from ice_keeper.task.action.optimization.partition_diagnostic import PartitionDiagnosis
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    TEST_FULL_NAME,
)
from tests.utils import (
    compare_multiline_strings,
    create_test_table_with_one_batch,
    discover_tables,
)

default_sort_rewrite_data_files_options = f"""options => map(
                'max-concurrent-file-group-rewrites', '100',
                'partial-progress.enabled', 'true',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true',
                'max-file-group-size-bytes', '{humanfriendly.parse_size("200 GB", binary=True)!s}',
                'target-file-size-bytes', '536870912',
                'output-spec-id', '0',
                'rewrite-all', 'true',
                'min-input-files', '1',
                'shuffle-partitions-per-file', '8')"""

default_binpack_rewrite_data_files_options = f"""options => map(
                'max-concurrent-file-group-rewrites', '100',
                'partial-progress.enabled', 'true',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true',
                'max-file-group-size-bytes', '{humanfriendly.parse_size("200 GB", binary=True)!s}',
                'target-file-size-bytes', '536870912',
                'output-spec-id', '0',
                'rewrite-all', 'false',
                'min-input-files', '1')"""


@dataclass
class OptimizeTestCase:
    test_name: str
    partitioned_by: str
    optimization_strategy: str
    expected_output: str


# The schema of the test table is
# (ts timestamp, id int, name string, category string, submission struct<ts timestamp>)

optimize_test_scenarios = [
    OptimizeTestCase("not_partitioned_not_binpack", "", "", ""),
    OptimizeTestCase("hours_ts__not_sorted", "hours(ts)", "", ""),
    OptimizeTestCase("hours_submission_ts__not_sorted", "hours(submission.ts)", "", ""),
    # ---------- binpack --------------
    OptimizeTestCase(
        "not_partitioned_binpack",
        "",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_binpack_rewrite_data_files_options}
        , strategy => 'binpack'
        , where => " (1 = 1) "
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__binpack",
        "hours(ts)",
        "binpack",
        f"""CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_binpack_rewrite_data_files_options}
        , strategy => 'binpack'
        , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
        )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__binpack",
        "hours(submission.ts)",
        "binpack",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour )  "
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_3__id__binpack",
        "days(ts), bucket(3, id)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
                table => 'test.test'
                , {default_binpack_rewrite_data_files_options}
                , strategy => 'binpack'
                , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
                 )
            """,
    ),
    # Truncate category_int to one bin.
    OptimizeTestCase(
        "truncate_category_int",
        "truncate(100000, category_int)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( local.system.truncate(100000, category_int) = 0 ) "
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_3__category__binpack",
        "months(ts), truncate(3, category)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month )  "
            )
        """,
    ),
    # ---------- Sort --------------
    OptimizeTestCase(
        "year_sort_by_id",
        "years(ts)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-01-01') and ts < date('2025-01-01') + interval 1 year ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "not_partitioned_sort_by_id",
        "",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " (1 = 1) "
        , sort_order => 'id'
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__id",
        "hours(ts)",
        "id",
        f"""CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
        , sort_order => 'id'
        )
         """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_4__id__id",
        "days(ts), bucket(4, id)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_1__category__name",
        "months(ts), truncate(1, category)",
        "name",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'name'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_2__category__name",
        "months(ts), truncate(2, category)",
        "name",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'name'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_4__category__id",
        "months(ts), truncate(4, category)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'id'
            )
         """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__id",
        "hours(submission.ts)",
        "id",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__id_DESC__submission_ts_nulls_first",
        "hours(submission.ts)",
        "id DESC, submission.ts nulls first",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'id DESC, submission.ts nulls first'
             )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_2__id__id_ASC_NULLS_LAST",
        "days(ts), bucket(2, id)",
        "id ASC NULLS LAST",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'id ASC NULLS LAST'
            )
        """,
    ),
    # ------- zorder --------
    OptimizeTestCase(
        "not_partitioned_zorder_id__name_",
        "",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " (1 = 1) "
        , sort_order => 'zorder(id, name)'
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__zorder_id__name_",
        "hours(ts)",
        "zorder(id, name)",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__zorder_id__name_",
        "hours(submission.ts)",
        "zorder(id, name)",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'zorder(id, name)'
             )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id___bucket_2__category__zorder_id__name_",
        "days(ts), bucket(5, id), bucket(2, category)",
        "zorder(id, name)",
        f"""
         CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id___truncate_2__category__zorder_id__name_",
        "days(ts), bucket(5, id), truncate(2, category)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id__zorder_id__name_",
        "days(ts), bucket(5, id)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
          """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_5__category__zorder_id__name_",
        "months(ts), truncate(5, category)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
]


@pytest.mark.integration
@pytest.mark.parametrize(
    argnames=("test_name", "partitioned_by", "optimization_strategy", "expected_output"),
    argvalues=[
        (test.test_name, test.partitioned_by, test.optimization_strategy, test.expected_output)
        for test in optimize_test_scenarios
    ],
    ids=[test.test_name for test in optimize_test_scenarios],
)
def test_optimize(
    test_name: str, partitioned_by: str, optimization_strategy: str, expected_output: str, executor: TaskExecutor
) -> None:
    # Change default min age for testing.
    properties = {
        "write.delete.mode": "merge-on-read",
        IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
        IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
    }
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_test_table_with_one_batch(
        event_time=dt, partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties
    )
    sql = f"delete from {TEST_FULL_NAME} where id = (select id from {TEST_FULL_NAME} where ts = '{dt}' limit 1)"
    STL.get().sql(sql)
    # add table to maintenance schedule
    discover_tables(executor, SCOPE_SCHEMA)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    mnt_props = maintenance_schedule.entries()[0]
    assert mnt_props

    os = OptimizationStrategy(mnt_props)
    if os.check_should_execute_action():
        spec_id = 0
        widening_rule = None
        summary = PartitionSummary(mnt_props, spec_id, widening_rule)
        diagnosis = PartitionDiagnosis(mnt_props, spec_id)
        rows = diagnosis.find_partitions_to_optimize(summary)
        assert len(rows) == ONE_EXPECTED
        sos = SubOptimizationStrategy(rows[0], spec_id, mnt_props, widening_rule)
        actual_output = sos.prepare_statement_to_execute()
        diff, details = compare_multiline_strings(expected_output, actual_output)
        if diff:
            msg = f"Test {test_name} failed. The actual output was {actual_output}.\nDifferences are {details}"
            raise Exception(msg)
    else:
        assert not mnt_props.optimization_spec.is_binpack()
        assert not mnt_props.optimization_spec.is_sorted()
