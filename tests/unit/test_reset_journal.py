from typing import Any
from unittest.mock import patch

from ice_keeper.config import Config
from ice_keeper.ice_keeper import Journal
from ice_keeper.stm import STL


def test_reset_journal_with_location_and_email() -> None:
    """Test reset generates correct SQL with location and notification email."""
    # Reload config to pick up template override
    c = Config.instance()
    prev_maintenance_schedule_table_name = c.maintenance_schedule_table_name
    prev_admin_table_location = c.admin_table_location
    prev_admin_table_notification_email = c.admin_table_notification_email
    try:
        c.journal_table_name = "catalog.schema.journal"
        c.admin_table_location = "file:///iceberg/admin"
        c.admin_table_notification_email = "alerts@example.com"

        with patch.object(STL, "sql_and_log") as mock_stl:
            Journal.reset()

            # Verify SQL calls
            assert mock_stl.call_count == 2  # noqa: PLR2004

            # Check DROP statement
            drop_call: Any = mock_stl.call_args_list[0]
            assert drop_call[0][0] == "drop table if exists catalog.schema.journal"

            # Check CREATE statement
            create_call: Any = mock_stl.call_args_list[1]
            create_sql: str = create_call[0][0]
            # Validate SQL structure
            assert "create table catalog.schema.journal" in create_sql
            assert "using iceberg" in create_sql
            assert "partitioned by (days(start_time))" in create_sql
            assert "location 'file:///iceberg/admin/journal'" in create_sql
            assert "'ice-keeper.notification-email'='alerts@example.com'" in create_sql
            assert "'write.metadata.delete-after-commit.enabled'='true'" in create_sql
            assert "'ice-keeper.should-optimize'='true'" in create_sql
            assert "'ice-keeper.optimization-strategy'='full_name'" in create_sql
    finally:  # Restore original config values
        c.maintenance_schedule_table_name = prev_maintenance_schedule_table_name
        c.admin_table_location = prev_admin_table_location
        c.admin_table_notification_email = prev_admin_table_notification_email


def test_reset_journal_with_no_location_and_no_email() -> None:
    # Reload config to pick up template override
    c = Config.instance()
    prev_maintenance_schedule_table_name = c.maintenance_schedule_table_name
    prev_admin_table_location = c.admin_table_location
    prev_admin_table_notification_email = c.admin_table_notification_email
    try:
        c.journal_table_name = "catalog.schema.journal"
        c.admin_table_location = None
        c.admin_table_notification_email = None

        with patch.object(STL, "sql_and_log") as mock_stl:
            Journal.reset()

            # Verify SQL calls
            assert mock_stl.call_count == 2  # noqa: PLR2004

            # Check DROP statement
            drop_call: Any = mock_stl.call_args_list[0]
            assert drop_call[0][0] == "drop table if exists catalog.schema.journal"

            # Check CREATE statement
            create_call: Any = mock_stl.call_args_list[1]
            create_sql: str = create_call[0][0]
            # Validate SQL structure
            assert "create table catalog.schema.journal" in create_sql
            assert "using iceberg" in create_sql
            assert "partitioned by (days(start_time))" in create_sql
            assert "location 'file:///iceberg/admin/journal'" not in create_sql
            assert "'ice-keeper.notification-email'='alerts@example.com'" not in create_sql
            assert "'write.metadata.delete-after-commit.enabled'='true'" in create_sql
            assert "'ice-keeper.should-optimize'='true'" in create_sql
            assert "'ice-keeper.optimization-strategy'='full_name'" in create_sql
    finally:  # Restore original config values
        c.maintenance_schedule_table_name = prev_maintenance_schedule_table_name
        c.admin_table_location = prev_admin_table_location
        c.admin_table_notification_email = prev_admin_table_notification_email
