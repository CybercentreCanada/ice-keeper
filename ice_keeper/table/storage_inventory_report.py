import logging
import re
from datetime import date

import pyspark.sql.functions as F  # noqa: N812

from ice_keeper import ActionFailed, FileScheme, TimeProvider
from ice_keeper.config import Config
from ice_keeper.stm import STL

from .schedule_entry import MaintenanceScheduleEntry

logger = logging.getLogger("ice-keeper")


# class InventoryReporProvider:
#     # Used for testing.
#     mock_dataframe: DataFrame | None = None

#     @classmethod
#     def inventory_table(cls) -> str:
#         if cls.mock_dataframe:
#             cls.mock_dataframe.createOrReplaceGlobalTempView("inventory_report")
#             return "global_temp.inventory_report"
#         return Config.instance().storage_inventory_report_table_name

#     @classmethod
#     def inventory_dataframe(cls) -> DataFrame:
#         if cls.mock_dataframe:
#             return cls.mock_dataframe
#         return STL.get().table(Config.instance().storage_inventory_report_table_name)


class StorageInventoryReport:
    latest_inventory_report_date: date | None = TimeProvider.current_date()
    latest_date_resolved = False

    ICEBERG_FILE_CRITERION = """
            (
                endswith(file_path, '.parquet')
                or endswith(file_path, '.avro')
                or endswith(file_path, '.json')
                or endswith(file_path, 'version-hint.text')
            )
            """

    def __init__(self, mnt_props: MaintenanceScheduleEntry) -> None:
        """Initialize an inventory report for the given maintenance entry.

        Args:
            mnt_props (MaintenanceScheduleEntry): The maintenance entry to generate the inventory report for.
        """
        self.mnt_props = mnt_props
        self.location_container = ""
        self.location_storage_account = ""
        self.location_file_path = ""
        self._init_table_location()

    def _handle_abfss_paths(self) -> None:
        # The location will be of the form abfss://<container>@<storage_account>.dfs.core.windows.net/iceberg/schema1/table1'
        # Regex to extract container and storage_account
        match = re.search(r"abfss://(.+?)@(.+?)\.dfs\.core\.windows\.net/(.+)", self.mnt_props.table_location)
        if not match:
            msg = f"Unable to extract container and storage account from given path [{self.mnt_props.table_location}]"
            raise Exception(msg)
        self.location_container = match.group(1)
        self.location_storage_account = match.group(2)
        file_path = match.group(3)
        self.location_file_path = f"{self.location_container}/{file_path}"

    def _handle_file_paths(self) -> None:
        # The location will be of the form file:///iceberg/schema1/table1'
        # Regex to extract container and storage_account
        match = re.search(r"file:///(.+)", self.mnt_props.table_location)
        if not match:
            msg = f"Unable to extract file path from given path [{self.mnt_props.table_location}]"
            raise Exception(msg)
        self.location_container = ""
        self.location_storage_account = ""
        self.location_file_path = match.group(1)

    def _init_table_location(self) -> None:
        # Get this tables location on disk.
        scheme = FileScheme.from_url(self.mnt_props.table_location)
        self.filesystem_scheme = scheme
        if self.filesystem_scheme == FileScheme.ABFSS:
            self._handle_abfss_paths()
        elif self.filesystem_scheme == FileScheme.FILE:
            self._handle_file_paths()
        else:
            msg = f"Filesystem scheme not supported [{self.filesystem_scheme}]."
            raise ActionFailed(msg)

    def check_init_latest_date(self) -> None:
        cls = StorageInventoryReport
        full_name = Config.instance().storage_inventory_report_table_name
        if full_name and not cls.latest_date_resolved:
            rows = STL.get().table(full_name).agg(F.max(F.col("loaded_at"))).take(1)
            if len(rows) > 0 and rows[0][0]:  # First row, first column.
                cls.latest_inventory_report_date = rows[0][0]
            else:
                msg = "Could not determine the last ingestion date of the inventory report"
                raise Exception(msg)

    def get_most_recent_inventory_report_date(self) -> date:
        self.check_init_latest_date()
        cls = StorageInventoryReport
        assert cls.latest_inventory_report_date, "Should throw if it can't determine the inventory date"
        return cls.latest_inventory_report_date

    def _all_file_paths_criterion(self, older_than: date) -> str:
        # Use startswith instead of LIKE. The LIKE operator can't be used to prune data files when it contains an underscore.
        # See https://github.com/apache/iceberg/issues/14995
        return f"""
            (
                loaded_at = date('{self.get_most_recent_inventory_report_date()}')
                and last_modified < timestamp '{older_than}'
                and storage_account = '{self.location_storage_account}'
                and container = '{self.location_container}'
                and (
                    startswith(file_path, '{self.location_file_path}/metadata/')
                    or startswith(file_path, '{self.location_file_path}/data/')
                )
            )
            """

    def get_base_path_with_scheme_stmt(self) -> str:
        if self.filesystem_scheme == FileScheme.ABFSS:
            return f"'abfss://{self.location_container}@{self.location_storage_account}.dfs.core.windows.net' || substr(file_path, length('{self.location_container}') + 1)"
        if self.filesystem_scheme == FileScheme.FILE:
            return "'file:///' || file_path"
        msg = f"Filesystem scheme not supported [{self.filesystem_scheme}]."
        raise ActionFailed(msg)

    def select_files_and_empty_folders_from_inventory_stmt(self, older_than: date) -> str:
        file_list_view_sql = self.select_iceberg_files_from_inventory_stmt(older_than)
        empty_dirs_sql = self.select_empty_folders_from_inventory_stmt(older_than)
        base_path_with_scheme_sql = self.get_base_path_with_scheme_stmt()
        return f"""
            -- the remove_orphan_files procedure needs a view with 2 columns: file_path and last_modified.
            -- Return a dataset sorted by the length of the file path in descending order to increase our
            -- chances that sub-directories are processed before their parent directories.
            -- This is not a guarantee since the list of files is handled by a delete thread pool,
            -- but it should help reduce the chances of hitting "Directory not empty" errors when trying to delete folders.
            select
                {base_path_with_scheme_sql} as file_path,
                last_modified
            from (
                select file_path, last_modified from ({empty_dirs_sql})
                union all
                select file_path, last_modified from ({file_list_view_sql})
            )
            order by
                length(file_path) desc
            """

    def select_iceberg_files_from_inventory_stmt(self, older_than: date) -> str:
        full_name = Config.instance().storage_inventory_report_table_name
        if not full_name:
            msg = "Storage inventory report table name is not configured."
            raise Exception(msg)

        return f"""
            select
                file_path,
                last_modified
            from
                {full_name}
            where
                {self._all_file_paths_criterion(older_than)}
                and {StorageInventoryReport.ICEBERG_FILE_CRITERION}
            """

    def select_empty_folders_from_inventory_stmt(self, older_than: date) -> str:
        full_name = Config.instance().storage_inventory_report_table_name
        if not full_name:
            msg = "Storage inventory report table name is not configured."
            raise Exception(msg)

        return f"""
            with all_paths as (
                select
                    replace(file_path, '{self.location_file_path}/data/', '') as file_path,
                    last_modified,
                    file_size_in_bytes
                from
                    {full_name}
                where
                    {self._all_file_paths_criterion(older_than)}
            ),
            all_folders as (
                select
                    distinct
                    file_path,
                    last_modified,
                    posexplode(split(file_path, '/')) as (level, folder)
                from
                    all_paths
                where
                    file_size_in_bytes = 0
                    and not {StorageInventoryReport.ICEBERG_FILE_CRITERION}
            ),
            folders_with_data as (
                select
                    file_path,
                    last_modified,
                    -- Remove the single file from array before exploding.
                    posexplode(slice(folders_and_file_array, 1, size(folders_and_file_array) -1)) as (level, folder)
                from (
                    select
                        file_path,
                        last_modified,
                        split(file_path, '/') as folders_and_file_array
                    from
                        all_paths
                    where
                        {StorageInventoryReport.ICEBERG_FILE_CRITERION}
                )
            ),
            empty_folders as (
                select
                    distinct a.file_path,
                    last_modified
                from
                    all_folders as a
                    left anti join folders_with_data as d
                    on (d.level = a.level and d.folder = a.folder)
            )

            select
                '{self.location_file_path}/data/' || file_path as file_path,
                last_modified
            from
                empty_folders
            """
