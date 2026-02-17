import logging
import threading

from pyspark.sql import DataFrame

from ice_keeper.config import Config, TemplateName
from ice_keeper.stm import STL, Scope

from .schedule_entry import MaintenanceScheduleEntry

logger = logging.getLogger("ice-keeper")


class PartitionHealth:
    partition_table_lock = threading.RLock()

    @classmethod
    def reset(cls) -> None:
        full_name = Config.instance().partition_health_table_name
        STL.sql_and_log(f"drop table if exists {full_name}", "Drop table")
        admin_table_notification_email = Config.instance().admin_table_notification_email
        # Ensures the base ends with a slash so the name is appended correctly
        table_location = Config.instance().get_table_location(full_name)
        template = Config.instance().load_template(TemplateName.PARTITION_HEALTH)
        sql = template.render(
            full_name=full_name,
            notification_email=admin_table_notification_email,
            location=table_location,
        )
        STL.sql_and_log(sql)

    def read(self, scope: Scope) -> DataFrame:
        sql_filter = scope.make_scoping_stmt()
        return STL.sql(f"select * from {Config.instance().partition_health_table_name}", "Read partition health").where(
            sql_filter
        )

    def write(self, before_view_name: str, after_view_name: str, mnt_props: MaintenanceScheduleEntry) -> None:
        insert_sql = f"""
            /* Writing health of partitions for table {mnt_props.full_name} */
            insert into {Config.instance().partition_health_table_name}
            select * from (
                select
                    now() as start_time,
                    '{mnt_props.full_name}' as full_name,
                    '{mnt_props.catalog}' as catalog,
                    '{mnt_props.schema}' as schema,
                    '{mnt_props.table_name}' as table_name,
                    b.partition_desc as partition_desc,
                    b.partition_age as partition_age,
                    struct(
                        b.n_files,
                        b.num_files_targetted_for_rewrite,
                        b.n_records,
                        b.avg_file_size,
                        b.min_file_size,
                        b.max_file_size,
                        b.sum_file_size,
                        b.corr) as before,
                    struct(
                        a.n_files,
                        a.num_files_targetted_for_rewrite,
                        a.n_records,
                        a.avg_file_size,
                        a.min_file_size,
                        a.max_file_size,
                        a.sum_file_size,
                        a.corr) as after

                from
                    {before_view_name} as b
                    left join {after_view_name} as a
                    on (b.partition_desc = a.partition_desc)
            )
            """

        desc = f"Storing partition health information for table [{mnt_props.full_name}]"
        with self.partition_table_lock:
            STL.sql_and_log(insert_sql, desc)
