from pyspark.sql import Row

from ice_keeper import Status
from ice_keeper.config import Config, TemplateName


def test_mail_generation() -> None:
    sql_stm = """
        call catalog1.system.expire_snapshots(
                table => 'schema1.table_name1',
                older_than => timestamp '2025-01-15 19:07:46.620488',
                retain_last => 60,
                stream_results => true)
    """
    row = Row(
        start_time="2024-01-01 00:00:00",
        full_name="cat1.schema1.table1",
        action="expire_snapshots",
        sql_stm=sql_stm,
        status=Status.FAILED.value,
    )
    rows = [row]
    context = {
        "rows": rows,
        "actual_num_failed_tasks": 1,
        "max_failed_tasks_in_preview": 10,
    }
    template = Config.instance().load_template(TemplateName.EMAIL_NOTIFICATION)
    html_body = template.render(context)
    assert "FAILED" in html_body
    assert "expire_snapshots" in html_body
    assert "cat1.schema1.table1" in html_body
