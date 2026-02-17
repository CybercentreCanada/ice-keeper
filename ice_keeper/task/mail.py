import dataclasses
import logging
import smtplib
from email.message import EmailMessage

from pyspark.sql import DataFrame, Row

from ice_keeper import Status
from ice_keeper.config import Config, TemplateName
from ice_keeper.output import rows_log_debug
from ice_keeper.stm import STL, Scope
from ice_keeper.table import Journal, MaintenanceSchedule

logger = logging.getLogger("ice-keeper")

NUMBER_OF_ROWS_IN_PREVIEW = 50


@dataclasses.dataclass
class Emailer:
    maintenance: MaintenanceSchedule
    should_send_emails: bool = False

    @classmethod
    def notification_email_fallback(cls) -> str:
        return Config.instance().notification_email_fallback

    def _get_failed_journal_entries_df(
        self,
        scope: Scope,
        last_num_days: int = 1,
    ) -> DataFrame:
        journal_entries_sql = Journal().read_sql(scope)
        maintenance_sql = self.maintenance.read_sql()

        sql = f"""
            with
            journal_entries as (
                /* only consider journal entries in date range */
                select
                    *
                from
                    ({journal_entries_sql})
                where
                    start_time >= current_date() - interval '{last_num_days - 1}' days
                    and status = '{Status.FAILED.value}'
            ),
            failed_by_day as (
                /* Find max start_time per day, this deals with multiple ice-keeper runs in a day */
                select
                    max(start_time) as start_time,
                    full_name,
                    action,
                    sql_stm
                from
                (
                    select
                        start_time,
                        full_name,
                        action,
                        /* Check that the exact same partition failed to rewrite */
                        case
                            when action = 'rewrite_data_files' then sql_stm
                            else ''
                        end as sql_stm
                    from
                        journal_entries
                )
                group by
                    date(start_time),
                    full_name,
                    action,
                    sql_stm
            ),
            failed_every_day as (
                /* Find actions that failed every single day in the date range */
                select
                    max(start_time) as start_time,
                    full_name,
                    action,
                    sql_stm
                from
                    failed_by_day
                group by
                    full_name,
                    action,
                    sql_stm
                having
                    count(*) = {last_num_days}
            ),
            /* Add status to the actions that failed every day in the date range */
            failed_journal_entries as (
                select
                    count(*) over(),
                    j.full_name,
                    j.action,
                    j.start_time,
                    j.exec_time_seconds,
                    j.sql_stm,
                    j.status
                from
                    journal_entries as j
                    join failed_every_day as f on(
                        j.full_name = f.full_name
                        and j.action = f.action
                        and j.start_time = f.start_time
                    )
            ),
            final as (
                select
                    if(len(s.notification_email) > 0, s.notification_email, '{self.notification_email_fallback()}') as notification_email,
                    f.full_name,
                    f.action,
                    from_unixtime(
                        unix_timestamp(f.start_time, 'yyyy-MM-dd HH:mm:ss')
                    ) as start_time,
                    f.sql_stm,
                    f.status
                from
                    failed_journal_entries as f
                    join ({maintenance_sql}) as s on (f.full_name = s.full_name)
                order by
                    1,
                    2,
                    3,
                    4
                )

            select * from final
          """

        journal_df = STL.sql_and_log(sql, "Get failed journal entries")
        rows_log_debug(journal_df.collect(), "Get failed executions")
        return journal_df

    def send_notifications(
        self,
        scope: Scope,
        last_num_days: int = 1,
    ) -> None:
        journal_df = self._get_failed_journal_entries_df(scope, last_num_days)
        rows = journal_df.collect()
        if len(rows) > 0:
            previous_email = rows[0].notification_email
            start_idx = 0
            for idx in range(len(rows)):
                # If user is changing
                current_email = rows[idx].notification_email
                if previous_email != current_email:
                    previous_user_rows = rows[start_idx:idx]
                    self._send_notification(previous_email, previous_user_rows)
                    start_idx = idx
                    previous_email = current_email
            # email last user
            previous_user_rows = rows[start_idx : len(rows)]
            self._send_notification(previous_email, previous_user_rows)

    def _send_notification(self, email: str, rows: list[Row]) -> None:
        logger.debug("Executing _send_notification with email: %s; and rows: %s", email, rows)
        subject = "ice-keeper failed tasks"

        max_failed_tasks_in_preview = len(rows)

        # only send first NUMBER_OF_ROWS_IN_PREVIEW rows
        if max_failed_tasks_in_preview > NUMBER_OF_ROWS_IN_PREVIEW:
            rows = rows[0:NUMBER_OF_ROWS_IN_PREVIEW]
        context = {
            "rows": rows,
            "actual_num_failed_tasks": max_failed_tasks_in_preview,
            "max_failed_tasks_in_preview": NUMBER_OF_ROWS_IN_PREVIEW,
        }
        template = Config.instance().load_template(TemplateName.EMAIL_NOTIFICATION)
        html_body = template.render(context)

        logger.debug("Configured with should_send_emails: %s", self.should_send_emails)
        if self.should_send_emails:
            emails = email.split(",")
            for email_address in emails:
                logger.debug("Sending email to: %s", email_address)
                self.send_email(email_address.strip(), subject, html_body)
        else:
            self.print_email(email, subject, html_body)

    def send_email(self, email: str, subject: str, html_body: str) -> None:
        server = Config.instance().smtp_server
        port = Config.instance().smtp_port
        username = Config.instance().smtp_username
        password = Config.instance().smtp_password
        from_address = Config.instance().smtp_email_from
        tls = Config.instance().smtp_tls

        missing_fields = []
        if not server:
            missing_fields.append("server")
        if not port:
            missing_fields.append("port")
        if not from_address:
            missing_fields.append("from")
        if missing_fields:
            msg = f"SMTP configuration is incomplete. Cannot send email. Missing settings: {', '.join(missing_fields)}."
            raise ValueError(msg)

        email_msg = EmailMessage()
        email_msg["Subject"] = subject
        email_msg["From"] = from_address
        email_msg["To"] = email

        email_msg.set_content(html_body, subtype="html")

        try:
            assert server, "SMTP server is not configured"
            assert port, "SMTP port is not configured"
            with smtplib.SMTP(server, port) as smtp:
                if tls:
                    smtp.starttls()  # Upgrade to a secure connection
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(email_msg)
                logger.debug("Email sent successfully to %s", email)
        except Exception as e:  # noqa: BLE001
            logger.error("Error sending email: %s", e)

    def print_email(self, email: str, subject: str, body: str) -> None:
        logger.debug("Would send email: %s, subject: %s, body: %s", email, subject, body)
