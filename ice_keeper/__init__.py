import logging
import logging.config
import os
import re
import textwrap
from datetime import date, datetime, timezone
from enum import Enum
from functools import cache
from pathlib import Path
from urllib.parse import urlparse

import envsubst
import yaml
from pyiceberg.table import TableProperties

from ice_keeper.config import Config


class IceKeeperTblProperty:
    """Static class containing constants that represent Iceberg table property keys used in the IceKeeper project.

    These are utilized to extract and store configuration related to table management.
    """

    NAMESPACE_PREFIX = "ice-keeper"

    SHOULD_EXPIRE_SNAPSHOTS = "ice-keeper.should-expire-snapshots"
    RETENTION_DAYS_SNAPSHOTS = "ice-keeper.retention-days-snapshots"
    SHOULD_REMOVE_ORPHAN_FILES = "ice-keeper.should-remove-orphan-files"
    RETENTION_DAYS_ORPHAN_FILES = "ice-keeper.retention-days-orphan-files"
    SHOULD_OPTIMIZE = "ice-keeper.should-optimize"
    OPTIMIZATION_STRATEGY = "ice-keeper.optimization-strategy"
    MIN_AGE_TO_OPTIMIZE = "ice-keeper.min-age-to-optimize"
    MAX_AGE_TO_OPTIMIZE = "ice-keeper.max-age-to-optimize"
    OPTIMIZATION_TARGET_FILE_SIZE_BYTES = "ice-keeper.optimization-target-file-size-bytes"
    OPTIMIZE_PARTITION_DEPTH = "ice-keeper.optimize-partition-depth"
    RETENTION_NUM_SNAPSHOTS = "ice-keeper.retention-num-snapshots"
    SHOULD_REWRITE_MANIFEST = "ice-keeper.should-rewrite-manifest"
    NOTIFICATION_EMAIL = "ice-keeper.notification-email"
    SHOULD_APPLY_LIFECYCLE = "ice-keeper.should-apply-lifecycle"
    LIFECYCLE_MAX_DAYS = "ice-keeper.lifecycle-max-days"
    LIFECYCLE_INGESTION_TIME_COLUMN = "ice-keeper.lifecycle-ingestion-time-column"
    HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS = TableProperties.MAX_SNAPSHOT_AGE_MS  # Iceberg property
    HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP = TableProperties.MIN_SNAPSHOTS_TO_KEEP  # Iceberg property
    WRITE_TARGET_FILE_SIZE_BYTES = TableProperties.WRITE_TARGET_FILE_SIZE_BYTES  # Iceberg property
    WIDENING_RULE_SELECT_CRITERIA = "ice-keeper.widening.rule.select.criteria"
    WIDENING_RULE_REQUIRED_PARTITION_COLUMNS = "ice-keeper.widening.rule.required_partition_columns"
    WIDENING_RULE_SRC_PARTITION = "ice-keeper.widening.rule.src.partition"
    WIDENING_RULE_DST_PARTITION = "ice-keeper.widening.rule.dst.partition"
    WIDENING_RULE_MIN_AGE_TO_WIDEN = "ice-keeper.widening.rule.min.age.to.widen"

    @classmethod
    @cache
    def get_values(cls) -> list[str]:
        """Returns a list of all property values defined in the class."""
        return [
            value
            for key, value in vars(cls).items()
            if not key.startswith("__")
            and not callable(value)  # Skip methods/functions
            and not isinstance(value, classmethod)  # Skip classmethods explicitly
            and key != cls.NAMESPACE_PREFIX
        ]


# Define a function to map log level strings to numeric values
def get_log_level(level_str: str) -> int:
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return levels.get(level_str.lower(), logging.NOTSET)


def configure_logger() -> None:
    if config_file := Config.instance().logging_config_file:
        with Path(config_file).open("r") as file:
            content = file.read()
            # This will replace ALL ${VAR} and ${VAR:-default} in the text
            expanded_content = envsubst.envsubst(content)
            config = yaml.safe_load(expanded_content)
            logging.config.dictConfig(config)
    else:
        # Default logging configuration if no config file is provided
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )


class FileScheme(Enum):
    FILE = "file"
    ABFSS = "abfss"
    S3 = "s3"

    @classmethod
    def is_valid_scheme(cls, path: str) -> bool:
        """Checks if the path starts with any of the defined schemes."""
        scheme = urlparse(path).scheme
        return scheme in {s.value for s in cls}

    @classmethod
    def from_url(cls, path: str) -> "FileScheme | None":
        """Returns the matching scheme if the path starts with one, otherwise None."""
        scheme = urlparse(path).scheme
        return cls(scheme) if scheme in {s.value for s in cls} else None

    def is_match(self, path: str) -> bool:
        """Checks if the path starts with this specific scheme."""
        return self.value == urlparse(path).scheme

    def remove_scheme(self, path: str) -> str:
        """Removes the scheme from the path if it starts with this scheme."""
        parsed = urlparse(path)
        if parsed.scheme == self.value:
            return parsed.path.lstrip("/")
        return path


class Command(Enum):
    OPTIMIZE = "optimize"
    EXPIRE = "expire"
    REWRITE_MANIFESTS = "rewrite_manifests"
    ORPHAN = "orphan"
    LIFECYCLE_DATA = "lifecycle"
    AUDIT_CONFIG = "audit_config"

    @classmethod
    def from_string(cls, value: str) -> "Command":
        """Convert a string to the corresponding Command enum, if valid."""
        try:
            return cls(value)
        except ValueError:
            msg = f"'{value}' is not a valid Command name. Valid options are: {[c.value for c in cls]}"
            raise ValueError(msg) from None

    @classmethod
    def from_strings(cls, values: tuple[str]) -> set["Command"]:
        return {Command.from_string(value) for value in values}


class Action(Enum):
    REWRITE_DATA_FILES = "rewrite_data_files"
    EXPIRE_SNAPSHOTS = "expire_snapshots"
    REWRITE_MANIFESTS = "rewrite_manifests"
    REMOVE_ORPHAN_FILES = "remove_orphan_files"
    LIFECYCLE_DATA = "lifecycle_data"
    DISCOVERY = "discovery"
    CONFIG_AUDITOR = "config_auditor"

    # Add a mapping from Command to Action as a class method
    @classmethod
    def from_command(cls, command: Command) -> "Action":
        command_to_action = {
            Command.OPTIMIZE: cls.REWRITE_DATA_FILES,
            Command.EXPIRE: cls.EXPIRE_SNAPSHOTS,
            Command.REWRITE_MANIFESTS: cls.REWRITE_MANIFESTS,
            Command.ORPHAN: cls.REMOVE_ORPHAN_FILES,
            Command.LIFECYCLE_DATA: cls.LIFECYCLE_DATA,
            Command.AUDIT_CONFIG: cls.CONFIG_AUDITOR,
        }
        if command in command_to_action:
            return command_to_action[command]
        msg = f"No corresponding Action found for Command: {command}"
        raise ValueError(msg)


class ActionWarning(Exception):  # noqa: N818
    """Exception raised to log a warning in the journal."""


class ActionFailed(Exception):  # noqa: N818
    """Exception raised to log a failure in the journal."""


class Status(Enum):
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    FAILED = "FAILED"


def should_escape(identifier: str) -> bool:
    return bool(re.search(r"[^a-zA-Z0-9_]", identifier)) and not (identifier.startswith("`") and identifier.endswith("`"))


def escape_identifier(identifier: str) -> str:
    """Check if identifier contains spaces or special characters.

    Backticks inside a name: Spark SQL does not generally support backticks within
    a single identifier (table or column name) as a valid part of the name itself.
    If a column name from a source includes a backtick, Spark may throw an AnalysisException.
    It is best practice to rename such columns if possible.
    """
    if should_escape(identifier):
        return f"`{identifier}`"

    return identifier


def quote_literal_value(value: str) -> str:
    """Quote a string literal escaping any quotes found in the value."""
    # First escape backslashes, then escape single quotes using SQL-style doubling.
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def dedent(sql: str) -> str:
    return textwrap.dedent(sql)


def get_user_name() -> str:
    return os.getenv("USER", "UNKNOWN")


class TimeProvider:
    """Provides current time."""

    fixed_time: None | datetime = None

    @classmethod
    def set(cls, fixed_time: datetime) -> None:
        cls.fixed_time = fixed_time

    @classmethod
    def current_datetime(cls) -> datetime:
        if cls.fixed_time:
            return cls.fixed_time
        return datetime.now(tz=timezone.utc)

    @classmethod
    def current_date(cls) -> date:
        if cls.fixed_time:
            return cls.fixed_time.date()
        return datetime.now(tz=timezone.utc).date()
