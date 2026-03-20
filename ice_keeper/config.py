import logging
import os
from enum import Enum
from pathlib import Path
from typing import ClassVar, Optional

import envsubst
import yaml
from jinja2 import Environment, FileSystemLoader, Template
from pydantic import BaseModel, Field

ICEKEEPER_CONFIG = "ICEKEEPER_CONFIG"

ICEKEEPER_TEMPLATE_DIR = "ICEKEEPER_TEMPLATE_DIR"

logger = logging.getLogger("ice-keeper")


class TemplateName(Enum):
    REMOVE_ORPHAN_FILES = "remove_orphan_files.sql.j2"
    REWRITE_DATA_FILES = "rewrite_data_files.sql.j2"
    EXPIRE_SNAPSHOTS = "expire_snapshots.sql.j2"
    REWRITE_MANIFESTS = "rewrite_manifests.sql.j2"
    LIFECYCLE_DATA = "lifecycle_data.sql.j2"
    EMAIL_NOTIFICATION = "email_notification.html.j2"
    JOURNAL = "journal.sql.j2"
    SCHEDULE = "schedule.sql.j2"
    PARTITION_HEALTH = "partition_health.sql.j2"


class Config(BaseModel):
    # Full table names for internal admin ice-keeper tables.
    maintenance_schedule_table_name: str
    partition_health_table_name: str
    journal_table_name: str
    # Optional location for internal admin tables. If not set, tables will be created in the default warehouse location of the catalog.
    # Setting this allows users to specify a custom location for the schema/database. To this location ice-keeper will add the table name.
    admin_table_location: str | None = None
    # Optional notification email for alerts about failed maintenance tasks on ice-keeper's internal admin tables.
    admin_table_notification_email: str | None = None
    # Full table name for the storage inventory report, which is used to track files in the data lake and identify orphans.
    # If not set, orphan file procedure will default back to using Spark APIs to list files, which may be less efficient at scale.
    storage_inventory_report_table_name: str | None = None
    # If no notification email provided by a table owner, use this fallback email for admin alerts about failed maintenance tasks.
    notification_email_fallback: str | None = None
    # Location of the logging configuration file, which will be used to configure logging for ice-keeper.
    logging_config_file: str | None = None

    # Template fields
    # Defaulting to an environment variable check directly in the Field
    template_dir: str | None = Field(default_factory=lambda: os.getenv(ICEKEEPER_TEMPLATE_DIR))

    # Optional SMTP configuration for email notifications
    smtp_server: str | None = None
    smtp_port: int | None = None
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_email_from: str | None = None
    smtp_tls: bool = False  # Whether to use TLS for SMTP connection

    # Class variable to store the singleton (not part of the Pydantic schema)
    _instance: ClassVar[Optional["Config"]] = None

    @classmethod
    def load_config(cls, file_path: str) -> "Config":
        with Path(file_path).open("r") as f:
            content = f.read()
            # This will replace ALL ${VAR} and ${VAR:-default} in the text
            expanded_content = envsubst.envsubst(content)
            # Load YAML into a dict
            raw_config = yaml.safe_load(expanded_content)

        if not isinstance(raw_config, dict):
            msg = f"Invalid configuration format in {file_path}: expected a mapping, got {type(raw_config).__name__}"
            raise ValueError(msg)

        # Unpack dict into Pydantic model for validation
        cls._instance = cls(**raw_config)
        return cls._instance

    @classmethod
    def instance(cls) -> "Config":
        """Returns the singleton instance."""
        if cls._instance is None:
            msg = "Config not initialized. Call Config.load_config() first."
            raise RuntimeError(msg)
        return cls._instance

    @property
    def template_search_paths(self) -> list[Path]:
        """Returns a list of paths to search for templates, override first."""
        # 1. Start with the internal package directory
        package_root = Path(__file__).parent / "templates"
        paths = [package_root]

        # 2. If user provided an override (via YAML or Env), prepend it
        if self.template_dir:
            override_path = Path(self.template_dir).resolve()
            if override_path.exists():
                paths.insert(0, override_path)
            else:
                msg = f"Template override path {override_path} does not exist."
                logger.warning(msg)

        return paths

    def load_template(self, template_name: TemplateName) -> Template:
        # FileSystemLoader accepts a list of strings/paths
        loader = FileSystemLoader([str(p) for p in self.template_search_paths])
        environment = Environment(loader=loader, autoescape=False)  # noqa: S701
        return environment.get_template(template_name.value)

    def get_table_location(self, full_table_name: str) -> str | None:
        """Constructs the table location based on the admin_table_location config and the provided full table."""
        table_location = None
        if self.admin_table_location:
            base = self.admin_table_location if self.admin_table_location.endswith("/") else self.admin_table_location + "/"
            table_name = full_table_name.split(".")[-1]
            table_location = base + table_name
        return table_location
