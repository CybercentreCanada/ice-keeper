import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
import yaml
from jinja2 import TemplateNotFound

from ice_keeper.config import ICEKEEPER_TEMPLATE_DIR, Config, TemplateName


@pytest.fixture(autouse=True)
def reset_config() -> None:
    # override reset_config fixture with no-op
    pass


@pytest.fixture
def sample_config_dict() -> dict[str, str]:
    """Sample configuration dictionary."""
    return {
        "maintenance_schedule_table_name": "maintenance_schedule",
        "partition_health_table_name": "partition_health",
        "journal_table_name": "journal",
        "storage_inventory_report_table_name": "storage_inventory",
        "notification_email_fallback": "admin@example.com",
        "logging_config_file": "/path/to/logging.conf",
    }


@pytest.fixture
def sample_config_yaml(sample_config_dict: dict[str, str]) -> str:
    """Sample configuration as YAML string."""
    return yaml.dump(sample_config_dict)


@pytest.fixture
def temp_config_file(sample_config_yaml: str) -> Generator[str, None, None]:
    """Create a temporary config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(sample_config_yaml)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):  # noqa: PTH110
        os.unlink(temp_path)  # noqa: PTH108


@pytest.fixture
def temp_template_dir() -> Generator[str, None, None]:
    """Create a temporary template directory with a sample template."""
    temp_dir = tempfile.mkdtemp()
    template_file = Path(temp_dir) / "remove_orphan_files.sql.j2"
    template_file.write_text("SELECT * FROM {{ table_name }};")

    yield temp_dir

    # Cleanup
    import shutil  # noqa: PLC0415

    shutil.rmtree(temp_dir)


@pytest.fixture(autouse=True)
def reset_singleton() -> Generator[None, None, None]:
    """Reset the singleton instance before each test."""
    Config._instance = None
    yield
    Config._instance = None


@pytest.fixture(autouse=True)
def clear_env_vars() -> Generator[None, None, None]:
    """Clear relevant environment variables before each test."""
    env_vars: list[str] = [ICEKEEPER_TEMPLATE_DIR]
    original_values: dict[str, str | None] = {}

    for var in env_vars:
        original_values[var] = os.environ.pop(var, None)

    yield

    # Restore original values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        else:
            os.environ.pop(var, None)


class TestConfigLoading:
    """Test cases for loading configuration."""

    def test_load_config_basic(self, temp_config_file: str, sample_config_dict: dict[str, str]) -> None:
        """Test loading a basic config file."""
        config = Config.load_config(temp_config_file)

        assert config.maintenance_schedule_table_name == sample_config_dict["maintenance_schedule_table_name"]
        assert config.partition_health_table_name == sample_config_dict["partition_health_table_name"]
        assert config.journal_table_name == sample_config_dict["journal_table_name"]
        assert config.storage_inventory_report_table_name == sample_config_dict["storage_inventory_report_table_name"]
        assert config.notification_email_fallback == sample_config_dict["notification_email_fallback"]
        assert config.logging_config_file == sample_config_dict["logging_config_file"]
        assert config.template_dir is None

    def test_load_config_with_env_substitution(self, sample_config_dict: dict[str, str]) -> None:
        """Test loading config with environment variable substitution."""
        os.environ["TEST_TABLE_NAME"] = "test_maintenance"
        os.environ["TEST_EMAIL"] = "test@example.com"

        config_with_env: dict[str, str] = sample_config_dict.copy()
        config_with_env["maintenance_schedule_table_name"] = "${TEST_TABLE_NAME}"
        config_with_env["notification_email_fallback"] = "${TEST_EMAIL}"

        yaml_content: str = yaml.dump(config_with_env)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = Config.load_config(temp_path)
            assert config.maintenance_schedule_table_name == "test_maintenance"
            assert config.notification_email_fallback == "test@example.com"
        finally:
            os.unlink(temp_path)  # noqa: PTH108
            del os.environ["TEST_TABLE_NAME"]
            del os.environ["TEST_EMAIL"]

    def test_load_config_with_default_env_substitution(self, sample_config_dict: dict[str, str]) -> None:
        """Test loading config with default values in env substitution."""
        config_with_defaults: dict[str, str] = sample_config_dict.copy()
        config_with_defaults["maintenance_schedule_table_name"] = "${NONEXISTENT_VAR:-default_table}"

        yaml_content: str = yaml.dump(config_with_defaults)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = Config.load_config(temp_path)
            assert config.maintenance_schedule_table_name == "default_table"
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_load_config_missing_file(self) -> None:
        """Test loading a non-existent config file."""
        with pytest.raises(FileNotFoundError):
            Config.load_config("/nonexistent/path/config.yaml")

    def test_load_config_invalid_yaml(self) -> None:
        """Test loading invalid YAML content."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name

        try:
            with pytest.raises(yaml.YAMLError):
                Config.load_config(temp_path)
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_load_config_missing_required_fields(self) -> None:
        """Test loading config with missing required fields."""
        incomplete_config: dict[str, str] = {
            "maintenance_schedule_table_name": "test_table"
            # Missing other required fields
        }

        yaml_content: str = yaml.dump(incomplete_config)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with pytest.raises(Exception):  # Pydantic validation error  # noqa: B017, PT011
                Config.load_config(temp_path)
        finally:
            os.unlink(temp_path)  # noqa: PTH108


class TestSingletonBehavior:
    """Test cases for singleton pattern."""

    def test_singleton_instance(self, temp_config_file: str) -> None:
        """Test that load_config creates a singleton instance."""
        config1 = Config.load_config(temp_config_file)
        config2 = Config.instance()

        assert config1 is config2
        assert Config._instance is config1

    def test_instance_before_initialization(self) -> None:
        """Test calling instance() before load_config() raises error."""
        with pytest.raises(RuntimeError, match="Config not initialized"):
            Config.instance()


class TestTemplateDirectoryOverride:
    """Test cases for template directory override functionality."""

    def test_template_dir_from_env(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test template directory override from environment variable."""
        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)

        assert config.template_dir == temp_template_dir

    def test_template_dir_from_yaml(self, sample_config_dict: dict[str, str], temp_template_dir: str) -> None:
        """Test template directory override from YAML config."""
        config_with_override: dict[str, str] = sample_config_dict.copy()
        config_with_override["template_dir"] = temp_template_dir

        yaml_content: str = yaml.dump(config_with_override)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = Config.load_config(temp_path)
            assert config.template_dir == temp_template_dir
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_template_dir_no_override(self, temp_config_file: str) -> None:
        """Test that template_dir is None when not set."""
        config = Config.load_config(temp_config_file)
        assert config.template_dir is None


class TestTemplateSearchPaths:
    """Test cases for template search paths."""

    def test_template_search_paths_no_override(self, temp_config_file: str) -> None:
        """Test template search paths without override."""
        config = Config.load_config(temp_config_file)
        paths: list[Path] = config.template_search_paths

        assert len(paths) == 1
        assert paths[0].name == "templates"
        assert paths[0].parent.name == "ice_keeper"  # Assuming the module is named 'config'

    def test_template_search_paths_with_override(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test template search paths with valid override."""
        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)
        paths: list[Path] = config.template_search_paths

        assert len(paths) == 2  # noqa: PLR2004
        assert paths[0] == Path(temp_template_dir).resolve()
        assert paths[1].name == "templates"

    def test_template_search_paths_nonexistent_override(self, temp_config_file: str, caplog: pytest.LogCaptureFixture) -> None:
        """Test template search paths with non-existent override directory."""
        nonexistent_dir: str = "/nonexistent/template/directory"
        os.environ[ICEKEEPER_TEMPLATE_DIR] = nonexistent_dir

        config = Config.load_config(temp_config_file)
        paths: list[Path] = config.template_search_paths

        # Should only have the default path, not the nonexistent one
        assert len(paths) == 1
        assert paths[0].name == "templates"

        # Should log a warning
        assert "does not exist" in caplog.text

    def test_template_search_paths_override_priority(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test that override directory comes first in search paths."""
        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)
        paths: list[Path] = config.template_search_paths

        # Override should be first
        assert paths[0] == Path(temp_template_dir).resolve()


class TestLoadTemplate:
    """Test cases for loading templates."""

    def test_load_template_from_package(self, temp_config_file: str) -> None:
        """Test loading a template from the package directory."""
        config = Config.load_config(temp_config_file)

        # This test assumes the package has the actual template files
        # If not, you may need to mock or skip this test
        try:
            template = config.load_template(TemplateName.REMOVE_ORPHAN_FILES)
            assert template is not None
            assert template.name == TemplateName.REMOVE_ORPHAN_FILES.value
        except TemplateNotFound:
            pytest.skip("Package templates not available in test environment")

    def test_load_template_from_override(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test loading a template from override directory."""
        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)
        template = config.load_template(TemplateName.REMOVE_ORPHAN_FILES)

        assert template is not None
        rendered: str = template.render(table_name="test_table")
        assert "test_table" in rendered

    def test_load_template_override_takes_precedence(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test that override template takes precedence over package template."""
        # Create a distinctive template in override directory
        override_template = Path(temp_template_dir) / "remove_orphan_files.sql.j2"
        override_template.write_text("OVERRIDE: {{ table_name }}")

        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)
        template = config.load_template(TemplateName.REMOVE_ORPHAN_FILES)

        rendered: str = template.render(table_name="test")
        assert "OVERRIDE:" in rendered


class TestTemplateNameEnum:
    """Test cases for TemplateName enum."""

    def test_template_name_values(self) -> None:
        """Test that all template names have correct values."""
        assert TemplateName.REMOVE_ORPHAN_FILES.value == "remove_orphan_files.sql.j2"
        assert TemplateName.REWRITE_DATA_FILES.value == "rewrite_data_files.sql.j2"
        assert TemplateName.EXPIRE_SNAPSHOTS.value == "expire_snapshots.sql.j2"
        assert TemplateName.REWRITE_MANIFESTS.value == "rewrite_manifests.sql.j2"
        assert TemplateName.LIFECYCLE_DATA.value == "lifecycle_data.sql.j2"
        assert TemplateName.EMAIL_NOTIFICATION.value == "email_notification.html.j2"
        assert TemplateName.JOURNAL.value == "journal.sql.j2"
        assert TemplateName.SCHEDULE.value == "schedule.sql.j2"
        assert TemplateName.PARTITION_HEALTH.value == "partition_health.sql.j2"

    def test_template_name_count(self) -> None:
        """Test that we have the expected number of templates."""
        assert len(TemplateName) == 9  # noqa: PLR2004


class TestConfigValidation:
    """Test cases for Pydantic validation."""

    def test_config_with_extra_fields(self, sample_config_dict: dict[str, str]) -> None:
        """Test that extra fields are ignored (or raise error, depending on Pydantic config)."""
        config_with_extra: dict[str, str] = sample_config_dict.copy()
        config_with_extra["extra_field"] = "extra_value"

        yaml_content: str = yaml.dump(config_with_extra)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            # This might raise or ignore depending on Pydantic model config
            # Adjust based on your actual model configuration
            config = Config.load_config(temp_path)
            assert not hasattr(config, "extra_field")
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_config_field_types(self, temp_config_file: str) -> None:
        """Test that config fields have correct types."""
        config = Config.load_config(temp_config_file)

        assert isinstance(config.maintenance_schedule_table_name, str)
        assert isinstance(config.partition_health_table_name, str)
        assert isinstance(config.journal_table_name, str)
        assert isinstance(config.storage_inventory_report_table_name, str)
        assert isinstance(config.notification_email_fallback, str)
        assert isinstance(config.logging_config_file, str)
        assert config.template_dir is None or isinstance(config.template_dir, str)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_config_file(self) -> None:
        """Test loading an empty config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("")
            temp_path = f.name

        try:
            with pytest.raises(Exception):  # Should raise validation error  # noqa: B017, PT011
                Config.load_config(temp_path)
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_config_with_null_values(self, sample_config_dict: dict[str, str | None]) -> None:
        """Test config with null values for required fields."""
        config_with_nulls: dict[str, str | None] = sample_config_dict.copy()
        config_with_nulls["maintenance_schedule_table_name"] = None

        yaml_content: str = yaml.dump(config_with_nulls)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with pytest.raises(Exception):  # Should raise validation error  # noqa: B017, PT011
                Config.load_config(temp_path)
        finally:
            os.unlink(temp_path)  # noqa: PTH108

    def test_template_search_paths_called_multiple_times(self, temp_config_file: str, temp_template_dir: str) -> None:
        """Test that template_search_paths can be called multiple times consistently."""
        os.environ[ICEKEEPER_TEMPLATE_DIR] = temp_template_dir

        config = Config.load_config(temp_config_file)

        paths1: list[Path] = config.template_search_paths
        paths2: list[Path] = config.template_search_paths

        assert paths1 == paths2
