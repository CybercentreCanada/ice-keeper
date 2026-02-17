import difflib
import logging
from typing import Any

from typing_extensions import override

from ice_keeper import Action, ActionWarning, IceKeeperTblProperty
from ice_keeper.catalog import load_table
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class ConfigAuditorStrategy(ActionStrategy):
    @override
    @classmethod
    def get_action(cls) -> Action:
        return Action.CONFIG_AUDITOR

    @override
    def task_description(self, full_name: str) -> str:
        return f"ConfigAuditor for table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        return True

    @override
    def prepare_statement_to_execute(self) -> str:
        return ""

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        table = load_table(self.mnt_props.catalog, self.mnt_props.schema, self.mnt_props.table_name)
        warnings = self._check_config(table.properties)
        if len(warnings) > 0:
            msg = str(warnings)
            raise ActionWarning(msg)
        self.disable_journaling()
        return {}

    def _check_config(self, user_config: dict[str, str]) -> list[str]:
        warnings = []

        for key in user_config:
            if key in IceKeeperTblProperty.get_values():
                continue

            # Step 3: If the key is invalid, find closely matching valid keys
            suggestions = difflib.get_close_matches(key, IceKeeperTblProperty.get_values(), n=1, cutoff=0.8)
            if suggestions:
                warnings.append(f"Possible typo: '{key}'. Did you mean '{suggestions[0]}'?")

        return warnings

    def _check_config2(self, user_config: dict[str, str]) -> list[str]:
        """Checks user-provided configurations for typos or invalid settings.

        :param user_config: A dictionary of user-provided configurations.
        :return: A list of warnings for misconfigured keys.
        """
        warnings = []

        for key in user_config:
            # Step 1: Check if the key starts with the namespace or a close variant
            if not self._is_relevant_key(key):
                continue  # Skip unrelated keys

            # Step 2: Check if the key matches a valid configuration
            if key in IceKeeperTblProperty.get_values():
                continue

            # Step 3: If the key is invalid, find closely matching valid keys
            suggestions = difflib.get_close_matches(key, IceKeeperTblProperty.get_values(), n=1, cutoff=0.8)
            if suggestions:
                warnings.append(f"Possible typo: '{key}'. Did you mean '{suggestions[0]}'?")
            else:
                warnings.append(f"Unknown configuration key: '{key}' is not valid in the system.")

        return warnings

    def _is_relevant_key(self, key: str) -> bool:
        """Check if a key starts with the namespace or a close variant.

        :param key: The configuration key to test.
        :return: True if the key is relevant, False otherwise.
        """
        if key.startswith(IceKeeperTblProperty.NAMESPACE_PREFIX):
            return True
        # Normalize the key and namespace by replacing underscores (_) with hyphens (-)
        normalized_key = key.replace("_", "-")

        # Step 2: Perform comparison only on the prefix length
        # Extract the prefix of the key based on the length of the namespace
        key_prefix = normalized_key[: len(IceKeeperTblProperty.NAMESPACE_PREFIX)]

        # Check close matches to the namespace prefix, e.g., to catch typos like "ice_keeper"
        prefix_suggestion = difflib.get_close_matches(key_prefix, [IceKeeperTblProperty.NAMESPACE_PREFIX], n=1, cutoff=0.8)
        if len(prefix_suggestion) > 0:
            return True

        iceberg_native_properties_used_by_icekeeper = [
            IceKeeperTblProperty.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS,
            IceKeeperTblProperty.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP,
            IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES,
        ]

        prefix_suggestion = difflib.get_close_matches(
            normalized_key, iceberg_native_properties_used_by_icekeeper, n=1, cutoff=0.8
        )
        return len(prefix_suggestion) > 0
