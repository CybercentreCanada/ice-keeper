from pyiceberg.table import Table
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType

from ice_keeper.catalog import load_table
from ice_keeper.config import Config
from ice_keeper.stm import STL, Scope

ZERO_EXPECTED = 0
ONE_EXPECTED = 1
TWO_EXPECTED = 2
THREE_EXPECTED = 3
FOUR_EXPECTED = 3
FIVE_EXPECTED = 5
SIX_EXPECTED = 6
SEVEN_EXPECTED = 7
TEN_EXPECTED = 10

TEST_CATALOG_NAME = "local"
TEST_SCHEMA_NAME = "test"
TEST_TABLE_NAME = "test"
TEST_FULL_NAME = f"{TEST_CATALOG_NAME}.{TEST_SCHEMA_NAME}.{TEST_TABLE_NAME}"
SCOPE_CATALOG = Scope(TEST_CATALOG_NAME)
SCOPE_SCHEMA = Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME)
SCOPE_TABLE = Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)
SCOPE_WHERE_FULL_NAME = Scope(where=f" full_name = '{TEST_FULL_NAME}' ")


def get_admin_catalog_and_schema() -> tuple[str, str]:
    catalog = Config.instance().maintenance_schedule_table_name.split(".")[0]
    schema = Config.instance().maintenance_schedule_table_name.split(".")[1]
    return (catalog, schema)


def set_tblproperty(key: str, value: str, table_name: str = TEST_TABLE_NAME) -> None:
    STL.sql(f"""alter table {TEST_CATALOG_NAME}.{TEST_SCHEMA_NAME}.{table_name} set tblproperties ("{key}"="{value}")""")


def set_tblproperties(properties: dict[str, str], table_name: str = TEST_TABLE_NAME) -> None:
    # Join the properties in the form of "key = 'value'"
    properties_str = ", ".join(f'"{key}" = "{value}"' for key, value in properties.items())
    STL.sql(f"""alter table {TEST_CATALOG_NAME}.{TEST_SCHEMA_NAME}.{table_name} set tblproperties ({properties_str})""")


inv_schema = StructType(
    [
        StructField("loaded_at", DateType()),
        StructField("last_modified", TimestampType()),
        StructField("storage_account", StringType()),
        StructField("container", StringType()),
        StructField("file_path", StringType()),
        StructField("file_size_in_bytes", LongType()),
    ]
)


def load_test_table(table_name: str = TEST_TABLE_NAME) -> Table:
    return load_table(TEST_CATALOG_NAME, TEST_SCHEMA_NAME, table_name)
