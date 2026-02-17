from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

CATALOGS: dict[str, Catalog] = {}


def load_catalog_from_cache(catalog_name: str) -> Catalog:
    """Load the catalog from the cache.

    Args:
        catalog_name (str): The name of the catalog to load.
    """
    if catalog_name not in CATALOGS:
        CATALOGS[catalog_name] = load_catalog(catalog_name)
    return CATALOGS[catalog_name]


def load_table(catalog_name: str, schema: str, table_name: str) -> Table:
    """Load the table's metadata and returns the table instance.

    Args:
        catalog_name (str): Catalog name.
        schema (str): Schema name.
        table_name (str): Table name.

    Returns:
        Table: the table instance with its metadata.

    Raises:
        NoSuchTableError: If a table with the name does not exist.
        Exception: If catalog does not exists.
    """
    catalog = load_catalog_from_cache(catalog_name)
    identifier = (schema, table_name)
    return catalog.load_table(identifier)
