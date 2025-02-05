import pyarrow as pa
from pyiceberg.catalog import load_catalog

def write_iceberg(
    arrow_table: pa.Table,
    catalog_name: str,
    table_identifier: str,
    catalog_config: dict = None
) -> None:
    """
    Writes a PyArrow Table to an Apache Iceberg table.

    Args:
        arrow_table: Input PyArrow Table to be written
        catalog_name: Name of the Iceberg catalog to use
        table_identifier: Namespace and table name (e.g., 'database.my_table')
        catalog_config: Optional configuration for the catalog
    """
    # Iceberg catalog
    catalog = load_catalog(catalog_name, **(catalog_config or {}))
    
    # Iceberg table
    table = catalog.load_table(table_identifier)
    
    # Arrow Table to PyArrow Dataset
    dataset = pa.dataset.dataset(arrow_table)
    
    # Iceberg transaction
    with table.transaction() as tx:
        tx.append_data(dataset)