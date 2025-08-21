from dask.dataframe import DataFrame
from dask.dataframe.api import new_collection
from pyiceberg.table import Table

from dask_iceberg._read import ReadIceberg


def read_iceberg(
    table: Table,
) -> DataFrame:
    return new_collection(ReadIceberg(table))
