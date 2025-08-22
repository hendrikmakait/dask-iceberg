from dask.dataframe import DataFrame
from dask.dataframe.api import new_collection
from pyiceberg.table import Table

from dask_iceberg._read import ReadIceberg


def read_iceberg(
    table: Table,
    snapshot_id: int | None = None,
) -> DataFrame:
    if snapshot_id is not None:
        snapshot = table.snapshot_by_id(snapshot_id)

        if snapshot is None:
            raise ValueError(f"iceberg snapshot ID not found: {snapshot_id}")

    return new_collection(ReadIceberg(table, snapshot_id))
