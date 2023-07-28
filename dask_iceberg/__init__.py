from dask_iceberg import _version

__version__ = _version.get_versions()["version"]

from dask_expr._collection import new_collection as _new_collection

from dask_iceberg._from_iceberg import FromIceberg


def from_iceberg(table_scan):
    return _new_collection(FromIceberg(table_scan))
