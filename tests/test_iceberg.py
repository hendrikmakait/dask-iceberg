import contextlib
import os

from pathlib import Path

import pytest

from dask.dataframe import assert_eq
from pyiceberg.table import StaticTable

import dask_iceberg


@pytest.fixture
def iceberg_path() -> str:
    # Iceberg requires absolute paths, so we'll symlink
    # the test table into /tmp/iceberg/t1/
    Path("/tmp/iceberg").mkdir(parents=True, exist_ok=True)
    current_path = Path(__file__).parent.resolve()

    with contextlib.suppress(FileExistsError):
        os.symlink(f"{current_path}/files/iceberg-table", "/tmp/iceberg/t1")

    iceberg_path = (
        Path(__file__).parent
        / "files"
        / "iceberg-table"
        / "metadata"
        / "v2.metadata.json"
    )
    return f"file://{iceberg_path.resolve()}"


def test_read_iceberg_plain(iceberg_path: str) -> None:
    table = StaticTable.from_metadata(iceberg_path)
    pdf = table.scan().to_pandas()
    df = dask_iceberg.read_iceberg(table)
    assert_eq(df, pdf)
