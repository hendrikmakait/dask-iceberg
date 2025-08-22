import contextlib
import os

from pathlib import Path

import pytest

from dask.dataframe import assert_eq
from pyiceberg.table import StaticTable

import dask_iceberg

from dask_iceberg._read import ReadIceberg


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
    assert_eq(df, pdf, check_index=False)


def test_read_iceberg_snapshot_id(iceberg_path: str) -> None:
    table = StaticTable.from_metadata(iceberg_path)
    pdf = table.scan(snapshot_id=7051579356916758811).to_pandas()

    df = dask_iceberg.read_iceberg(table, snapshot_id=7051579356916758811)
    assert_eq(df, pdf, check_index=False)


def test_read_iceberg_snapshot_id_not_found(iceberg_path: str) -> None:
    table = StaticTable.from_metadata(iceberg_path)

    with pytest.raises(ValueError, match="snapshot ID not found"):
        dask_iceberg.read_iceberg(table, snapshot_id=1234567890).compute()


def test_read_iceberg_column_projection_pushdown(iceberg_path: str) -> None:
    table = StaticTable.from_metadata(iceberg_path)
    pdf = table.scan(selected_fields=("id", "str")).to_pandas()

    df = dask_iceberg.read_iceberg(table)[["id", "str"]]
    optimized = df.optimize(fuse=False)
    print(optimized)
    assert isinstance(optimized.expr, ReadIceberg)
    assert list(optimized.columns) == ["id", "str"]
    assert_eq(df, pdf, check_index=False)


def test_read_iceberg_filter_on_column(iceberg_path: str) -> None:
    table = StaticTable.from_metadata(iceberg_path)
    pdf = table.scan(row_filter="id < 2").to_pandas()
    df = dask_iceberg.read_iceberg(table)
    df = df[df["id"] < 2]
    optimized = df.optimize(fuse=False)
    print(optimized)
    assert isinstance(optimized.expr, ReadIceberg)
    assert_eq(df, pdf, check_index=False)
    # assert res.collect().rows() == [(1, "1", datetime(2023, 3, 1, 18, 15))]

    # res = lf.filter(pl.col("id") == 2)
    # assert res.collect().rows() == [(2, "2", datetime(2023, 3, 1, 19, 25))]

    # res = lf.filter(pl.col("id").is_in([1, 3]))
    # assert res.collect().rows() == [
    #     (1, "1", datetime(2023, 3, 1, 18, 15)),
    #     (3, "3", datetime(2023, 3, 2, 22, 0)),
    # ]
