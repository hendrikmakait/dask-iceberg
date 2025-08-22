from functools import cached_property, lru_cache
from typing import Any

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs

from dask._task_spec import Task
from dask.dataframe.dask_expr._expr import PartitionsFiltered
from dask.dataframe.dask_expr.io.parquet import FragmentWrapper, _determine_type_mapper
from dask.typing import Key
from dask.utils import parse_bytes
from pyiceberg.manifest import FileFormat


class ReadIceberg(PartitionsFiltered):
    _parameters = ["table", "_partitions"]
    _defaults = {"_partitions": None}

    def columns(self):
        return self.table.metadata.schema().column_names

    @cached_property
    def _funcname(self):
        return "read-iceberg"

    @cached_property
    def _name(self):
        return f"{self._funcname}-{self.deterministic_token}"

    @property
    def _meta(self):
        return self.table.metadata.schema().as_arrow().empty_table().to_pandas()

    def _divisions(self):
        # TODO: Can we leverage sort order to get a better estimate?
        return (None,) * (len(self._scan().plan_files()) + 1)

    def _scan(self):
        # TODO: Implement filtering and projection
        return self.table.scan()

    def _filtered_task(self, name: Key, index: int) -> Task:
        # TODO: implement delete files
        arrow_format = pa_ds.ParquetFileFormat()
        fragment = arrow_format.make_fragment(
            self._scan().plan_files()[index].file.file_path, self.fs
        )
        return Task(
            name,
            ReadIceberg._table_to_pandas,
            Task(
                None,
                ReadIceberg._fragment_to_table,
                fragment_wrapper=FragmentWrapper(fragment, filesystem=self.fs),
                schema=self.table.metadata.schema().as_arrow(),
            ),
            _data_producer=True,
        )

    @staticmethod
    def _fragment_to_table(fragment_wrapper, schema):
        # Copied from dask.dataframe.dask_expr.io.ReadParquetPyarrowFS._fragment_to_table
        # _maybe_adjust_cpu_count()
        if isinstance(fragment_wrapper, FragmentWrapper):
            fragment = fragment_wrapper.fragment
        else:
            fragment = fragment_wrapper
        # if isinstance(filters, list):
        #     filters = pq.filters_to_expression(filters)
        return fragment.to_table(
            schema=schema,
            # columns=columns,
            # filter=filters,
            # Batch size determines how many rows are read at once and will
            # cause the underlying array to be split into chunks of this size~
            # (max). We'd like to avoid fragmentation as much as possible and
            # and to set this to something like inf but we have to set a finite,
            # positive number.
            # In the presence of row groups, the underlying array will still be
            # chunked per rowgroup
            batch_size=10_000_000,
            fragment_scan_options=pa.dataset.ParquetFragmentScanOptions(
                pre_buffer=True,
                cache_options=pa.CacheOptions(
                    hole_size_limit=parse_bytes("4 MiB"),
                    range_size_limit=parse_bytes("32.00 MiB"),
                ),
            ),
            # TODO: Reconsider this. The OMP_NUM_THREAD variable makes it harmful to enable this
            use_threads=True,
        )

    @staticmethod
    def _table_to_pandas(table):
        # TODO: Reimplement additional logic found in original
        df = table.to_pandas(
            types_mapper=_determine_type_mapper(
                user_types_mapper=None,
                dtype_backend=None,
                pyarrow_strings_enabled=None,
            ),
            use_threads=False,
            self_destruct=True,
            ignore_metadata=True,
        )
        return df

    @cached_property
    def fs(self):
        fs_input = None  # self.operand("filesystem")
        if isinstance(fs_input, pa.fs.FileSystem):
            return fs_input
        else:
            fs = pa_fs.FileSystem.from_uri(self.table.location())[0]
            # if storage_options := self.storage_options:
            #     # Use inferred region as the default
            #     region = {} if "region" in storage_options else {"region": fs.region}
            #     fs = type(fs)(**region, **storage_options)
            return fs


@lru_cache
def _get_file_format(
    file_format: FileFormat, **kwargs: dict[str, Any]
) -> pa_ds.FileFormat:
    if file_format == FileFormat.PARQUET:
        return pa_ds.ParquetFileFormat(**kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
