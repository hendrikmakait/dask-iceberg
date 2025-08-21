from functools import cached_property

from dask._task_spec import Task
from dask.dataframe.dask_expr._expr import PartitionsFiltered
from dask.typing import Key


class ReadIceberg(PartitionsFiltered):
    _parameters = ["table", "_partitions"]
    _defaults = {"_partitions": None}

    def columns(self):
        return self.table.metadata.schema.column_names()

    @cached_property
    def _funcname(self):
        return "read-iceberg"

    @cached_property
    def _name(self):
        return f"{self._funcname}-{self.deterministic_token}"

    def _meta(self):
        return self.table.metadata.schema.as_arrow().empty_table().to_pandas()

    def _filtered_task(self, name: Key, index: int) -> Task:
        return super()._filtered_task(name, index)
