"""
This module contains the class for the :class:`~MetadataInitializer` and all metadata-related tasks.
"""

from enum import Enum
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import yaml
from loguru import logger
from typing_extensions import override

from xflow.context import JobContext
from xflow.task import TaskResultFunction
from xflow.task import TaskResultIterator
from xflow.task import TaskResultType
from xflow.task import TaskType
from xnl.metadata.base import Metadata
from xnl.metadata.metadata import DimensionMetadata
from xnl.metadata.metadata import TableMetadata
from xnl.table_builder.base import PipelineReturnBuilder
from xnl.table_builder.mixin import ExternalFileLoaderMixin
from xnl.table_builder.mixin import ResultNameMixin


class MetadataType(Enum):
    """
    Enum of available concrete :class:`~Metadata` classes.
    """

    TABLE = TableMetadata
    DIMENSION = DimensionMetadata


class MetadataInitializer(PipelineReturnBuilder, ExternalFileLoaderMixin, ResultNameMixin):
    """
    One of the builder classes which represents loading of table schema and other metadata needed for the entire job.
    """

    LOADER_PATH: Optional[Path] = None

    def __init__(self):
        super().__init__()
        self._init_loader("yaml")
        self.metadata_cls: Type[Metadata] = MetadataType.TABLE.value
        self.add_pre_processing(lambda job_name: {"metadata_name": job_name})

    def with_type(self, metadata_type: MetadataType):
        """
        Sets the metadata type of the object.

        :param metadata_type: The metadata type.
        :return:
        """
        return self.copy_on_write(self.metadata_cls, metadata_type.value)

    def from_raw(self):
        """
        Retrieves all the metadata yaml file directly from a directory defined in the :meth:`~set_loader_path` then
        loads it.

        :return:
        """

        def _from_raw() -> List[TableMetadata]:
            logger.info("Loading table metadata from raw task definition")
            metadata = [self.metadata_cls.from_dict(yaml.safe_load(file)) for file in self.loaded_files]

            return metadata

        return self._set_main_function(_from_raw)

    def from_job_context(self, src_key: str = None):
        """
        Retrieves a single metadata from a dictionary of metadata loaded by :class:`~JobContext`. The job name retrieve
        by the function handles "_hist_" and "dim_" related metadata.

        :param src_key: The key of the metadata. If this is not specified, it will be equal to the job name. Otherwise,
            you can provide a literal string if you know the exact key, or provide a format which involves the job name
            variable enclosed in brackets, i.e. foo/{metadata_name}/bar.
        :param: job_name - The metadata key or filename is expected also to be the job name.
        :return:
        """

        def _from_job_context(src_table_schemas: Dict[str, Any], metadata_name: str, src_key: str) -> TableMetadata:
            # Make historical jobs refer to the same schema
            metadata_name = metadata_name.replace("_hist_", "_")

            if metadata_name.startswith("dim_"):
                self.metadata_cls = MetadataType.DIMENSION.value

            metadata_name = f"{src_key.replace('{metadata_name}', f'{metadata_name}')}"
            logger.info(f"Loading table metadata from job context for job: {metadata_name}")
            metadata = self.metadata_cls.from_dict(src_table_schemas[metadata_name])

            return metadata

        return self._set_main_function(_from_job_context, src_key=src_key or "{metadata_name}")

    @override
    def _generate_task_fn(self) -> TaskResultFunction:
        def task_fn(context: JobContext) -> TaskResultIterator:
            metadata, output_dict = self._main_fn_with_pre_processing(context)

            for function in self._post_processing_fns:
                output = self._execute_subfunction(function, metadata)
                if self._update_all_kwargs(output, raise_error=False):
                    output_dict.update(output)

            output_dict.update({self.result_name: metadata})
            yield self.task_result_type(self.result_name, output_dict)

        return task_fn

    @override
    @property
    def task_result_type(self) -> TaskResultType:
        return TaskResultType.DICT

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.INITIALIZER
