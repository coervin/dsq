"""
This module contains the class for the :class:`~TableTransformer` and all transformer-type tasks.
"""

import inspect
from pathlib import Path
from types import ModuleType
from typing import Callable
from typing import List
from typing import Optional
from typing import Union

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame
from typing_extensions import Self
from typing_extensions import override

from xcomms.logger import pformat
from xflow.context import JobContext
from xflow.task import TaskResultFunction
from xflow.task import TaskResultIterator
from xflow.task import TaskResultType
from xflow.task import TaskType
from xflow.task import TupleResult
from xflow.task import transformer
from xnl.metadata.metadata import TableMetadata
from xnl.table_builder import sql_utils
from xnl.table_builder.base import PipelineReturnBuilder
from xnl.table_builder.mixin import SingleExtFileLoaderMixin


class TableTransformer(PipelineReturnBuilder, SingleExtFileLoaderMixin):
    """
    One of the builder classes which represents dataframe transformation through SQL queries.
    """

    LOADER_PATH: Optional[Path] = None

    def __init__(self):
        super().__init__()
        self._init_loader("sql")
        self._modules: List[ModuleType] = [sql_utils]

    def add_modules(self, module: Union[List, ModuleType]) -> Self:
        """
        Adds the module(s) that the loaded sql file uses to refer to python objects specified in the query.

        :param module: The module to add.
        :return:
        """
        if module == sql_utils:
            return self
        return self.copy_on_write(self._modules, module)

    @override
    @property
    def main_fn(self) -> Callable[..., str]:
        def read_sql():
            logger.info(f"Loading modules to locals(): {pformat(self._modules)}")
            for module in self._modules:
                locals().update(dict(inspect.getmembers(module, inspect.isfunction)))
            logger.debug(f"locals(): {pformat(locals())}")

            #  TODO: Hash the sql files and encrypt those hashes with a seed.
            #   Then match those decrypted hashes with the hashes from the deployed code.
            #   Though the responsible of the generation will be reliant on the developer of the product
            #   Therefore a utility function is needed:
            #   Fernet & MetroHash
            locals().update(self.all_kwargs)
            # ast.literal_eval which is the more preferred way than eval cannot currently
            # parse properly the files with embedded f-strings inside represented through brackets i.e. {}

            sql = eval(f'f"""{self.loaded_files}"""')  # nosec pylint: disable=eval-used

            return sql

        return read_sql

    @override
    def _generate_task_fn(self) -> TaskResultFunction:
        def task_fn(context: JobContext) -> TaskResultIterator:
            main_output, _ = self._main_fn_with_pre_processing(context)
            has_post_processing = len(self._post_processing_fns) > 0

            if has_post_processing:
                required_result_types = {
                    TaskResultType.DICT,
                    TaskResultType.TUPLE,
                    TaskResultType.DATAFRAME_TO_TEMPVIEW,
                    TaskResultType.DATAFRAME,
                }
            else:
                required_result_types = {TaskResultType.SQL_TO_TEMPVIEW, TaskResultType.SQL_TO_DATAFRAME}

            if self.task_result_type not in required_result_types:
                raise ValueError(
                    f"TaskResultType should be one of {required_result_types}, given {self.task_result_type}"
                )

            # Separated from the initial check above to prevent executing the SQL script while there's a possibility
            # of a ValueError
            if has_post_processing:
                logger.info(f"Executing sql: \n{main_output}")
                main_output = context.spark.sql(main_output)

            yield self._result_after_post_processing((main_output, _))

        return task_fn

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.TRANSFORMER

    #  TODO: Add required variables for sql format string
    #  TODO: Parse the sql script and check or throw errors if there are any missing variables in the f-string


@transformer
def parse_metadata_properties(interim_df: DataFrame, table_metadata: TableMetadata, actual_date: str):
    """
    Transformer task to parse and execute transformations with specific keywords when specified in the `meta` field
    of the table schema/metadata.

    :param interim_df: The dataframe to transform.
    :param table_metadata: The schema where to extract the meta field.
    :param actual_date: The actual date.
    :return:
    """
    columns_to_rename = table_metadata.meta.get("rename_columns_if_exists", {})
    for from_col, to_col in columns_to_rename.items():
        logger.info(f"Renaming column {from_col} to {to_col}")
        interim_df = interim_df.withColumnRenamed(from_col, to_col)

    fill_inexisting_columns_with_date = table_metadata.meta.get("fill_inexisting_columns_with_date")
    fill_value = f"date'{actual_date}'"

    if fill_inexisting_columns_with_date:
        inexisting_columns = {col.name.lower() for col in table_metadata.columns} - set(
            map(str.lower, interim_df.columns)
        )

        logger.info(f"Filling the following inexisting columns with {fill_value} value: {inexisting_columns}")
        for col in inexisting_columns:
            interim_df = interim_df.withColumn(col, F.expr(fill_value))

    yield TupleResult("interim_df", interim_df)
