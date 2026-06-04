"""
This module contains the class for the :class:`~TableCleanser`.
"""

from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

import yaml
from loguru import logger
from typing_extensions import override

from xflow.task import TaskResultType
from xflow.task import TaskType
from xnl.table_builder import sql_utils
from xnl.table_builder.base import SimpleReturnBuilder
from xnl.table_builder.mixin import SingleExtFileLoaderMixin


class TableCleanser(SimpleReturnBuilder, SingleExtFileLoaderMixin):
    """
    One of the builder classes which represents table cleansing. This is closely similar and can be completely replaced
    by the :class:`~TableTransformer` but is geared towards only cleansing and such through basic sql transformations
    for each column specified in a separate yaml file.
    """

    LOADER_PATH: Optional[Path] = None

    def __init__(self):
        super().__init__()
        self._init_loader("yaml")

    @staticmethod
    def _create_sql_field(field: Dict[str, Any], table_alias: str) -> str:
        field_name = field["name"]
        field_alias = field.get("alias")
        field_cast = field.get("cast")
        field_transform = field.get("transform")
        field_transform_args = field.get("transform_args", {})

        table_alias_accessor = table_alias + "."
        field_name = (
            table_alias_accessor if table_alias is not None and table_alias_accessor not in field_name else ""
        ) + field_name

        sql_field = field_name
        if field_transform is not None:
            sql_field = getattr(sql_utils, field_transform)(field_name, **field_transform_args)
        elif field_cast is not None:
            sql_field = f"CAST({field_name} AS {field_cast})"

        if field_alias is not None:
            sql_field = " ".join([sql_field, f"AS {field_alias}"])

        return sql_field

    @override
    @property
    def main_fn(self) -> Callable[..., str]:
        def _clean_table(actual_date: str):
            config = yaml.safe_load(self.loaded_files)
            table_name = config.get("table_name")
            table_alias = config.get("alias")
            sql_select_fields = []

            for field in config["fields"]:
                if field.get("use_actual_date_as_business_date"):
                    sql_select_fields.append(f"CAST('{actual_date}' AS DATE) AS business_date")
                    continue

                sql_select_fields.append(self._create_sql_field(field, table_alias))
            sql_select_fields = "SELECT " + ", ".join(sql_select_fields)

            from_statement = f"FROM {table_name}"
            if table_alias is not None:
                from_statement += f" {table_alias}"

            sql_script = " ".join([sql_select_fields, from_statement])
            logger.info(f"Table cleanser sql: {sql_script}")
            return sql_script

        return _clean_table

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.TRANSFORMER

    @override
    @property
    def task_result_type(self) -> TaskResultType:
        return TaskResultType.SQL_TO_TEMPVIEW
