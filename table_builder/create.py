"""
This module contains the class for the :class:`~TableInitializer` and all extractor-type tasks.
"""
import re
from types import SimpleNamespace
from typing import Any
from typing import Callable
from typing import List
from typing import Union

from loguru import logger
from pyspark.sql import SparkSession
from typing_extensions import Self
from typing_extensions import override

from xflow.context import JobContext
from xflow.task import TaskType
from xflow.task import TaskVoidFunction
from xnl.metadata.base import Metadata
from xnl.metadata.metadata import TableMetadata
from xnl.table_builder.base import SimpleVoidBuilder
from xnl.table_builder.iceberg import IcebergClient
from xnl.table_builder.mixin import DatabaseDatamartMixin
from xnl.table_builder.mixin import Deferred
from xnl.table_builder.mixin import Scd2Columns
from xnl.table_builder.mixin import _is_attr_empty


class TableInitializer(SimpleVoidBuilder):
    """
    One of the builder classes which represents the creation of the table if it is not existing.
    """

    @staticmethod
    def _generate_create_sql_query(
        table_metadata: Metadata,
        full_table_name,
        target_table_location: str,
        data_format: str,
        partitioned_by: str,
    ) -> str:
        table_comment = table_metadata.table_comment
        comment_clause = f"COMMENT '{table_comment}'" if table_comment else ""
        columns = ",\n".join([" ".join([key, value]) for key, value in table_metadata.schema().items()])

        sql_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {columns}
            )
            {data_format}
            {target_table_location}
            {partitioned_by}
            {comment_clause}
        """
        return sql_query

    def _create_parquet_table(
        self,
        spark: SparkSession,
        table_metadata: Metadata,
        full_table_name: str,
        target_table_location: str,
    ):
        partitions = table_metadata.partition_columns()

        if target_table_location is None:
            raise ValueError(f"No target_table_location has been set for {full_table_name} table")

        data_format = "USING parquet"
        partitioned_by = ""
        if partitions:
            partitioned_by = f"PARTITIONED BY ({','.join(partitions)})"
        location = f"LOCATION '{target_table_location}'"

        sql_query = self._generate_create_sql_query(
            table_metadata, full_table_name, location, data_format, partitioned_by
        )
        logger.info(sql_query)
        spark.sql(sql_query)

    def _create_iceberg_table(
        self,
        spark: SparkSession,
        table_metadata: Metadata,
        full_table_name: str,
        target_table_location: str,
    ):
        partitioned_by = ""
        partitions = table_metadata.partition_columns()

        if len(partitions) > 0:
            scd2_partitions = []
            if table_metadata.has_scd2_fields:
                scd2_partitions = [
                    Scd2Columns.CURRENT_ROW_FLAG.name,
                    f"month({Scd2Columns.ROW_EFF_START_DATE.name})",
                ]
                partitions.remove(Scd2Columns.CURRENT_ROW_FLAG.name)
            partitioned_by = f"PARTITIONED BY ({','.join(scd2_partitions + partitions)})"

        location = ""
        if target_table_location is not None:
            location = f"LOCATION '{str(re.escape(target_table_location))}'"

        if table_metadata.write_ordered_by is None:
            raise ValueError(
                f"write_ordered_by has not been set in the metadata for {full_table_name} table. "
                "Provide a list of columns on how the table should be ordered by on write."
            )

        data_format = "USING iceberg"
        sql_query = self._generate_create_sql_query(
            table_metadata, full_table_name, location, data_format, partitioned_by
        )

        logger.info(sql_query)
        spark.sql(sql_query)

        sql_query = (
            f"ALTER TABLE {full_table_name} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY "
            f"{','.join(table_metadata.write_ordered_by)}"
        )
        logger.info(sql_query)
        spark.sql(sql_query)

    @override
    @property
    def main_fn(self) -> Callable[..., Any]:
        """
        Creates a new table given the table schema if it is not yet existing.

        :return:
        """

        def _create_table(
            spark: SparkSession,
            table_metadata: Metadata,
            target_db_name: str,
            iceberg_catalog_name: str = IcebergClient.DEFAULT_CATALOG,
            target_table_location: str = None,
            enable_iceberg: bool = False,
        ):
            full_table_name = f"`{target_db_name}`.`{table_metadata.name}`"
            table_check = full_table_name
            if enable_iceberg:
                table_check = f"{iceberg_catalog_name}." + table_check

            if spark.catalog.tableExists(table_check):
                logger.info(f"{table_check} table already exists")
                return

            logger.info(
                f"Creating {full_table_name} table located at "
                f"{target_table_location} with schema: {table_metadata.struct_type}"
            )

            create_table_fn = self._create_parquet_table
            if enable_iceberg:
                create_table_fn = self._create_iceberg_table

            create_table_fn(spark, table_metadata, full_table_name, target_table_location)

        return _create_table

    def sql_type(self):
        """
        Provides more options tailored towards datamarts for creating multiple tables which are mostly utilized in
        CDP collection data product.

        :return:
        """
        return self._init_subclass(ComplexTableInitializer)

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.INITIALIZER


class ComplexTableInitializer(TableInitializer, DatabaseDatamartMixin):
    """
    Do not use this class directly, access it by calling `Table.initializer().sql_type()`.
    """

    def __init__(self):
        super().__init__()
        self.overwrite: Deferred[bool] = SimpleNamespace(value=False)
        self.table_partition: Deferred[str] = SimpleNamespace()
        self._datamart_filenames: List[str] = []
        self._table_metadata: List[Union[TableMetadata, str]] = []

    def overwrite_table(self, overwrite: bool) -> Self:
        """
        Sets whether to overwrite the table(s) or not.

        :param overwrite: Boolean to determining whether existing table is for overwrite.
        :return:
        """
        return self.copy_on_write(self.overwrite, overwrite)

    def add_datamart_filename(self, datamart_filename: Union[List, str]) -> Self:
        """
        Adds a new datamart to process.

        :param datamart_filename: filename/s to be added.
        :return:
        """
        return self.copy_on_write(self._datamart_filenames, datamart_filename)

    def add_table_metadata(self, table_metadata: Union[List, TableMetadata, str]) -> Self:
        """
        Adds a new table metadata to process.

        :param table_metadata: The new TableMetadata or names to be added.
        :return:
        """
        return self.copy_on_write(self._table_metadata, table_metadata)

    def partitioned_by(self, table_partition: str) -> Self:
        """
        Sets the columns to use as a partition.

        :param table_partition: Partition key/s.
        :return:
        """
        return self.copy_on_write(self.table_partition, table_partition)

    @override
    def _generate_task_fn(self) -> TaskVoidFunction:
        def task_fn(context: JobContext):
            kwargs = self._generate_all_kwargs(context)

            if len(self._table_metadata) > 0 and all(isinstance(elem, str) for elem in self._table_metadata):
                retrieved_metadata = []
                for metadata_name in self._table_metadata:
                    retrieved_metadata.extend(kwargs[metadata_name])

                self._table_metadata = retrieved_metadata

            tables = zip(
                self._datamart_filenames if len(self._datamart_filenames) > 0 else [None] * len(self._tables),
                self._tables,
                self._table_metadata,
            )

            for datamart_filename, target_table, table_metadata in tables:
                self.all_kwargs.update(
                    {
                        "datamart_filename": datamart_filename,
                        "target_table": target_table,
                        "table_metadata": table_metadata,
                    }
                )
                self._execute_subfunction(self.main_fn)

        return task_fn

    @override
    @property
    def main_fn(self) -> Callable[..., Any]:
        def _create_table(
            spark: SparkSession,
            target_table: str,
            db_name: str,
            dm_path: str,
            table_metadata: TableMetadata,
            table_partition: str = None,
            datamart_filename: str = None,
            overwrite: bool = True,
        ):
            is_datamart = datamart_filename is not None

            if is_datamart and target_table is None:
                target_table = datamart_filename[:-4].lower()
            full_table_name = f"`{db_name}`.`{target_table}`"

            if overwrite:
                drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
                logger.info(f"Drop Table SQL Query - {full_table_name}: {drop_sql}")
                spark.sql(drop_sql)

            data_format = "USING PARQUET"
            if not _is_attr_empty(table_partition):
                partitioned_by = f"PARTITIONED BY ({table_partition})"
                data_format = " ".join([data_format, partitioned_by])

            if is_datamart:
                data_format = """
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                    WITH SERDEPROPERTIES (
                      'serialization.format' = '|',
                      'field.delim' = '|',
                      'collection.delim' = 'STX',
                      'mapkey.delim' = 'ETX'
                    )
                    """

            columns = ",\n".join([" ".join([key, value]) for key, value in table_metadata.schema().items()])
            table_comment = table_metadata.table_comment
            comment_clause = f"COMMENT '{table_comment}'" if table_comment else ""
            table_location = (datamart_filename + "/") if is_datamart else target_table
            sql = f"""
                    CREATE TABLE IF NOT EXISTS {full_table_name} (
                        {columns}
                    )
                    {data_format}
                    LOCATION '{dm_path}/{table_location}'
                    {comment_clause}
                """
            logger.info(f"Creating {full_table_name} table with sql: {sql}")
            spark.sql(sql)

        return _create_table
