"""
This module contains the class for the :class:`~TableLoader` and all loader-type tasks.
"""
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from urllib.parse import urlparse

import boto3
import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing_extensions import Self
from typing_extensions import TypeAlias
from typing_extensions import override

from xcomms.constants import DATE_FORMAT
from xflow.context import JobConfig
from xflow.context import JobContext
from xflow.task import TaskDataframeFunction
from xflow.task import TaskFunction
from xflow.task import TaskResultType
from xflow.task import TaskType
from xnl.commons.incremental_load import _calculate_data_to_insert
from xnl.commons.incremental_load import _get_max_id
from xnl.data_quality.cloudwatch import CdpCloudwatchDimension
from xnl.data_quality.cloudwatch import CloudwatchMetric
from xnl.data_quality.cloudwatch import CloudwatchMetricsReporter
from xnl.data_quality.spark_atlas import SparkAtlasClient
from xnl.metadata.base import Metadata
from xnl.metadata.metadata import TableMetadata
from xnl.table_builder.base import PipelineReturnBuilder
from xnl.table_builder.iceberg import IcebergClient
from xnl.table_builder.iceberg import IcebergWriteClient
from xnl.table_builder.mixin import DatabaseDatamartMixin
from xnl.table_builder.mixin import Deferred
from xnl.table_builder.mixin import RequiredAttr
from xnl.table_builder.mixin import _is_attr_empty
from xnl.table_builder.sql_utils import clean_all_str_columns
from xnl.utils import get_table_info

TextLoaderType: TypeAlias = "TextLoader"
HdfsLoaderType: TypeAlias = "HdfsLoader"
RawParquetS3LoaderType: TypeAlias = "RawParquetS3Loader"
RawAvroPathLoaderType: TypeAlias = "RawAvroPathLoader"


def delete_s3_prefix(s3_path: str) -> None:
    parsed = urlparse(s3_path)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


class TableLoader(PipelineReturnBuilder):
    """
    One of the builder classes which represents loading of data into an existing table.
    """

    def __init__(self):
        super().__init__()
        self.result_name = "interim_df"

    @staticmethod
    def _write_parquet(
        spark: SparkSession, full_table_name: str, write_df: DataFrame, write_mode: str, enable_intelligent_tier: bool
    ):
        logger.info(f"Writing data to {full_table_name} table with {write_mode} write mode")

        if enable_intelligent_tier:
            bucket, prefix, table_partition = get_table_info(spark, full_table_name)
            write_df.write.parquet(
                "/".join(["s3a:/", bucket, prefix]),
                mode=write_mode,
                partitionBy=table_partition,
                compression="snappy",
            )
        else:
            write_df.write.mode(write_mode).insertInto(full_table_name)

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        """
        Inserts the dataframe into the specified table and logs several Cloudwatch metrics upon its execution.

        :return:
        """

        def _write_to_table(
            spark: SparkSession,
            config: JobConfig,
            target_db_name: str,
            interim_df: DataFrame,
            table_metadata: Metadata,
            enable_intelligent_tier: bool = False,
            external_parameters: Dict[str, Any] = None,
            repartition_count: int = None,
            write_mode: str = "overwrite",
            write_fn: Callable = None,
        ):
            external_parameters = external_parameters or {}
            if repartition_count and repartition_count > 0:
                interim_df = interim_df.repartition(repartition_count)
            write_df = interim_df.select(
                *[col for col in table_metadata.schema(names_only=True) if col in interim_df.columns]
            )

            full_table_name = f"`{target_db_name}`.`{table_metadata.name}`"
            if write_fn is None:

                def write_fn():
                    self._write_parquet(spark, full_table_name, write_df, write_mode, enable_intelligent_tier)

            if external_parameters.get("metrics_enabled", False) in (True, "true"):
                cdp_dim = CdpCloudwatchDimension(
                    target_db_name,
                    table_metadata.name,
                    spark.conf.get("spark.app.name", CloudwatchMetric.UNDEFINED_DIM),
                    external_parameters.get("dag_id", CloudwatchMetric.UNDEFINED_DIM),
                    external_parameters.get("cloudwatch_namespace", "CDP/SparkJobMetrics"),
                )

                CloudwatchMetricsReporter(cdp_dim).publish_metrics(write_fn)
            else:
                write_fn()

            if external_parameters.get("lineage_tracking_enabled", False) in (True, "true"):
                SparkAtlasClient(config, external_parameters.get("env")).publish_lineage(
                    write_df,
                    target_db_name,
                    table_metadata.name,
                    external_parameters.get("lineage_custom_attributes", None),
                )

            return interim_df

        return _write_to_table

    def scd2(self):
        """
        Determine which of the data are modified/new then insert those into the specified table, incorporating SCD2.

        :return:
        """

        def _get_data_to_insert(
            spark: SparkSession,
            interim_df: DataFrame,
            table_metadata: TableMetadata,
            actual_date: str,
            target_db_name: str,
            replace_na_to_empty_string=True,
            enable_cache=False,
        ):
            new_data_table_name = "new_data_table_name"
            target_table_name = "target_table_name"

            if enable_cache:
                logger.info("Caching dataframe...")
                interim_df.cache()
                logger.info("Dataframe successfully cached.")

            if replace_na_to_empty_string:
                interim_df = clean_all_str_columns(interim_df)

            interim_df.createOrReplaceTempView(new_data_table_name)

            spark.table(f"`{target_db_name}`.`{table_metadata.name}`").createOrReplaceTempView(target_table_name)

            dim_cols = table_metadata.dim_columns()
            id_name = dim_cols[0] if len(dim_cols) > 0 else None
            max_id = 0
            if id_name is not None:
                max_id = _get_max_id(spark, id_name, target_table_name)

            interim_df = _calculate_data_to_insert(
                spark=spark,
                max_id=max_id,
                new_data_table_name=new_data_table_name,
                id_name=id_name,
                pkeys=table_metadata.pkeys(names_only=False),
                fields=table_metadata.data_columns(names_only=False),
                target_table_name=target_table_name,
                actual_date=datetime.strptime(actual_date, DATE_FORMAT),
                actual_date_col_name=",".join(table_metadata.partition_columns()),
            )

            return {"interim_df": interim_df}

        return self.add_pre_processing(_get_data_to_insert)

    def sql_type(self):
        """
        Provides more options tailored towards datamarts for inserting data into table via hdfs, text, and parquet,
        which are mostly utilized in CDP collection data product.

        :return:
        """
        return self._init_subclass(ComplexTableLoader)

    def iceberg(self):
        """
        Provide functionality for loading into Iceberg Table, mainly for SCD tables
        """
        return self._init_subclass(IcebergTableLoader)

    def raw_avro_path(self) -> RawAvroPathLoaderType:
        """
        Load a dataframe as an Avro file to an arbitrary path (e.g., S3) without DB involvement.
        """
        return self._init_subclass(RawAvroPathLoader)

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.LOADER

    @override
    @property
    def task_result_type(self) -> TaskResultType:
        return TaskResultType.DATAFRAME


class ComplexTableLoader(TableLoader, DatabaseDatamartMixin):
    """
    Do not use this class directly, access it by calling `Table.loader().sql_type()`.
    """

    def __init__(self):
        super().__init__()
        self.spark_config: Dict[str, Any] = {}
        self._write_mode: Deferred[str] = SimpleNamespace()
        self._view_names: List[str] = []

    def with_write_mode(self, write_mode: str) -> Self:
        """
        Sets the write mode for loading the table.

        :param write_mode: The write mode.
        :return:
        """
        return self.copy_on_write(self._write_mode, write_mode)

    def add_view_names(self, view_names: List[str]) -> Self:
        """
        Add existing table views which is also loaded to tables.

        :param view_names: The table views to include.
        :return:
        """
        return self.copy_on_write(self._view_names, view_names)

    @override
    def _generate_task_fn(self) -> TaskFunction:
        def task_fn(context: JobContext):
            self._generate_all_kwargs(context)
            is_first_split = True
            write_mode = "overwrite" if _is_attr_empty(self._write_mode) else self._write_mode

            multipart_table = None
            if len(self._tables) == 1 and len(self._view_names) > 0:
                multipart_table = self._tables[0]
                self._tables = self._view_names

            for table in self._tables:
                result_df = context.spark.table(table)
                if result_df.count() == 0:
                    logger.info(f"No data to process. Job {table} finished.")
                    continue

                if multipart_table is not None and write_mode == "overwrite" and not is_first_split:
                    write_mode = "append"

                self.all_kwargs.update(
                    {"result_df": result_df, "target_table": multipart_table or table, "write_mode": write_mode}
                )
                yield self.task_result_type(self.result_name, self._execute_subfunction(self.main_fn))
                is_first_split = False

        return task_fn

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        def _load_as_dataframe(
            spark: SparkSession,
            result_df: DataFrame,
            db_name: str,
            target_table: str,
            write_mode: str,
            repartition: int = None,
            dm_path: str = None,
            partition_cols: List[str] = None,
            checkpoint: bool = False,
            save_as: str = None,
        ):
            dm_path = dm_path or ""
            partition_cols = partition_cols or []
            output_table = f"`{db_name}`.`{target_table}`"
            output_table_path = f"{dm_path}/{target_table}"

            if len(partition_cols) > 0:
                result_df = ComplexTableLoader._move_partition_cols_to_end(result_df, partition_cols)

            if checkpoint:
                spark.sparkContext.setCheckpointDir(f"{dm_path}/checkpoints")
                result_df = result_df.checkpoint()

            if repartition is not None:
                result_df = result_df.repartition(repartition)

            logger.info(f"Writing data to {output_table} table with {write_mode} write mode")
            df_writer = result_df.write.mode(write_mode)

            if save_as is not None:
                if len(partition_cols) > 0:
                    df_writer = df_writer.partitionBy(*partition_cols)

                if save_as == "table":
                    df_writer.option("path", output_table_path).saveAsTable(output_table)
                else:
                    raise ValueError(f"Unsupported save_as value: {save_as}")

            else:
                df_writer.format("parquet").insertInto(output_table)

            return result_df

        return _load_as_dataframe

    @staticmethod
    def _move_partition_cols_to_end(dataframe: DataFrame, partition_cols: List[str]) -> DataFrame:
        cols = dataframe.columns
        for partition_key in partition_cols:
            cols.append(cols.pop(cols.index(partition_key)))

        if cols != dataframe.columns:
            dataframe = dataframe.select(*cols)

        return dataframe

    def text(self) -> TextLoaderType:
        """
        Load the table as a text file.

        :return:
        """
        return self._init_subclass(TextLoader)

    def hdfs(self) -> HdfsLoaderType:
        """
        Load the table to HDFS.

        :return:
        """
        return self._init_subclass(HdfsLoader)

    def raw_parquet_s3(self) -> RawParquetS3LoaderType:
        """
        Load the table simply to S3.

        :return:
        """
        return self._init_subclass(RawParquetS3Loader)


class TextLoader(ComplexTableLoader):
    """
    Do not use this class directly, access it by calling `Table.loader().sql_type().text()`.
    """

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        def load_as_text(result_df: DataFrame, db_name: str, target_table: str):
            output_table = f"{db_name}/{target_table}/"
            logger.info(f"Writing text data to {output_table} table")

            result_df = result_df.fillna("").select(F.concat_ws("|", *result_df.columns))
            result_df = result_df.withColumn("sep", F.lit("\r"))
            result_df = result_df.select(F.concat_ws("", *result_df.columns))
            (
                result_df.coalesce(1)
                .write.option("header", False)
                .option("delimiter", "")
                .option("emptyValue", None)
                .option("nullValue", None)
                .mode("overwrite")
                .text(output_table)
            )

            return result_df

        return load_as_text


class HdfsLoader(ComplexTableLoader):
    """
    Do not use this class directly, access it by calling `Table.loader().sql_type().hdfs()`.
    """

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        def load_as_hdfs(
            result_df: DataFrame,
            target_table: str,
            write_mode: str = "overwrite",
            partition_cols: List[str] = None,
        ):
            target_table = f"hdfs:///{target_table}"
            logger.info(f"Writing data to {target_table} table")
            partition_cols = partition_cols or []
            result_df.write.partitionBy(*partition_cols).mode(write_mode).parquet(target_table)

            return result_df

        return load_as_hdfs


class RawParquetS3Loader(ComplexTableLoader):
    """
    Do not use this class directly, access it by calling `Table.loader().sql_type().raw_parquet_s3()`.
    """

    def __init__(self):
        super().__init__()
        self.s3_location: Deferred[str] = SimpleNamespace()

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        def load_as_parquet_s3(
            result_df: DataFrame,
            target_table: str,
            write_mode: str = "overwrite",
        ):
            target_table = f"s3a://{self.s3_location}/{target_table}"
            logger.info(f"Writing data to {target_table} table")
            result_df.write.mode(write_mode).parquet(target_table)

            return result_df

        return load_as_parquet_s3

    def with_location(self, location: str) -> Self:
        """
        Sets the S3 bucket location where the table is located.

        :param location: The S3 bucket.
        :return:
        """
        return self.copy_on_write(self.s3_location, location)

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.s3_location, self.with_location)]


class RawAvroPathLoader(TableLoader):
    """
    Do not use this class directly, access it by calling `Table.loader().raw_avro_path()`.
    Load the dataframe as avro to the target location. Avro is not Spark built-in so ensure
    spark-avro_{hadoop-version}-{spark-version}.jar is included in the Spark classpath.
    """

    @override
    @property
    def main_fn(self) -> TaskDataframeFunction:
        def load_avro(
            interim_df: DataFrame,
            target_raw_location: str,
            clean_target_location: bool = True,
            coalesce_count: int = 0,
            compression: str = "snappy",
            write_mode: str = "overwrite",
        ):
            df_to_write = interim_df.cache()
            is_empty = df_to_write.isEmpty()
            if not is_empty:
                if clean_target_location and write_mode == "overwrite":
                    logger.info(f"Cleaning files in {target_raw_location}.")
                    delete_s3_prefix(target_raw_location)

                if coalesce_count and coalesce_count > 0:
                    df_to_write = df_to_write.coalesce(coalesce_count)

                logger.info(f"Loading data to {target_raw_location}.")
                (
                    df_to_write.write.mode(write_mode)
                    .format("avro")
                    .option("compression", compression)
                    .save(target_raw_location)
                )
            else:
                logger.info("Empty result dataframe for loading. Skipping loading.")
            return df_to_write

        return load_avro


class IcebergTableLoader(TableLoader):
    """
    Do not use this class directly, access it by calling `Table.loader().iceberg()`.
    """

    def __init__(self):
        super().__init__()
        self.add_pre_processing(self._init_client)
        self.cast_hash_columns_to_string: Deferred[bool] = SimpleNamespace()

    def with_string_cast_to_hash_cols(self, cast_hash_columns_to_string: bool) -> Self:
        """
        Toggle string-casting of hash columns for Iceberg row hash calculation.

        :param cast_hash_columns_to_string: Whether to cast hash columns to string before hashing.
        :return:
        """
        return self.copy_on_write(self.cast_hash_columns_to_string, cast_hash_columns_to_string)

    @staticmethod
    def _init_client(
        spark: SparkSession,
        table_metadata: Metadata,
        actual_date: str,
        target_db_name: str,
        cast_hash_columns_to_string: bool = False,
        catalog_name: str = IcebergClient.DEFAULT_CATALOG,
    ):
        client = IcebergWriteClient(
            table_metadata,
            spark,
            actual_date,
            target_db_name,
            catalog_name,
            cast_hash_columns_to_string=cast_hash_columns_to_string,
        )

        return {"iceberg_client": client}

    @override
    def scd2(self):
        """
        Load delta table into target using Iceberg SCD2
        """

        def _insert_scd2(
            iceberg_client: IcebergWriteClient,
            interim_df: DataFrame,
            job_name: str,
            pipeline_name: str,
            enable_icbg_scd2_dedup: bool = False,
        ):
            return {
                "write_fn": lambda: iceberg_client.load_scd2(
                    interim_df, f"{pipeline_name}-{job_name}", enable_icbg_scd2_dedup=enable_icbg_scd2_dedup
                )
            }

        return self.add_pre_processing(_insert_scd2)

    def scd1(self):
        """
        Load delta table into target using Iceberg SCD1
        """

        def _insert_scd1(
            iceberg_client: IcebergWriteClient,
            interim_df: DataFrame,
            job_name: str,
            pipeline_name: str,
        ):
            return {"write_fn": lambda: iceberg_client.load_scd1(interim_df, f"{pipeline_name}-{job_name}")}

        return self.add_pre_processing(_insert_scd1)

    def overwrite(self):
        def _insert_overwrite(
            iceberg_client: IcebergWriteClient,
            interim_df: DataFrame,
        ):
            return {"write_fn": lambda: iceberg_client.load_overwrite(interim_df)}

        return self.add_pre_processing(_insert_overwrite)
