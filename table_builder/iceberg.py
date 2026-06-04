"""
Module containing Iceberg class functionality
"""
from datetime import date
from datetime import datetime
from time import sleep
from typing import List
from typing import Tuple
from typing import Union

import boto3
import pyspark.sql.functions as F
from botocore.exceptions import ClientError
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from xcomms.constants import DATE_FORMAT
from xcomms.logger import LogConfig
from xcomms.logger import LogLevel
from xcomms.logger import print_header
from xcomms.utils import exponential_backoff
from xnl.metadata.base import Metadata
from xnl.table_builder.mixin import ControlColumns
from xnl.table_builder.mixin import DeletedFlag
from xnl.table_builder.mixin import CurrentFlag
from xnl.table_builder.mixin import Scd2Columns
from xnl.utils import get_table_info


class IcebergClient:  # pragma: no cover pylint: disable=too-few-public-methods
    """
    A base client class interact with Apache Iceberg tables.
    The base class provides functionality for SCD2 Iceberg table operations,
    """

    # Tests are already existing for this class but cannot be executed on the current setup of Spark
    # Meant for future use
    DEFAULT_CATALOG = "spark_catalog"

    def __init__(
        self,
        business_keys: List[str],
        table_name: str,
        spark: SparkSession,
        actual_date: Union[str, date],
        database_name: str,
        catalog_name: str = DEFAULT_CATALOG,
    ):
        self.spark = spark
        # business_keys must be strictly sanitized and provided as list since it may cause errors with Pyspark
        #   xspy4j.Py4JException: Method toSeq([class java.util.HashMap]) does not exist
        #   e.g. OrderedDict can be treated intelligently by python as iterable, but Pyspark cannot thus being
        #   converted to a HashMap
        self.business_keys = business_keys if isinstance(business_keys, list) else list(business_keys)
        self.database_name = database_name
        self.actual_date = datetime.strptime(actual_date, DATE_FORMAT) if isinstance(actual_date, str) else actual_date
        self.table_name = table_name
        self.catalog_name = catalog_name
        self._full_table_name = f"{catalog_name}.{self.database_name}.{self.table_name}"
        self._target_alias = "target"
        self._source_alias = "source"

    def read_scd2(self) -> DataFrame:
        """
        Read the current data from SCD2 Iceberg table using column is_current = 1 filter

        :return: DataFrame
        """
        return self.spark.sql(
            f"""
            SELECT {self._target_alias}.*
            FROM {self._full_table_name} AS {self._target_alias}
            WHERE {self._target_alias}.{Scd2Columns.CURRENT_ROW_FLAG.name} = '{CurrentFlag.YES.value}'
            """
        )


class IcebergWriteClient(IcebergClient):
    """
    A client class for writing data to Apache Iceberg tables, extending the base IcebergClient.
    This class provides functionalities for handling SCD Type 1 and Type 2 operations,
    migrating tables to Iceberg format, and managing metadata.
    """

    def __init__(self, table_metadata: Metadata, *args, cast_hash_columns_to_string: bool = False, **kwargs):
        super().__init__(table_metadata.pkeys(names_only=False), table_metadata.name, *args, **kwargs)

        self.has_control_fields = table_metadata.has_control_fields
        self.scd_columns = []

        if table_metadata.has_scd2_fields:
            self.scd_columns.extend(list(Scd2Columns))
        if self.has_control_fields:
            self.scd_columns.extend(list(ControlColumns))

        self.scd_column_names = [col.name for col in self.scd_columns]
        _, _, self._table_partitions = get_table_info(self.spark, self._full_table_name)
        self._merge_on_id_cols = " AND ".join(
            [f"{self._target_alias}.{col}={self._source_alias}.{col}" for col in self.business_keys]
        )
        self._debug_columns = list(set(self.business_keys + self._table_partitions + self.scd_column_names))
        self.partition_by_id_cols = ",".join(self.business_keys)
        self.is_null_id_cols = " AND ".join([f"{self._target_alias}.{col} IS NULL" for col in self.business_keys])
        self.cast_hash_columns_to_string = cast_hash_columns_to_string

    def _log_dataframe(self, dataframe: DataFrame, header: str):
        if LogConfig.level.no > LogLevel.DEBUG.value.no:
            return

        logger.debug("Dataframe count:", dataframe.count())
        logger.debug(print_header(header))
        dataframe.select(*self._debug_columns).show(truncate=False)
        logger.debug(print_header(" END "))

    def _extract_hash_columns(self, dataframe: DataFrame) -> List[str]:
        return sorted(list(set(dataframe.columns) - set(self._table_partitions) - set(self.scd_column_names)))

    def _add_scd2_columns(self, dataframe: DataFrame) -> DataFrame:
        dataframe = (
            dataframe.withColumn(
                Scd2Columns.ROW_EFF_START_DATE.name,
                F.lit(self.actual_date).cast(Scd2Columns.ROW_EFF_START_DATE.value["type"]),
            )
            .withColumn(
                Scd2Columns.ROW_EFF_END_DATE.name,
                F.lit(date(9999, 12, 31)).cast(Scd2Columns.ROW_EFF_END_DATE.value["type"]),
            )
            .withColumn(Scd2Columns.CURRENT_ROW_FLAG.name, F.lit("Y").cast(Scd2Columns.CURRENT_ROW_FLAG.value["type"]))
        )

        return dataframe

    def _add_control_columns(self, dataframe: DataFrame, pipeline_job_name: str):
        current_timestamp = F.current_timestamp()

        dataframe = (
            dataframe.withColumn(
                ControlColumns.CTRL_CREATE_USER_ID.name,
                F.lit(pipeline_job_name).cast(ControlColumns.CTRL_CREATE_USER_ID.value["type"]),
            )
            .withColumn(
                ControlColumns.CTRL_CREATE_TS.name,
                F.lit(current_timestamp).cast(ControlColumns.CTRL_CREATE_TS.value["type"]),
            )
            .withColumn(
                ControlColumns.CTRL_LAST_UPDATE_USER_ID.name,
                F.lit(pipeline_job_name).cast(ControlColumns.CTRL_LAST_UPDATE_USER_ID.value["type"]),
            )
            .withColumn(
                ControlColumns.CTRL_LAST_UPDATE_TS.name,
                F.lit(current_timestamp).cast(ControlColumns.CTRL_LAST_UPDATE_TS.value["type"]),
            )
        )

        return dataframe

    def prepare_delta_df(self, dataframe: DataFrame, pipeline_job_name: str, scd_type: int = 1) -> Tuple[str, str]:
        """
        Prepares a DataFrame for SCD operations as delta.
        Computes hash columns, adds control fields and SCD type 2 columns if necessary, and creates a temporary view.
        Returns the view name and a comma-separated list of data columns.

        :param dataframe: input Dataframe to prepare for SCD operations
        :param pipeline_job_name: The name of the job that last updated the table.
        :return: View name and a comma-separated list of data columns.
        :rType: Tuple[str, str]
        """
        delta_df_view = "delta_df"

        hash_columns = self._extract_hash_columns(dataframe)
        logger.info(f"Hash Columns: {hash_columns}")

        # Cast column to string before hash to avoid the error below when replacing a NULL value with __NULL__
        # pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.DATA_DIFF_TYPES]
        if self.cast_hash_columns_to_string:
            hash_columns = [
                F.coalesce(F.col(column_name).cast("string"), F.lit("__NULL__")) for column_name in hash_columns
            ]
        else:
            hash_columns = [F.coalesce(F.col(column_name), F.lit("__NULL__")) for column_name in hash_columns]

        dataframe = dataframe.withColumn(ControlColumns.ROW_HASH_VALUE.name, F.md5(F.concat(*hash_columns)))
        table_partitions = self._table_partitions.copy()

        if self.has_control_fields:
            dataframe = self._add_control_columns(dataframe, pipeline_job_name)

        if scd_type == 2:
            dataframe = self._add_scd2_columns(dataframe)
            try:
                table_partitions.remove(Scd2Columns.CURRENT_ROW_FLAG.name)
            except ValueError:
                logger.warning(
                    f"{Scd2Columns.CURRENT_ROW_FLAG.name} is not set as "
                    f"partition for {self._full_table_name} table. Partitions: {table_partitions}"
                )

        dataframe.createOrReplaceTempView(delta_df_view)
        data_columns = ",".join(self._extract_hash_columns(dataframe) + table_partitions)

        return delta_df_view, data_columns

    def _deduplicate_current_rows(self):
        """
        Deduplicates existing rows where multiple records represent the same data version for a business key.
        This fixes two types of data corruption from the re-insertion bug:

        1. Current duplicates: Multiple rows with current_row_flag = 'Y' for the same business key
        2. Consecutive hash duplicates: Multiple rows with the same business key + row_hash_value that form
           a consecutive chain (where end_date = next start_date) WITHOUT any NULL end_dates in between.
           This pattern indicates normalized duplicates from the bug, not legitimate history.

        Legitimate history (insertâ†’deleteâ†’reinsert) is preserved because tombstone rows (NULL end_date)
        break the consecutive chain pattern.

        Keeps the row with the earliest row_eff_start_date and DELETES all other duplicate rows.

        :return:
        """
        current_flag_col = Scd2Columns.CURRENT_ROW_FLAG.name
        row_start_col = Scd2Columns.ROW_EFF_START_DATE.name
        row_end_col = Scd2Columns.ROW_EFF_END_DATE.name
        row_hash_col = ControlColumns.ROW_HASH_VALUE.name

        # Identify which rows should be DELETED
        # For current duplicates and consecutive hash chains, keep earliest row_eff_start_date, delete others
        dedup_query = f"""
            WITH current_dupes AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self.partition_by_id_cols}
                        ORDER BY
                            CASE WHEN {row_end_col} = date'9999-12-31' THEN 0 ELSE 1 END,
                            {row_start_col} ASC,
                            {ControlColumns.CTRL_CREATE_TS.name} ASC
                    ) as row_rank
                FROM {self._full_table_name}
                WHERE {current_flag_col} = '{CurrentFlag.YES.value}'
            ),
            current_dupe_keys AS (
                SELECT DISTINCT {self.partition_by_id_cols}
                FROM current_dupes
                GROUP BY {self.partition_by_id_cols}
                HAVING COUNT(*) > 1
            ),
            hash_rows AS (
                SELECT *,
                    LEAD({row_start_col}) OVER (
                        PARTITION BY {self.partition_by_id_cols}, {row_hash_col}
                        ORDER BY {row_start_col}
                    ) as next_start_date,
                    LEAD({row_end_col}) OVER (
                        PARTITION BY {self.partition_by_id_cols}, {row_hash_col}
                        ORDER BY {row_start_col}
                    ) as next_end_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self.partition_by_id_cols}, {row_hash_col}
                        ORDER BY
                            CASE WHEN {row_end_col} = date'9999-12-31' THEN 0 ELSE 1 END,
                            {row_start_col} ASC,
                            {ControlColumns.CTRL_CREATE_TS.name} ASC
                    ) as hash_rank
                FROM {self._full_table_name}
                WHERE {row_hash_col} IS NOT NULL
            ),
            consecutive_chain_members AS (
                SELECT DISTINCT h1.{self.partition_by_id_cols}, h1.{row_hash_col}
                FROM hash_rows h1
                WHERE EXISTS (
                    SELECT 1 FROM hash_rows h2
                    WHERE {" AND ".join([f"h1.{col}=h2.{col}" for col in self.business_keys])}
                      AND h1.{row_hash_col} = h2.{row_hash_col}
                      AND h1.{row_end_col} IS NOT NULL
                      AND h1.{row_end_col} = h2.{row_start_col}
                      AND h2.{row_end_col} IS NOT NULL
                )
            ),
            rows_to_delete AS (
                SELECT r.{row_hash_col}, r.{row_start_col}, r.{row_end_col}, r.{current_flag_col},
                       {", ".join([f"r.{col}" for col in self.business_keys])}
                FROM current_dupes r
                INNER JOIN current_dupe_keys d ON {" AND ".join([f"r.{col}=d.{col}" for col in self.business_keys])}
                WHERE r.row_rank > 1

                UNION

                SELECT h.{row_hash_col}, h.{row_start_col}, h.{row_end_col}, h.{current_flag_col},
                       {", ".join([f"h.{col}" for col in self.business_keys])}
                FROM hash_rows h
                INNER JOIN consecutive_chain_members c
                    ON {" AND ".join([f"h.{col}=c.{col}" for col in self.business_keys])}
                    AND h.{row_hash_col} = c.{row_hash_col}
                WHERE h.hash_rank > 1
            )
            SELECT DISTINCT * FROM rows_to_delete
        """

        rows_to_delete = self.spark.sql(dedup_query)

        # Create a temp view for the DELETE operation
        rows_to_delete.createOrReplaceTempView("dedup_deletes")

        # DELETE the duplicate rows completely from the table
        self.spark.sql(
            f"""
            DELETE FROM {self._full_table_name}
            WHERE EXISTS (
                SELECT 1 FROM dedup_deletes
                WHERE {" AND ".join(
                    [f"{self._full_table_name}.{col}=dedup_deletes.{col}" for col in self.business_keys]
                )}
                    AND {self._full_table_name}.{row_hash_col} = dedup_deletes.{row_hash_col}
                    AND {self._full_table_name}.{row_start_col} = dedup_deletes.{row_start_col}
            )
            """
        )

    def load_scd2(self, dataframe: DataFrame, pipeline_job_name: str, enable_icbg_scd2_dedup: bool = False):
        """
        Loads a DataFrame into a SCD2 Table. Handles updates and inserts by comparing checksums and business keys.
        Also deduplicates any existing rows where multiple records have current_row_flag = 'Y' for the same
        business key (fixes historical data corruption).

        :param dataframe: DataFrame to be loaded
        :param pipeline_job_name: The name of the job that last updated the table.
        :param enable_icbg_scd2_dedup: Set to True if to deduplicate current rows before loading.
        :return:
        """

        if enable_icbg_scd2_dedup:
            self._deduplicate_current_rows()

        delta_df_view, data_columns = self.prepare_delta_df(dataframe, pipeline_job_name, scd_type=2)

        row_hash_col = ControlColumns.ROW_HASH_VALUE.name
        row_start_col = Scd2Columns.ROW_EFF_START_DATE.name
        row_end_col = Scd2Columns.ROW_EFF_END_DATE.name

        current_flag_col = Scd2Columns.CURRENT_ROW_FLAG.name
        is_current_row_deleted = (
            f"{self._source_alias}.{row_hash_col} IS NULL AND {self._target_alias}.{row_end_col} IS NOT NULL"
        )
        current_and_deleted_df = self.spark.sql(
            f"""
            SELECT {self._target_alias}.*,
            CASE
                WHEN {is_current_row_deleted} THEN '{DeletedFlag.YES.value}'
                ELSE '{DeletedFlag.NO.value}'
            END AS {DeletedFlag.COLUMN_NAME.value}
            FROM {self._full_table_name} AS {self._target_alias}
            LEFT JOIN {delta_df_view} AS {self._source_alias} ON {self._merge_on_id_cols}
            WHERE {self._target_alias}.{current_flag_col} = '{CurrentFlag.YES.value}'
            AND (
                --  Target current row is deleted in source
                {is_current_row_deleted}
                OR
                -- Target current row is changed in source
                {self._target_alias}.{row_hash_col} != {self._source_alias}.{row_hash_col}
                OR
                -- Target current row that was deleted is being reinserted
                ({self._target_alias}.{row_hash_col} = {self._source_alias}.{row_hash_col}
                AND {self._target_alias}.{row_end_col} IS NULL)
            )
            """
        )

        current_df_view = "current_df_view"
        current_df = current_and_deleted_df.withColumn(DeletedFlag.COLUMN_NAME.value, F.lit(DeletedFlag.NO.value))
        current_df.createOrReplaceTempView(current_df_view)
        self._log_dataframe(current_df, current_df_view)

        deleted_df_view = "deleted_df_view"
        deleted_df = current_and_deleted_df.filter(
            f"{DeletedFlag.COLUMN_NAME.value} = '{DeletedFlag.YES.value}'"
        ).withColumn(
            row_start_col,
            F.lit(self.actual_date).cast(Scd2Columns.ROW_EFF_START_DATE.value["type"]),
        )
        deleted_df = self._add_control_columns(deleted_df, pipeline_job_name)

        deleted_df.createOrReplaceTempView(deleted_df_view)
        self._log_dataframe(deleted_df, deleted_df_view)

        insert_df_view = "insert_df_view"
        insert_df = self.spark.sql(
            f"""
            SELECT {self._source_alias}.*,
            '{DeletedFlag.NO.value}' AS {DeletedFlag.COLUMN_NAME.value}
            FROM {delta_df_view} AS {self._source_alias}
            LEFT JOIN {self._full_table_name} AS {self._target_alias} ON {self._merge_on_id_cols}
            WHERE
                (
                    {self._target_alias}.{row_hash_col} != {self._source_alias}.{row_hash_col}
                    AND {self._target_alias}.{current_flag_col} = '{CurrentFlag.YES.value}'
                )
                OR (
                    {self.is_null_id_cols} AND {self._target_alias}.{row_hash_col} IS NULL
                )
                OR (
                    {self._target_alias}.{current_flag_col} = '{CurrentFlag.YES.value}'
                    AND {self._target_alias}.{row_end_col} IS NULL
                    AND {self._target_alias}.{row_hash_col} = {self._source_alias}.{row_hash_col}
                )
            """
        ).select(*current_df.columns)
        insert_df.createOrReplaceTempView(insert_df_view)
        self._log_dataframe(insert_df, insert_df_view)

        if LogConfig.level.no <= LogLevel.DEBUG.value.no:
            source_fields = ",".join([f"{self._source_alias}.{field}" for field in self._debug_columns])
            target_fields = ",".join([f"{self._target_alias}.{field}" for field in self._debug_columns])
            insert_df_debug = self.spark.sql(
                f"""
                SELECT {source_fields}, {target_fields} FROM {delta_df_view} AS {self._source_alias}
                LEFT JOIN {self._full_table_name} AS {self._target_alias} ON {self._merge_on_id_cols}
                """
            )
            logger.debug(print_header(insert_df_view + f"_raw ({self._source_alias} & {self._target_alias} fields)"))
            logger.debug("Dataframe count:", insert_df_debug.count())
            insert_df_debug.show(truncate=False)
            logger.debug(print_header(" END "))

        control_fields_cte = ""
        control_fields_query = ""
        create_ts_col = ControlColumns.CTRL_CREATE_TS.name
        create_user_id_col = ControlColumns.CTRL_CREATE_USER_ID.name

        if self.has_control_fields:
            control_fields_cte = f""",
                LEAD({create_ts_col}) OVER row_start_window AS eff_update_ts,
                LEAD({create_user_id_col}) OVER row_start_window AS eff_update_user_id
            """

            control_fields_query = f""",
                {ControlColumns.CTRL_CREATE_TS.name},
                {ControlColumns.CTRL_CREATE_USER_ID.name},
                COALESCE(eff_update_ts, {create_ts_col}) AS {ControlColumns.CTRL_LAST_UPDATE_TS.name},
                COALESCE(eff_update_user_id, {create_user_id_col}) AS {ControlColumns.CTRL_LAST_UPDATE_USER_ID.name}
            """

        table_update_query = f"""
            WITH table_to_update AS (
                  SELECT * FROM {current_df_view}
                UNION
                  SELECT * FROM {insert_df_view}
                UNION
                  SELECT * FROM {deleted_df_view}
            ),
            table_updated AS (
                SELECT *,
                LEAD({row_start_col}) OVER row_start_window AS eff_from
                {control_fields_cte}
                FROM table_to_update
                WINDOW row_start_window AS (PARTITION BY {self.partition_by_id_cols} ORDER BY {row_start_col})
            )
            SELECT {data_columns},
                {row_hash_col},
                {row_start_col},
                CASE
                    WHEN {DeletedFlag.COLUMN_NAME.value} = '{DeletedFlag.YES.value}' THEN NULL
                    WHEN {row_end_col} IS NULL AND eff_from IS NOT NULL THEN NULL
                    ELSE COALESCE(eff_from, date'9999-12-31')
                END AS {Scd2Columns.ROW_EFF_END_DATE.name},
                CASE WHEN eff_from IS NULL
                    THEN '{CurrentFlag.YES.value}'
                    ELSE '{CurrentFlag.NO.value}' END AS {current_flag_col}
                {control_fields_query}
            FROM table_updated
        """

        df_table_update = self.spark.sql(table_update_query)
        df_table_update_view = "table_update"

        # There's a bug with using UDF with MERGE INTO statement,
        #   so there will be a need to evaluate and persist them
        #   separately if there is any UDF involved in the query.
        # https://github.com/apache/iceberg/issues/7656
        # https://stackoverflow.com/questions/76288396/apache-iceberg-bug-merge-into-pyspark-with-udf-causes-cannot-generate-code-for
        df_table_update.cache()
        df_table_update.show(truncate=False)
        self._log_dataframe(df_table_update, df_table_update_view)
        df_table_update = df_table_update.select(self.spark.table(f"{self.database_name}.{self.table_name}").columns)
        df_table_update.createOrReplaceTempView(df_table_update_view)

        # Just to take note, iceberg may potentially override null values even though they are not included to
        #   the set of updates to be merged into. For example in unit testing, the valid_to value of pkey=0
        # changes from null to 1970-01-01 08:00:00 after the query below.
        self.spark.sql(
            f"""
            MERGE INTO {self._full_table_name} {self._target_alias}
            USING (SELECT * FROM {df_table_update_view}) {self._source_alias}
            ON {self._merge_on_id_cols} AND {self._target_alias}.{row_hash_col} = {self._source_alias}.{row_hash_col}
            AND {self._target_alias}.{row_start_col} = {self._source_alias}.{row_start_col}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    def migrate_scd2(self, dataframe: DataFrame, old_scd2_view: str, pipeline_job_name: str, sort_order: str):
        """
        Migrates old SCD2 view to new SCD2 format. Handles the addition of SCD2 columns to both the target table
        and the old SCD2 view, and merges the latest rows into the target table.

        :param dataframe: DataFrame to be loaded to scd2 table
        :param old_scd2_view: old scd2 view name
        :param pipeline_job_name: The name of the job that last updated the table.
        :param sort_order: string of columns with ASC/DESC to assign sorting on all data
        :return:
        """
        logger.info(f"Migrating {self._full_table_name} to new SCD2 format")
        target_table_columns = self.spark.sql(f"SELECT * FROM {self._full_table_name} LIMIT 0").columns
        has_no_scd2_columns = len(set(target_table_columns).intersection(self.scd_column_names)) == 0

        if has_no_scd2_columns:
            logger.info(f"Adding the following columns to {self._full_table_name}: {self.scd_column_names}")
            self.spark.sql(
                f"""
                ALTER TABLE {self._full_table_name} ADD COLUMNS (
                    {",".join([f"{col.name} {col.value['type']}" for col in self.scd_columns])}
                )
                """
            )

        old_scd2_view_df = self.spark.table(old_scd2_view)
        if len(set(old_scd2_view_df.columns).intersection(self.scd_column_names)) == 0:
            for col in self.scd_columns:
                old_scd2_view_df = old_scd2_view_df.withColumn(col.name, F.lit(None).astype(col.value["type"]))
            logger.info(
                f"Adding the following columns to the passed old SCD2 view '{old_scd2_view}': {self.scd_column_names}"
            )
            old_scd2_view_df.printSchema()

        merge_on_partition_cols = " AND ".join(
            [f"{self._target_alias}.{col}={self._source_alias}.{col}" for col in self._table_partitions]
        )
        is_null_scd2_cols = " AND ".join([f"{col} IS NULL" for col in self.scd_column_names])
        old_scd2_view_df = old_scd2_view_df.filter(is_null_scd2_cols)
        latest_scd2_rows = old_scd2_view_df.count()

        if latest_scd2_rows > 0:
            logger.info(f"Marking latest rows with as 'current': {latest_scd2_rows} rows")
            existing_table_view, _ = self.prepare_delta_df(old_scd2_view_df, pipeline_job_name, scd_type=2)

            # https://github.com/apache/iceberg/issues/9827
            # https://github.com/apache/iceberg/issues/5556
            self.spark.sql(
                f"MERGE INTO {self._full_table_name} {self._target_alias} "
                f"USING (SELECT * FROM {existing_table_view}) {self._source_alias} "
                f"ON {self._merge_on_id_cols} AND {merge_on_partition_cols} "
                f"WHEN MATCHED THEN UPDATE SET * "
                f"WHEN NOT MATCHED THEN INSERT *"
            )

        self.spark.sql(f"ALTER TABLE {self._full_table_name} ADD PARTITION FIELD {Scd2Columns.CURRENT_ROW_FLAG.name}")
        self.spark.sql(
            f"ALTER TABLE {self._full_table_name} " f"ADD PARTITION FIELD month({Scd2Columns.ROW_EFF_START_DATE.name})"
        )

        self.spark.sql(
            f"CALL {self.catalog_name}.system.rewrite_data_files("
            f"table => '{self._full_table_name}', "
            f"strategy => 'sort', "
            f"sort_order => '{sort_order}')"
        )

        self.load_scd2(dataframe, pipeline_job_name)

    def migrate_to_iceberg(
        self,
        target_table_suffix: str = "iceberg",
        in_place_migration: bool = False,
        validate_output_table: bool = False,
    ):
        """
        Migrates an Athena table into an Iceberg table, while also dropping the original table via Athena.
        This function manages table creation, partitioning, and optional validation of the resulting table.
        It also supports the creation of metadata files for the new Iceberg table.

        :param target_table_suffix: suffix to append to the target table name.
        :param in_place_migration: whether to create new iceberg table while dropping old athena table and order by
        partition columns.
        :param validate_output_table: whether to check for discrepancy between old athena and new iceberg table
        """
        # 'migrate' procedure does not work for glue because it involves renaming of tables,
        # see https://github.com/apache/iceberg/issues/7317
        # https://aws.amazon.com/blogs/big-data/migrate-an-existing-data-lake-to-a-transactional-data-lake-using-apache-iceberg/
        # df = spark.sql(f"CALL spark_catalog.system.migrate('{db_name}.{table_name}')")

        table_name = f"{self._full_table_name}_{target_table_suffix}"
        logger.info(f"Migrating {self._full_table_name} to Apache Iceberg")

        partitioned_by = ""
        data_limit = "LIMIT 0"

        if len(self._table_partitions) > 0:
            comma_separated_partitions = ",".join(self._table_partitions)
            partitioned_by = f" PARTITIONED BY ({comma_separated_partitions})"

            if in_place_migration:
                data_limit = f"ORDER BY {comma_separated_partitions}"

        executed_drop_table = False
        backoff_iter = exponential_backoff(factor=2)

        while self.spark.catalog.tableExists(table_name) and not in_place_migration:
            if executed_drop_table:
                backoff_value = next(backoff_iter)
                logger.info(f"Waiting for {backoff_value}secs. for existing table to be dropped")
                sleep(backoff_value)
            else:
                logger.info(f"Dropping existing table: {table_name}")
                athena_client = boto3.client("athena")
                try:
                    response = athena_client.start_query_execution(
                        QueryString=f"DROP TABLE {self._full_table_name}_{target_table_suffix}"
                    )
                    logger.info(f"Query execution response: {response}")
                except ClientError as err:
                    raise PermissionError(
                        "Unable to drop the table via Athena client, it needs to be dropped manually"
                    ) from err
                executed_drop_table = True

        logger.info(
            f"Creating iceberg table {table_name} from {self._full_table_name} using in-place: {in_place_migration}"
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} USING iceberg{partitioned_by} AS
            SELECT * FROM {self._full_table_name} {data_limit}
            """
        )

        if in_place_migration:
            logger.info(f"Creating metadata for the new files in {self._full_table_name}")
            self.spark.sql(
                f"""
                CALL {self.catalog_name}.system.add_files(
                    table => '{table_name}',
                    source_table => '{self._full_table_name}
                ')
                """
            ).show()

        logger.info(print_header("Data files location"))
        self.spark.sql(f"SELECT file_path FROM {table_name}.files").show(truncate=False)

        logger.info(print_header("Metadata files"))
        self.spark.sql(f"SELECT snapshot_id, manifest_list FROM {table_name}.snapshots").show(truncate=False)

        LogConfig.log_dataframe(self.spark.sql(f"SELECT * FROM {table_name}"))

        if validate_output_table:
            diff_count = self.spark.sql(
                f"SELECT * FROM {table_name} EXCEPT ALL SELECT * FROM {self._full_table_name} "
                f"UNION SELECT * FROM {self._full_table_name} EXCEPT ALL SELECT * FROM {table_name}"
            ).count()

            if diff_count > 0:
                raise AssertionError(
                    f"{self._source_alias} table {self._full_table_name} and "
                    f"{self._target_alias} table {table_name} has a "
                    f"total discrepancy of {diff_count} rows"
                )

        logger.info("Migration completed successfully")

    def load_scd1(self, dataframe: DataFrame, pipeline_job_name: str):
        """
        Loads SCD1 into the target table.

        This method processes the input DataFrame to identify changes and updates the target table
        accordingly. It handles both updates and inserts based on the row hash value.

        :param dataframe: input DataFrame
        :param pipeline_job_name: The name of the job that last updated the table.
        :return:
        """
        delta_df_view, data_columns = self.prepare_delta_df(dataframe, pipeline_job_name)

        window = ""
        window_cols = ""
        window_cols_update = ""
        rn_filter = ""
        if self.has_control_fields:
            window = (
                f"WINDOW last_update_window AS (PARTITION BY {self.partition_by_id_cols} "
                f"ORDER BY {ControlColumns.CTRL_LAST_UPDATE_TS.name} DESC)"
            )
            window_cols = f""",
                LEAD({ControlColumns.CTRL_CREATE_TS.name}) OVER last_update_window AS eff_create_ts,
                LEAD({ControlColumns.CTRL_CREATE_USER_ID.name}) OVER last_update_window AS eff_create_user_ts,
                ROW_NUMBER() OVER last_update_window AS row_number
            """
            window_cols_update = f""",
                COALESCE(eff_create_ts, {ControlColumns.CTRL_LAST_UPDATE_TS.name})
                AS {ControlColumns.CTRL_CREATE_TS.name},
                COALESCE(eff_create_user_ts, {ControlColumns.CTRL_LAST_UPDATE_USER_ID.name})
                AS {ControlColumns.CTRL_CREATE_USER_ID.name},
                {ControlColumns.CTRL_LAST_UPDATE_TS.name},
                {ControlColumns.CTRL_LAST_UPDATE_USER_ID.name}
            """
            rn_filter = "WHERE row_number = 1"

        row_hash_col = ControlColumns.ROW_HASH_VALUE.name
        current_df_view = "current_df_view"
        self.spark.sql(
            f"""
            SELECT {self._target_alias}.* FROM {self._full_table_name} AS {self._target_alias}
            JOIN {delta_df_view} AS {self._source_alias} ON {self._merge_on_id_cols}
            WHERE {self._target_alias}.{row_hash_col} != {self._source_alias}.{row_hash_col}
            """
        ).createOrReplaceTempView(current_df_view)

        insert_df_view = "insert_df_view"
        self.spark.sql(
            f"""
            SELECT {self._source_alias}.*
            FROM {delta_df_view} AS {self._source_alias}
            LEFT JOIN {self._full_table_name} AS {self._target_alias} ON {self._merge_on_id_cols}
            WHERE {self._target_alias}.{row_hash_col} != {self._source_alias}.{row_hash_col} OR
                {self.is_null_id_cols} AND {self._target_alias}.{row_hash_col} IS NULL
        """
        ).select(self.spark.table(self._full_table_name).columns).createOrReplaceTempView(insert_df_view)

        self.spark.table(insert_df_view)

        table_update_query = f"""
            WITH table_to_update AS ((SELECT * FROM {current_df_view}) UNION (SELECT * FROM {insert_df_view})),
            table_updated AS (
                SELECT * {window_cols} FROM table_to_update {window}
            )
            SELECT {data_columns}, {row_hash_col} {window_cols_update} FROM table_updated {rn_filter}
        """

        df_table_update = self.spark.sql(table_update_query)
        df_table_update_view = "table_update"
        df_table_update.cache()

        self._log_dataframe(df_table_update, df_table_update_view)
        df_table_update = df_table_update.select(self.spark.table(f"{self.database_name}.{self.table_name}").columns)
        df_table_update.createOrReplaceTempView(df_table_update_view)

        self.spark.sql(
            f"""
            MERGE INTO {self._full_table_name} {self._target_alias}
            USING (SELECT * FROM {df_table_update_view}) {self._source_alias}
            ON {self._merge_on_id_cols}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        )

    def load_overwrite(self, dataframe: DataFrame):
        dataframe.writeTo(self._full_table_name).overwritePartitions()
