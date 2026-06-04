"""
This module contains the class for the :class:`~TableExtractor` and all extractor-type tasks.
"""
from datetime import datetime
from datetime import timedelta
from enum import Enum
from types import SimpleNamespace
from typing import Any
from typing import Dict
from typing import List
from typing import Union

from loguru import logger
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing_extensions import Literal
from typing_extensions import Self
from typing_extensions import TypeAlias
from typing_extensions import override

from xcomms.constants import DATE_FORMAT
from xflow.context import JobConfig
from xflow.context import JobContext
from xflow.task import TaskType
from xnl.commons.incremental_load import _get_actual_df
from xnl.metadata.base import Metadata
from xnl.table_builder.base import PipelineReturnBuilder
from xnl.table_builder.iceberg import IcebergClient
from xnl.table_builder.mixin import DatabaseNameMixin
from xnl.table_builder.mixin import Deferred
from xnl.table_builder.mixin import RequiredAttr

SnapshotExtractorType: TypeAlias = "SnapshotExtractor"
FullExtractorType: TypeAlias = "FullExtractor"
Scd2ExtractorType: TypeAlias = "Scd2Extractor"
ColumnOrName = Union[Column, str]


class ExtractMode(Enum):
    """
    Type of extraction using specific date
    """

    MIN = "minimum_date"
    """
    Extracts the minimum available partition within the month of given actual date
    """

    MAX = "max_date"
    """
    Extracts the maximum available partition not greater than the specific actual date
    """

    ACTUAL = "actual"
    """
    Extracts the snapshot exactly equal to specified actual date
    """


class TableExtractor(PipelineReturnBuilder, DatabaseNameMixin):
    """
    One of the builder classes which represents extracting of a single table.
    """

    def __init__(self):
        super().__init__()
        self.table_name: Deferred[str] = SimpleNamespace()
        self.actual_date_col_name: str = "actual_date"
        self.filter_condition: Deferred[ColumnOrName] = SimpleNamespace()

    def with_actual_date_col_from_metadata(self):
        """
        Adds a pre-processing step to fetch the actual date column from the metadata

        This method extracts the actual date column name from the table metadata. It assumes that
        the table is partitioned by a single column, which is inferred to be the actual date column.
        If the table has more than one partition, an error is raised.

        :return:
        """

        def _extract_actual_date_col_name(table_metadata: Metadata):
            partition_columns = table_metadata.partition_columns()
            if len(partition_columns) == 0:
                return {}

            if len(partition_columns) > 1:
                raise AssertionError(
                    "Only one partition is expected to exists which is inferred to be the actual_date_col_name, "
                    f"but found multiple partitions: {partition_columns}"
                )

            return {"actual_date_col_name": partition_columns[0]}

        return self.add_pre_processing(_extract_actual_date_col_name)

    def with_table(self, table_name: str) -> Self:
        """
        Sets the name of the table to extract.

        :param table_name: The table name.
        :return:
        """
        return self.copy_on_write(self.table_name, table_name)

    def with_actual_date_col_name(self, actual_date_col_name: str) -> Self:
        """
        The column name which represents the actual date.

        :param actual_date_col_name: The column name.
        :return:
        """
        return self.copy_on_write(self.actual_date_col_name, actual_date_col_name)

    def with_filter(self, filter_condition: ColumnOrName) -> Self:
        """
        Sets an option to provide a filter predicate to the extracted dataframe.

        :param filter_condition: The filter for the dataframe.
        :return:
        """

        def _df_filter(dataframe: DataFrame):
            return dataframe.filter(filter_condition)

        return self.add_post_processing(_df_filter)

    def snapshot(self) -> SnapshotExtractorType:
        """
        Provides different table snapshot options for extracting a table.

        :return:
        """
        return self._init_subclass(SnapshotExtractor)

    def full(self) -> FullExtractorType:
        """
        Extracts the full table only, the :meth:`~basic` method has to be called afterwards.

        :return:
        """
        return self._init_subclass(FullExtractor)

    def scd2(self) -> Scd2ExtractorType:
        """
        Extracts the latest/actual dataframe of an SCD2 table, the :meth:`~basic` method has to be called afterwards.

        :return:
        """
        return self._init_subclass(Scd2Extractor)

    @staticmethod
    def _truncate_date_to(
        column: str,
        level: Union[Literal["day"], Literal["month"], Literal["year"]],
        last_day: bool = False,
        actual_value: bool = True,
    ) -> str:
        if actual_value:
            column = f"'{column}'"
        actual_date_str = f"CAST({column} AS DATE)"
        is_day_level = level == "day"

        if not is_day_level:
            actual_date_str = f"TRUNC({actual_date_str}, '{level}')"

        if last_day and not is_day_level:
            if level == "year":
                actual_date_str = f"ADD_MONTHS({actual_date_str}, 11)"
            actual_date_str = f"LAST_DAY({actual_date_str})"

        return actual_date_str

    @override
    def _generate_all_kwargs(self, context: JobContext) -> Dict[str, Any]:
        kwargs = super()._generate_all_kwargs(context)
        db_name = "" if kwargs["db_name"] == context.config.VIEW.value else f"`{kwargs['db_name']}`."
        kwargs.update({"full_table_name": f"{db_name}`{kwargs['table_name']}`"})

        return kwargs

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.table_name, self.with_table)]

    @override
    @property
    def task_type(self) -> TaskType:
        return TaskType.EXTRACTOR

    def with_provided_dataframe(self) -> Self:
        """
        Provides the table immediately for pre-processing purposes before doing the extraction.

        :return:
        """
        return self.add_pre_processing(lambda spark, full_table_name: {"dataframe": spark.table(full_table_name)})

    def _load_dataframe(self, spark: SparkSession, full_table_name: str, dataframe: DataFrame = None) -> DataFrame:
        return dataframe if dataframe is not None else spark.table(full_table_name)


class SnapshotExtractor(TableExtractor):
    """
    Do not use this class directly, access it by calling `Table.extractor().snapshot()`.
    """

    def business_day_y1dpf(
        self, months_back: int = 1, nth_day: Union[Literal["end"], Literal["half"], int] = "end"
    ) -> Self:
        """
        Extracts the nth business day based on the given actual date using the FBEQ's y1dpf table.

        :param months_back: The nth previous months from the actual date.
        :param nth_day: The nth business day of the specified previous month. Defaults to 'end' which would return
            the last business day for the month. The 'half' would return the half of the month, while any value which
            are out of bounds would return [1st, last] business day respectively, otherwise specifying the nth
            business day.
        :return:
        """

        def _business_day_y1dpf(
            spark: SparkSession,
            config: JobConfig,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            months_back: int,
            nth_day: int,
            dataframe: DataFrame = None,
        ):
            actual_date = datetime.strptime(actual_date, "%Y-%m-%d")
            for _ in range(months_back):
                actual_date = actual_date.replace(day=1) - timedelta(days=1)

            month_bd = spark.sql(
                f"""
                SELECT cdate_date AS actual_date
                FROM `{config.db_name_fbeq_prepared.value}`.`y1dpf`
                WHERE EXTRACT(MONTH FROM cdate_date) = {actual_date.month}
                AND EXTRACT(YEAR FROM cdate_date) = {actual_date.year}
            """
            )

            if isinstance(nth_day, int):
                nth_day = max(nth_day, 1)
                month_bd = month_bd.sort("actual_date").collect()
                nth_day = len(month_bd) - 1 if len(month_bd) < nth_day else nth_day - 1
                month_bd = month_bd[nth_day][0]
            elif nth_day == "end":
                month_bd = month_bd.filter("pflag = 'M'").collect()[0][0]
            elif nth_day == "half":
                month_bd = month_bd.filter("pflag = 'H'").collect()[0][0]
            else:
                raise ValueError(f"Unsupported argument for nth_day: {nth_day}")

            dataframe = self._load_dataframe(spark, full_table_name, dataframe)
            return dataframe.filter(f"{actual_date_col_name} = date'{month_bd}'")

        return self._set_main_function(_business_day_y1dpf, months_back=months_back, nth_day=nth_day)

    def last(self, mode: ExtractMode = ExtractMode.MAX, is_max_date: bool = True, months_back: int = 0) -> Self:
        """
        Extracts the latest available snapshot of the table which is not greater than the specified actual date by
        default.

        :param mode: Determine the type of extraction performed using the specified actual date. Refer to the enum class
                     for definition.
        :param is_max_date: Whether should the extraction get the latest one available or not.
                            **Note**: This parameter is retained for backward compatibility.
                            It is recommended to use the `mode` parameter for new usage.
        :param months_back: Nth month when the specific date should be retrieved.
        :return:
        """

        def _last(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            mode: ExtractMode,
            is_max_date: bool,
            months_back: int,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            is_df_provided = dataframe is not None
            dataframe = self._load_dataframe(spark, full_table_name, dataframe)

            # For backward compatibility of `is_max_date`
            mode = (
                ExtractMode.MAX
                if (is_max_date and mode == ExtractMode.MAX)
                else ExtractMode.ACTUAL
                if (not is_max_date and mode == ExtractMode.MAX)
                else mode
            )

            def _adjust_date_to_months_back(input_date: str, _months_back: int = 0) -> str:
                if _months_back > 0:
                    _trunc_month = self._truncate_date_to(input_date, "month")
                    return f"DATE_ADD(ADD_MONTHS({_trunc_month}, -{_months_back - 1}), -1)"
                return f"CAST('{input_date}' AS DATE)"

            if mode == ExtractMode.MAX:
                actual_date_str = _adjust_date_to_months_back(actual_date, months_back)

                query = f"""
                    SELECT COALESCE(MAX({actual_date_col_name}), DATE '2000-01-01')
                    FROM {full_table_name}
                    WHERE {actual_date_col_name} <= {actual_date_str}
                """  # nosec

                if is_df_provided:
                    default_date = datetime.strptime("2000-01-01", "%Y-%m-%d")
                    filtered_df = dataframe.filter(F.col(actual_date_col_name) <= F.to_date(F.lit(actual_date)))
                    actual_date = filtered_df.select(
                        F.coalesce(F.max(F.col(actual_date_col_name)), F.lit(default_date))
                    ).collect()[0][0]
                else:
                    actual_date = spark.sql(query).collect()[0][0]

            elif mode == ExtractMode.MIN:
                actual_date_str = _adjust_date_to_months_back(actual_date, months_back)

                year_month_filter = (
                    f"EXTRACT(YEAR FROM {actual_date_col_name}) = EXTRACT(YEAR FROM {actual_date_str}) "
                    f"AND EXTRACT(MONTH FROM {actual_date_col_name}) = EXTRACT(MONTH FROM {actual_date_str})"
                )

                if is_df_provided:
                    actual_date = (
                        dataframe.filter(year_month_filter).select(F.min(F.col(actual_date_col_name))).collect()[0][0]
                    )
                else:
                    query = f"""
                        SELECT COALESCE(MIN({actual_date_col_name}), DATE '2000-01-01')
                        FROM {full_table_name}
                        WHERE {year_month_filter}
                    """
                    actual_date = spark.sql(query).collect()[0][0]

            logger.info(f"Extracting last snapshot from {full_table_name} on {actual_date}")
            return dataframe.filter(f"{actual_date_col_name} = date'{actual_date}'")

        return self._set_main_function(_last, mode=mode, is_max_date=is_max_date, months_back=months_back)

    def on_date(self, actual_date: str = None, inclusive: bool = True) -> Self:
        """
        Extracts a snapshot of the table until the specified date, i.e. <= actual_date. If the actual date must not
        be included in the snapshot, set inclusive to False.

        :param actual_date: The actual date.
        :param inclusive: Whether the actual date should be included in the snapshot or not.
        :return:
        """

        def _on_date(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            inclusive: bool,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            actual_date_filter = f"{actual_date_col_name} <{'=' if inclusive else ''} date'{actual_date}'"
            date_log = actual_date_filter.replace("date'", "").replace("'", "")
            logger.info(f"Extracting snapshot from {full_table_name} with {date_log}")

            dataframe = self._load_dataframe(spark, full_table_name, dataframe)
            return dataframe.filter(actual_date_filter)

        return self._set_main_function(_on_date, actual_date=actual_date, inclusive=inclusive)

    def for_month_period(self, months_back: int = None, include_actual_date_month: bool = True) -> Self:
        """
        Extracts a snapshot of the table within specified months of the actual date. For instance, if a given actual
        date is 2023-01-10, and months_back to 4, then it would extract 2022-09-11 <= actual_date <= 2023-01-10.
        If the actual date should not be included, set it to False, then it would instead extract 2022-09-01 <=
        actual_date <= 2023-12-31.

        :param months_back: The number of previous months that should be included.
        :param include_actual_date_month: Whether to include the actual date's month or not.
        :return:
        """

        def _for_month_period(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            months_back: int,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            if not include_actual_date_month and months_back == 0:
                raise ValueError("The given parameters does not cover any dates to extract")

            logger.info(
                f"Extracting snapshot from {full_table_name} within last {months_back} months until {actual_date}"
            )
            dataframe = self._load_dataframe(spark, full_table_name, dataframe)
            actual_date_lit = f"CAST('{actual_date}' AS DATE)"
            trunc_month = self._truncate_date_to(actual_date, "month")

            if include_actual_date_month:
                lower_date = f"> ADD_MONTHS({actual_date_lit}, -{months_back})"
                if months_back == 0:
                    lower_date = f">= {trunc_month}"

                return dataframe.filter(f"{actual_date_col_name} {lower_date}").filter(
                    f"{actual_date_col_name} <= {actual_date_lit}"
                )

            return dataframe.filter(f"{actual_date_col_name} >= ADD_MONTHS({trunc_month}, -{months_back})").filter(
                f"{actual_date_col_name} < {trunc_month}"
            )

        return self._set_main_function(_for_month_period, months_back=months_back)

    def for_day_period(self, days_back: int = None) -> Self:
        """
        Extracts a snapshot of the table within specified days of the actual date. For instance, if a given actual
        date is 2023-01-10, and days_back to 4, then it would extract 2022-01-07 <= actual_date <= 2023-01-10.

        :param days_back: The number of previous days that should be included.
        :return:
        """

        def _for_day_period(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            days_back: int,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            logger.info(f"Extracting snapshot from {full_table_name} within last {days_back} days until {actual_date}")
            dataframe = self._load_dataframe(spark, full_table_name, dataframe)
            return dataframe.filter(
                f"{actual_date_col_name} > DATE_SUB(CAST('{actual_date}' AS DATE), {days_back})"
            ).filter(f"{actual_date_col_name} <= CAST('{actual_date}' AS DATE)")

        return self._set_main_function(_for_day_period, days_back=days_back)

    def historical_range(
        self,
        start_dt: str = None,
        end_dt: str = None,
        trunc_dt: Literal["year", "month", "day"] = "day",
        start_dt_col: str = None,
        end_dt_col: str = None,
    ) -> Self:
        """
        Extracts a snapshot based exactly on the date range specified in the parameters. This is meant to be the most
        generic method for table extraction.

        :param start_dt: The start date.
        :param end_dt: The end date.
        :param trunc_dt: Whether to truncate the specified date. Defaults to day which does not truncate anything.
        :param start_dt_col: The name of the start date column. If both start_dt_col and end_dt_col are not specified,
            then it is set to the actual_date_col_name, both of them should be provided or not.
        :param end_dt_col: The name of the end date column. If both start_dt_col and end_dt_col are not specified,
            then it is set to the actual_date_col_name, both of them should be provided or not.
        :return:
        """

        def _historical_range(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            actual_date_col_name: str,
            start_dt: str,
            end_dt: str,
            trunc_dt: str,
            start_dt_col: str,
            end_dt_col: str,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            dataframe = self._load_dataframe(spark, full_table_name, dataframe)

            if start_dt_col is None and end_dt_col is None:
                logger.info(
                    f"Extracting historical snapshot from {full_table_name} "
                    f"with {start_dt} <= {actual_date_col_name} <= {end_dt}"
                )

                return dataframe.filter(
                    f"{self._truncate_date_to(start_dt, trunc_dt)} <= {actual_date_col_name} "
                    f"AND {actual_date_col_name} <= {self._truncate_date_to(end_dt, trunc_dt, last_day=True)}"
                )

            logger.info(
                f"Extracting historical snapshot from {full_table_name} "
                f"with {start_dt_col} <= {actual_date} < {end_dt_col}"
            )
            return dataframe.filter(f"{start_dt_col} <= date'{actual_date}' AND date'{actual_date}' < {end_dt_col}")

        return self._set_main_function(
            _historical_range,
            start_dt_col=start_dt_col,
            end_dt_col=end_dt_col,
            start_dt=start_dt,
            end_dt=end_dt,
            trunc_dt=trunc_dt,
        )


class FullExtractor(TableExtractor):
    """
    Do not use this class directly, access it by calling `Table.extractor().full()`.
    """

    def basic(self) -> Self:
        """
        Initializes the function needed for the full table extraction.

        :return:
        """

        def _full_extract(spark: SparkSession, full_table_name: str, dataframe: DataFrame = None) -> DataFrame:
            logger.info(f"Extracting full snapshot from {full_table_name}")
            dataframe = self._load_dataframe(spark, full_table_name, dataframe)
            return dataframe

        return self._set_main_function(_full_extract)


class Scd2Extractor(TableExtractor):
    """
    Do not use this class directly, access it by calling `Table.extractor().scd2()`.
    """

    def __init__(self):
        super().__init__()
        self.add_pre_processing(self._clean_args)

    @staticmethod
    def _clean_args(business_keys: Union[str, List[str]], actual_date: str):
        if isinstance(business_keys, str):
            business_keys = [business_keys]
        if isinstance(actual_date, str):
            actual_date = datetime.strptime(actual_date, DATE_FORMAT)

        return {"business_keys": business_keys, "actual_date": actual_date}

    def basic(self, del_flag: bool = False) -> Self:
        """
        Initializes the function needed for the SCD2 table extraction.

        :param del_flag: The flag whether we to mark deleted rows or not.
        :return:
        """

        def _scd2_extract(
            spark: SparkSession,
            full_table_name: str,
            actual_date: datetime,
            actual_date_col_name: str,
            business_keys: List[str],
            del_flag: bool,
            dataframe: DataFrame = None,
        ) -> DataFrame:
            return _get_actual_df(
                dataframe=self._load_dataframe(spark, full_table_name, dataframe),
                actual_date=actual_date,
                business_keys=business_keys,
                actual_date_col_name=actual_date_col_name,
                del_flag=del_flag,
            )

        return self._set_main_function(_scd2_extract, del_flag=del_flag)

    def iceberg(self) -> "IcebergScd2Extractor":
        """
        Extracts the latest/actual dataframe of an SCD2 table, the :meth:`~basic` method has to be called afterwards.

        :return:
        """
        return self._init_subclass(IcebergScd2Extractor)


class IcebergScd2Extractor(Scd2Extractor):
    """
    Use this class by calling Table.extractor().scd2().iceberg()
    """

    def basic(self):  # pylint: disable=arguments-differ
        """
        Initializes the function needed for Iceberg SCD2 table extraction.
        """

        def _iceberg_scd2_extract(
            spark: SparkSession,
            full_table_name: str,
            actual_date: str,
            business_keys: List[str],
            catalog_name: str = IcebergClient.DEFAULT_CATALOG,
        ):
            db_name, table_name = full_table_name.split(".")
            client = IcebergClient(business_keys, table_name, spark, actual_date, db_name, catalog_name)
            return client.read_scd2()

        return self._set_main_function(_iceberg_scd2_extract)
