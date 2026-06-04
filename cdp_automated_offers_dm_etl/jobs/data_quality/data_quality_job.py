from copy import deepcopy
from typing import Iterator

from loguru import logger
from xflow.context import JobConfig
from xflow.job import JobFlow
from xflow.task import DictResult, TaskResult, TaskResultType, initializer
from xnl.data_quality.dq_job import ERROR_TABLE_META, DataQualityConfig, DataQualityJob
from xnl.metadata.metadata import TableMetadata
from xnl.table_builder import Table
from xnl.table_builder.mixin import RequiredAttr

from cdp_automated_offers_dm_etl.config.config import Config


class DQJob(DataQualityJob):

    @property
    def config(self) -> DataQualityConfig:
        return DataQualityConfig("automated_offers_dm", Config().db_name_datamarts_automated_offers)

    @initializer
    def read_metadata(self, config: JobConfig, job_name: str) -> Iterator[TaskResult]:
        """
        Retrieve the required table schema for the DQ error table.

        :param config: The configuration where to retrieve the database name and location.
        :param job_name: The name of the job.
        :return:
        """
        logger.info(f"Job name in read_metadata {job_name}")
        error_metadata = deepcopy(ERROR_TABLE_META)
        error_metadata["name"] = self.config.repository_name
        table_metadata = TableMetadata.from_dict(error_metadata)

        yield DictResult(
            "metadata",
            {
                "target_table_name": table_metadata.name,
                "target_db_name": config.db_name_dq_validation.value,
                "target_table_location": f"{config.dq_validation_location.value}/{table_metadata.name}",
                "table_error_metadata": table_metadata,
            },
        )

    @property
    def metadata_flow(self) -> JobFlow:
        """
        Metadata flow overriden for :class:`~DataQualityJob`.

        :return:
        """

        def add_table_meta(metadata: TableMetadata):
            return {
                "table_name": metadata.name,
            }

        return [
            Table.metadata()
            .from_job_context()
            .add_pre_processing(
                lambda metadata_name: {"metadata_name": metadata_name.replace(DataQualityJob.job_prefix, "")}
            )
            .add_post_processing(add_table_meta)
            .with_result_name("table_source_metadata"),
            self.read_metadata,
        ]

    @property
    def process_table_flow(self) -> JobFlow:
        """
        Table processing flow overriden for :class:`~DataQualityJob`. Extracts the last snapshot of the concerned
        table and then execute data quality checks.

        :return:
        """

        if "dm" in self.context.job_name and self.context.job_name != "dm_total_relationship_balance":
            return [
                (
                    Table.extractor()
                    .full()
                    .basic()
                    .with_actual_date_col_from_metadata()
                    .rename_kwargs(table_source_metadata="table_metadata")
                    .with_table(RequiredAttr.PROVIDED)
                    .with_db_name(self.config.db_name)
                    .with_result_name("dataframe")
                    .with_result_type(TaskResultType.DATAFRAME)
                ),
                self.dq_task,
            ]
        return super().process_table_flow
