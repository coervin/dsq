from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_dsp_gap_files_latest = (
    base_extract.snapshot().last().with_db_name(Config().db_name_dsp_gap_files_prepared).reuse()
)

extract_dsp_gap_files_all = base_extract.full().basic().with_db_name(Config().db_name_dsp_gap_files_prepared).reuse()

extract_branch_listing_latest = (
    extract_dsp_gap_files_latest.with_table("branch_listing")
    .with_actual_date_col_name("actual_date")
    .with_result_name("branch_listing")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_business_days_all = (
    extract_dsp_gap_files_all.with_table("business_days")
    .with_result_name("business_days")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_holidays_all = (
    extract_dsp_gap_files_all.with_table("holidays")
    .with_result_name("holidays")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
