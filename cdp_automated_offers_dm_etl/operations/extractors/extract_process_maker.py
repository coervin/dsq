from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract


extract_process_maker_13mb = (
    base_extract.snapshot()
    .with_actual_date_col_name("business_date")
    .for_month_period(months_back=13)
    .with_db_name(Config().db_name_pm_prepared)
    .reuse()
)

extract_credit_card_processing_tu_13mb = (
    extract_process_maker_13mb.with_table("credit_card_processing_tu")
    .with_result_name("credit_card_processing_tu")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
