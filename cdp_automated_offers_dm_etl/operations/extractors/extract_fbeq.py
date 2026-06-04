from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_fbeq_latest = (
    base_extract.snapshot()
    .last()
    .with_actual_date_col_name("business_date")
    .with_db_name(Config().db_name_fbeq_prepared)
    .reuse()
)

extract_scpf_latest = (
    extract_fbeq_latest.with_table("scpf")
    .with_result_name("scpf")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
