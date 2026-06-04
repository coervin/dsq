from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_crm = base_extract.snapshot().last(is_max_date=False).with_db_name(Config().db_name_datamarts_crm).reuse()

extract_cust_info = (
    extract_crm.with_table("cust_info")
    .with_result_name("cust_info")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
