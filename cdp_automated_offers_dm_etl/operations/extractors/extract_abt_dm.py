from xflow.task import TaskResultType
from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_abt_latest = (
    base_extract.snapshot()
    .last()
    .with_actual_date_col_name("business_date")
    .with_db_name(Config().db_name_datamarts_abt)
    .reuse()
)

extract_df_abt_customer_latest = (
    extract_abt_latest.add_kwargs(except_cols=["bbnumber"])
    .with_table("df_abt_customer")
    .with_result_name("df_abt_customer")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
