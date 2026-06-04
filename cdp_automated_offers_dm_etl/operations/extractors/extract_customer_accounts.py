from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract


extract_customer_accounts_latest = (
    base_extract.snapshot().last().with_db_name(Config().db_name_datamarts_customer_accounts).reuse()
)

extract_df_customer_account_latest = (
    extract_customer_accounts_latest.with_table("df_customer_account")
    .with_actual_date_col_name("reporting_date")
    .with_result_name("df_customer_account")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
