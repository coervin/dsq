from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_prime_cc = (
    base_extract.snapshot()
    .last(is_max_date=False)
    .with_actual_date_col_name("reporting_date")
    .with_db_name(Config().db_name_datamarts_prime_cc)
    .reuse()
)

extract_df_cards_accounts = (
    extract_prime_cc.with_table("df_cards_accounts")
    .with_result_name("df_cards_accounts")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
