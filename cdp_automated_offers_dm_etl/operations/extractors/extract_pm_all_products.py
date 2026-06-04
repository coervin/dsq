from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract
from cdp_automated_offers_dm_etl.utils.sql_utils import clean_mobile_num

extract_pm_all_prod_dm_latest = (
    base_extract.snapshot().last().with_db_name(Config().db_name_datamarts_pm_all_products).reuse()
)

extract_pm_all_prod_dm_for_month_period = (
    base_extract.snapshot()
    .for_month_period(months_back=1)
    .with_actual_date_col_name("reporting_date")
    .with_db_name(Config().db_name_datamarts_pm_all_products)
    .reuse()
)


extract_df_pm_eq_latest = (
    extract_pm_all_prod_dm_latest.with_table("df_pm_eq")
    .with_actual_date_col_name("reporting_date")
    .with_result_name("df_pm_eq")
    .add_post_processing(clean_mobile_num)
    .add_kwargs(mobile_column=["eq_customer_mobile_phone", "borrower_mobile_number"])
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)


extract_pm_application_latest = (
    extract_pm_all_prod_dm_latest.with_table("pm_application")
    .with_result_name("pm_application")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_df_pm_eq_for_month_period = (
    extract_pm_all_prod_dm_for_month_period.with_table("df_pm_eq")
    .with_result_name("df_pm_eq")
    .add_post_processing(clean_mobile_num)
    .add_kwargs(mobile_column=["eq_customer_mobile_phone", "borrower_mobile_number"])
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
