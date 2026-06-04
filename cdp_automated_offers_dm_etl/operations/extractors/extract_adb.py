from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract


extract_adb_4mb = (
    base_extract.snapshot().for_month_period(months_back=4).with_db_name(Config().db_name_datamarts_adb).reuse()
)

extract_df_adb_4mb = (
    extract_adb_4mb.with_table("df_adb")
    .with_result_name("df_adb")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
