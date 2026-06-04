from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_staging_automated_offers = (
    base_extract.snapshot().last(is_max_date=False).with_db_name(Config().db_name_staging_automated_offers).reuse()
)

extract_staging_cross_sell_base = (
    extract_staging_automated_offers.with_table("staging_cross_sell_base")
    .with_result_name("staging_cross_sell_base")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
