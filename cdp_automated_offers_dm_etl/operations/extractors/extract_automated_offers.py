from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_automated_offers = (
    base_extract.snapshot().last(is_max_date=False).with_db_name(Config().db_name_datamarts_automated_offers).reuse()
)

extract_automated_offers_for_month_period = (
    base_extract.snapshot()
    .for_month_period(months_back=2)
    .with_db_name(Config().db_name_datamarts_automated_offers)
    .reuse()
)

extract_dm_cross_sell_base = (
    extract_automated_offers.with_table("dm_cross_sell_base")
    .with_result_name("dm_cross_sell_base")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_cc_stp_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_cc_cross_sell_stp_offers")
    .with_result_name("dm_casa_to_cc_cross_sell_stp_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_al_stp_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_al_cross_sell_stp_offers")
    .with_result_name("dm_casa_to_al_cross_sell_stp_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_hl_stp_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_hl_cross_sell_stp_offers")
    .with_result_name("dm_casa_to_hl_cross_sell_stp_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_cc_ptb_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_cc_cross_sell_ptb_offers")
    .with_result_name("dm_casa_to_cc_cross_sell_ptb_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_al_ptb_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_al_cross_sell_ptb_offers")
    .with_result_name("dm_casa_to_al_cross_sell_ptb_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_dm_casa_to_hl_ptb_offers_for_month = (
    extract_automated_offers_for_month_period.with_table("dm_casa_to_hl_cross_sell_ptb_offers")
    .with_result_name("dm_casa_to_hl_cross_sell_ptb_offers_for_month")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
