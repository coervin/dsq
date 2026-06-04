from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_gap_files_latest = base_extract.snapshot().last().with_db_name(Config().db_name_gap_prepared).reuse()

extract_gap_files_all = base_extract.full().basic().with_db_name(Config().db_name_gap_prepared).reuse()

extract_dosri_distinct_latest = (
    extract_gap_files_latest.with_table("dosri_distinct")
    .with_result_name("dosri_distinct")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_enfis_bbn_latest = (
    extract_gap_files_latest.with_table("enfis_bbn")
    .with_result_name("enfis_bbn")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_ref_risk_industry_all = (
    extract_gap_files_all.with_table("ref_risk_industry")
    .with_result_name("ref_risk_industry")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_pl_active_accounts_latest = (
    extract_gap_files_latest.with_table("pl_active_accounts")
    .with_result_name("pl_active_accounts")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_pl_active_accounts_woff_latest = (
    extract_gap_files_latest.with_table("pl_active_accounts_woff")
    .with_result_name("pl_active_accounts_woff")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_salad_accounts_latest = (
    extract_gap_files_latest.with_table("salad_accounts")
    .with_result_name("salad_accounts")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_winnow_b_score_cc_latest = (
    extract_gap_files_latest.with_table("winnow_b_score_cc")
    .with_actual_date_col_name("period_date")
    .with_result_name("winnow_b_score_cc")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)

extract_act_ex1_latest = (
    extract_gap_files_latest.with_table("act_ex1")
    .with_result_name("act_ex1")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
)
