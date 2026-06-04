from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_dataiku_latest = base_extract.snapshot().last().with_db_name(Config().db_name_dataiku_prepared).reuse()


extract_ptbcasatocc_latest = (
    extract_dataiku_latest.with_table("ptbcasatocc")
    .with_actual_date_col_name("business_date")
    .with_result_name("ptbcasatocc")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_depo_segmentation_v2_latest = (
    extract_dataiku_latest.with_table("depo_segmentation_v2")
    .with_actual_date_col_name("business_date")
    .with_result_name("depo_segmentation_v2")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_incomeestimatorv2_latest = (
    extract_dataiku_latest.with_table("incomeestimatorv2")
    .with_actual_date_col_name("business_date")
    .with_result_name("incomeestimatorv2")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_depo_clusters_v2_latest = (
    extract_dataiku_latest.with_table("depo_clusters_v2")
    .with_actual_date_col_name("business_date")
    .with_result_name("depo_clusters_v2")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_casatohlptb_latest = (
    extract_dataiku_latest.with_table("casatohlptb")
    .with_actual_date_col_name("business_date")
    .with_result_name("casatohlptb")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_ptbcasatoal_latest = (
    extract_dataiku_latest.with_table("ptbcasatoal")
    .with_actual_date_col_name("business_date")
    .with_result_name("ptbcasatoal")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
