from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract


extract_crm_replica_db_latest = (
    base_extract.snapshot().last().with_db_name(Config().db_name_crm_replica_db_prepared).reuse()
)

extract_acc_ex2_latest = (
    extract_crm_replica_db_latest.with_table("acc_ex2")
    .with_result_name("acc_ex2")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_acc_ex1_latest = (
    extract_crm_replica_db_latest.with_table("acc_ex1")
    .with_result_name("acc_ex1")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_lea_ex1_latest = (
    extract_crm_replica_db_latest.with_table("lea_ex1")
    .with_result_name("lea_ex1")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_activity_latest = (
    extract_crm_replica_db_latest.with_table("activity")
    .with_result_name("activity")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_lookupmaster_latest = (
    extract_crm_replica_db_latest.with_table("lookupmaster")
    .with_result_name("lookupmaster")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_leads_latest = (
    extract_crm_replica_db_latest.with_table("leads")
    .with_result_name("leads")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)

extract_products_latest = (
    extract_crm_replica_db_latest.with_table("products")
    .with_result_name("products")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
