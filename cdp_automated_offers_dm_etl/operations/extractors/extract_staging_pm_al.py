from xflow.task import TaskResultType

from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract

extract_staging_pm_al_all = base_extract.full().basic().with_db_name(Config().db_name_staging_pm_al).reuse()


extract_staging_pm_al_non_grid_application_data_all = (
    extract_staging_pm_al_all.with_table("staging_pm_al_non_grid_application_data")
    .with_result_name("staging_pm_al_non_grid_application_data")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .reuse()
)
