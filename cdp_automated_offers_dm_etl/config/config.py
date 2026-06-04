from xflow.context import config_dataclass, ParamStoreKey
from xflow.job import JobConfig


@config_dataclass
class Config(JobConfig):
    db_name_datamarts_automated_offers: ParamStoreKey
    datamarts_location_automated_offers: ParamStoreKey
    db_name_staging_automated_offers: ParamStoreKey
    staging_location_automated_offers: ParamStoreKey
    db_name_datamarts_clare: ParamStoreKey
    datamarts_location_clare: ParamStoreKey
    export_location_exponea: ParamStoreKey
    db_name_datamarts_customer_accounts: ParamStoreKey
    db_name_dwh_customer: ParamStoreKey
    db_name_dataiku_prepared: ParamStoreKey
    db_name_gap_prepared: ParamStoreKey
    db_name_datamarts_pm_all_products: ParamStoreKey
    db_name_fbeq_prepared: ParamStoreKey
    db_name_staging_pm_al: ParamStoreKey
    db_name_datamarts_prime_cc: ParamStoreKey
    db_name_datamarts_abt: ParamStoreKey
    db_name_datamarts_adb: ParamStoreKey
    db_name_pm_prepared: ParamStoreKey
    db_name_crm_replica_db_prepared: ParamStoreKey
    db_name_dsp_gap_files_prepared: ParamStoreKey
    db_name_datamarts_crm: ParamStoreKey
    metadata_location: ParamStoreKey
