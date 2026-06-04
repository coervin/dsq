from xnl.metadata.metadata import TableMetadata
from xnl.table_builder import Table

from cdp_automated_offers_dm_etl.config.config import Config


def add_target_meta(metadata: TableMetadata, config: Config):
    return {
        "target_table_name": metadata.name,
        "target_db_name": config.db_name_datamarts_automated_offers.value,
        "target_table_location": f"{config.datamarts_location_automated_offers.value}/{metadata.name}",
        "target_directory_dump": config.export_location_exponea.value,
        "target_temp_location": f"{config.datamarts_location_automated_offers.value}/temp",
    }


def add_target_stg_meta(metadata: TableMetadata, config: Config):

    meta = add_target_meta(metadata, config)
    meta["target_db_name"] = config.db_name_staging_automated_offers.value
    meta["target_table_location"] = f"{config.staging_location_automated_offers.value}/{metadata.name}"

    return meta


meta_initializer = (
    Table.metadata().from_job_context().add_post_processing(add_target_meta).with_result_name("table_metadata")
)

stg_meta_initializer = (
    Table.metadata().from_job_context().add_post_processing(add_target_stg_meta).with_result_name("table_metadata")
)
