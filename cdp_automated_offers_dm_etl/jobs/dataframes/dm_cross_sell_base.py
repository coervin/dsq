from xflow.job import ComposableJob, JobFlow
from xnl.table_builder import Table

from cdp_automated_offers_dm_etl.operations.extractors.extract_staging_automated_offers import (
    extract_staging_cross_sell_base,
)
from cdp_automated_offers_dm_etl.operations.initializers.init_vars import meta_initializer
from cdp_automated_offers_dm_etl.operations.transformers.transform_dm_cross_sell_base import (
    transform_dm_cross_sell_base,
)


class AutoOffersDmCrossSellBase(ComposableJob):
    @property
    def metadata_flow(self) -> JobFlow:
        return meta_initializer

    @property
    def create_table_flow(self) -> JobFlow:
        return Table.initializer()

    @property
    def process_table_flow(self) -> JobFlow:
        return [
            extract_staging_cross_sell_base,
            transform_dm_cross_sell_base,
        ]

    @property
    def load_table_flow(self) -> JobFlow:
        return Table.loader()
