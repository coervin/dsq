from xflow.job import ComposableJob, JobFlow
from xnl.table_builder import Table

from cdp_automated_offers_dm_etl.operations.initializers.init_vars import meta_initializer
from cdp_automated_offers_dm_etl.operations.extractors.extract_staging_automated_offers import (
    extract_staging_cross_sell_base,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_automated_offers import (
    extract_dm_casa_to_hl_stp_offers_for_month,
    extract_dm_casa_to_al_ptb_offers_for_month,
    extract_dm_casa_to_cc_ptb_offers_for_month,
    extract_dm_casa_to_hl_ptb_offers_for_month,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_dataiku import (
    extract_casatohlptb_latest,
)
from cdp_automated_offers_dm_etl.operations.transformers.transform_dm_casa_to_hl_cross_sell_ptb_offers import (
    transform_dm_casa_to_hl_cross_sell_ptb_offers,
)


class AutoOffersDmCasaToHLPTBOffers(ComposableJob):
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
            extract_casatohlptb_latest,
            extract_dm_casa_to_hl_stp_offers_for_month,
            extract_dm_casa_to_al_ptb_offers_for_month,
            extract_dm_casa_to_cc_ptb_offers_for_month,
            extract_dm_casa_to_hl_ptb_offers_for_month,
            transform_dm_casa_to_hl_cross_sell_ptb_offers,
        ]

    @property
    def load_table_flow(self) -> JobFlow:
        return Table.loader()
