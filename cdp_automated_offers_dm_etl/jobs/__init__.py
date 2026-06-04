# flake8: noqa

# staging
from cdp_automated_offers_dm_etl.jobs.staging.staging_cross_sell_base import AutoOffersStgCrossSellBase

# dataframes
from cdp_automated_offers_dm_etl.jobs.dataframes.dm_cross_sell_base import AutoOffersDmCrossSellBase

from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_cc_cross_sell_stp_offers import (
    AutoOffersDmCasaToCCSTPOffers,
)

from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_al_cross_sell_stp_offers import (
    AutoOffersDmCasaToALSTPOffers,
)
from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_hl_cross_sell_stp_offers import (
    AutoOffersDmCasaToHLSTPOffers,
)
from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_cc_cross_sell_ptb_offers import (
    AutoOffersDmCasaToCCPTBOffers,
)

from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_al_cross_sell_ptb_offers import (
    AutoOffersDmCasaToALPTBOffers,
)

from cdp_automated_offers_dm_etl.jobs.dataframes.dm_casa_to_hl_cross_sell_ptb_offers import (
    AutoOffersDmCasaToHLPTBOffers,
)
