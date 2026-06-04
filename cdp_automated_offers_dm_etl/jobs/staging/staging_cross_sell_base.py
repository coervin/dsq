from xflow.job import ComposableJob, JobFlow
from xnl.table_builder import Table

from cdp_automated_offers_dm_etl.operations.initializers.init_vars import stg_meta_initializer
from cdp_automated_offers_dm_etl.operations.extractors.extract_abt_dm import extract_df_abt_customer_latest
from cdp_automated_offers_dm_etl.operations.extractors.extract_adb import extract_df_adb_4mb
from cdp_automated_offers_dm_etl.operations.extractors.extract_customer_accounts import (
    extract_df_customer_account_latest,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_dataiku import (
    extract_depo_segmentation_v2_latest,
    extract_incomeestimatorv2_latest,
    extract_ptbcasatocc_latest,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_dwh_customer import (
    extract_dim_cust_class,
    extract_dim_cust_indiv,
    extract_dim_cust_indiv_pii,
    extract_dim_cust_contact,
    extract_dim_cust_contact_pii,
    extract_dim_cust_contact_email,
    extract_dim_cust_contact_pii_mobile,
    extract_dim_cust_contact_pii_landline,
    extract_dim_cust_address,
    extract_dim_cust_address_pii,
    extract_dim_cust_id_proof,
    extract_dim_cust_id_proof_pii,
    extract_dim_cust_indiv_employment,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_fbeq import extract_scpf_latest

from cdp_automated_offers_dm_etl.operations.extractors.extract_gap_files import (
    extract_dosri_distinct_latest,
    extract_enfis_bbn_latest,
    extract_pl_active_accounts_latest,
    extract_pl_active_accounts_woff_latest,
    extract_ref_risk_industry_all,
    extract_salad_accounts_latest,
    extract_winnow_b_score_cc_latest,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_pm_all_products import (
    extract_df_pm_eq_for_month_period,
    extract_pm_application_latest,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_prime_cc_dm import extract_df_cards_accounts
from cdp_automated_offers_dm_etl.operations.extractors.extract_process_maker import (
    extract_credit_card_processing_tu_13mb,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_staging_pm_al import (
    extract_staging_pm_al_non_grid_application_data_all,
)

from cdp_automated_offers_dm_etl.operations.extractors.extract_dsp_gap_files import (
    extract_branch_listing_latest,
)
from cdp_automated_offers_dm_etl.operations.extractors.extract_crm import extract_cust_info
from cdp_automated_offers_dm_etl.operations.transformers.transform_staging_cross_sell_base import (
    transform_staging_cross_sell_base,
)


class AutoOffersStgCrossSellBase(ComposableJob):
    @property
    def metadata_flow(self) -> JobFlow:
        return stg_meta_initializer

    @property
    def create_table_flow(self) -> JobFlow:
        return Table.initializer()

    @property
    def process_table_flow(self) -> JobFlow:
        return [
            extract_df_customer_account_latest,
            extract_dim_cust_indiv_pii,
            extract_ptbcasatocc_latest,
            extract_dosri_distinct_latest,
            extract_dim_cust_class,
            extract_depo_segmentation_v2_latest,
            extract_df_pm_eq_for_month_period,
            extract_scpf_latest,
            extract_enfis_bbn_latest,
            extract_ref_risk_industry_all,
            extract_staging_pm_al_non_grid_application_data_all,
            extract_pm_application_latest,
            extract_pl_active_accounts_latest,
            extract_pl_active_accounts_woff_latest,
            extract_df_cards_accounts,
            extract_salad_accounts_latest,
            extract_winnow_b_score_cc_latest,
            extract_df_abt_customer_latest,
            extract_df_adb_4mb,
            extract_credit_card_processing_tu_13mb,
            extract_incomeestimatorv2_latest,
            extract_dim_cust_indiv,
            extract_dim_cust_contact,
            extract_dim_cust_contact_pii,
            extract_dim_cust_contact_email,
            extract_dim_cust_contact_pii_mobile,
            extract_dim_cust_contact_pii_landline,
            extract_dim_cust_address,
            extract_dim_cust_address_pii,
            extract_dim_cust_id_proof,
            extract_dim_cust_id_proof_pii,
            extract_cust_info,
            extract_dim_cust_indiv_employment,
            extract_branch_listing_latest,
            transform_staging_cross_sell_base,
        ]

    @property
    def load_table_flow(self) -> JobFlow:
        return Table.loader()
