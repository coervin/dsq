from xflow.task import TaskResultType
from cdp_automated_offers_dm_etl.config.config import Config
from cdp_automated_offers_dm_etl.operations.extractors.base_extract import base_extract
from cdp_automated_offers_dm_etl.utils.sql_utils import clean_mobile_num, clean_landline_num

extract_dwh_customer = base_extract.full().basic().with_db_name(Config().db_name_dwh_customer).reuse()

extract_dim_cust_indiv = (
    extract_dwh_customer.with_table("dim_cust_indiv")
    .with_result_name("dim_cust_indiv")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)


extract_dim_cust_indiv_pii = (
    extract_dwh_customer.with_table("dim_cust_indiv_pii")
    .with_result_name("dim_cust_indiv_pii")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_class = (
    extract_dwh_customer.with_table("dim_cust_class")
    .with_result_name("dim_cust_class")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_contact = (
    extract_dwh_customer.with_table("dim_cust_contact")
    .with_result_name("dim_cust_contact")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_contact_mobile = (
    extract_dwh_customer.with_table("dim_cust_contact")
    .with_result_name("dim_cust_contact_mobile")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
    .with_filter("contact_category_desc LIKE 'Mobile Number%'")
)

extract_dim_cust_contact_email = (
    extract_dwh_customer.with_table("dim_cust_contact")
    .with_result_name("dim_cust_contact_email")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
    .with_filter("contact_category_desc LIKE 'Email%'")
)

extract_dim_cust_contact_pii = (
    extract_dwh_customer.with_table("dim_cust_contact_pii")
    .with_result_name("dim_cust_contact_pii")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_contact_pii_mobile = (
    extract_dwh_customer.with_table("dim_cust_contact_pii")
    .with_result_name("dim_cust_contact_pii_mobile")
    .add_post_processing(clean_mobile_num)
    .add_kwargs(mobile_column="contact_details_text")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_contact_pii_landline = (
    extract_dwh_customer.with_table("dim_cust_contact_pii")
    .with_result_name("dim_cust_contact_pii_landline")
    .add_post_processing(clean_landline_num)
    .add_kwargs(landline_column="contact_details_text")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_address = (
    extract_dwh_customer.with_table("dim_cust_address")
    .with_result_name("dim_cust_address")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_address_pii = (
    extract_dwh_customer.with_table("dim_cust_address_pii")
    .with_result_name("dim_cust_address_pii")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_id_proof = (
    extract_dwh_customer.with_table("dim_cust_id_proof")
    .with_result_name("dim_cust_id_proof")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_id_proof_pii = (
    extract_dwh_customer.with_table("dim_cust_id_proof_pii")
    .with_result_name("dim_cust_id_proof_pii")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_indiv_employment = (
    extract_dwh_customer.with_table("dim_cust_indiv_employment")
    .with_result_name("dim_cust_indiv_employment")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

# to be deleted
extract_dim_cust_contact_pii_email = (
    extract_dwh_customer.with_table("dim_cust_contact_pii")
    .with_result_name("dim_cust_contact_pii_email")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
)

extract_dim_cust_contact_landline = (
    extract_dwh_customer.with_table("dim_cust_contact")
    .with_result_name("dim_cust_contact_landline")
    .with_result_type(TaskResultType.DATAFRAME_TO_TEMPVIEW)
    .with_filter("current_row_flag = 'Y' AND row_eff_end_date IS NOT NULL")
    .with_filter("contact_category_desc LIKE 'Landline%'")
)
