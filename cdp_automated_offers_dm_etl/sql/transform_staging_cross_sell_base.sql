WITH date_bound AS (
    -- - Using a CTE (date_bounds) avoids recalculating date_trunc multiple times.
    SELECT
        add_months(trunc(current_date, 'MM'), -3)  AS start_3_months,
        add_months(trunc(current_date, 'MM'), -6)  AS start_6_months,
        add_months(trunc(current_date, 'MM'), -12) AS start_12_months,
        CAST(add_months(trunc(current_date, 'MM'), -12) AS timestamp) AS start_12_months_ts,
        trunc(current_date, 'MM') AS end_date
)
, dedup_dca_dnc AS (
    SELECT
        bbnumber,
        accountnumber,
        CASE WHEN SUM (df_customer_account.dnc_tag) > 0 THEN 1 ELSE 0 END AS dnc_tag
    FROM df_customer_account
        GROUP BY 1,2
)
, dedup_dca_cc_tag AS (
    SELECT * FROM (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY bbnumber, accountnumber ORDER BY credit_card_tag DESC) AS row_num,
            bbnumber,
            account_basic_number,
            accountnumber,
            internal_accountnumber,
            deposit_account_number,
            credit_card_card_no,
            credit_card_tag,
            deposit_months_on_books,
            credit_card_months_on_books,
            low_cost_deposit_mtd_adb,
            high_cost_deposit_mtd_adb,
            low_cost_deposit_ob,
            high_cost_deposit_ob,
            reporting_date
        FROM df_customer_account
        WHERE NULLIF(bbnumber, '') IS NOT NULL
            AND NULLIF(account_basic_number, '') IS NOT NULL
            AND NULLIF(accountnumber, '') IS NOT NULL
    ) WHERE row_num = 1
)
, dedup_df_customer_account AS (
    SELECT
        ddca.bbnumber,
        ddca.account_basic_number,
        ddca.accountnumber,
        ddca.internal_accountnumber,
        ddca.deposit_account_number,
        ddca.credit_card_card_no,
        ddca.credit_card_tag,
        ddca.deposit_months_on_books,
        ddca.credit_card_months_on_books,
        ddca.low_cost_deposit_mtd_adb,
        ddca.high_cost_deposit_mtd_adb,
        ddca.low_cost_deposit_ob,
        ddca.high_cost_deposit_ob,
        ddca.reporting_date,
        ddnc.dnc_tag
    FROM dedup_dca_cc_tag ddca
    LEFT JOIN dedup_dca_dnc ddnc
        ON ddca.bbnumber = ddnc.bbnumber
        AND ddca.accountnumber = ddnc.accountnumber
)
, eom_df_pm_eq AS (
    SELECT * FROM df_pm_eq
    WHERE reporting_date = (SELECT
    MAX(reporting_date)
    FROM df_pm_eq
    WHERE reporting_date BETWEEN DATE_TRUNC('month', ADD_MONTHS(DATE'{actual_date}', -1))
        AND LAST_DAY(ADD_MONTHS(DATE'{actual_date}', -1))
    )
)
, tagging_df_pm_eq AS (
    SELECT
        dpe.bbn,
        dpe.app_number,
        CASE WHEN SUM (
            CASE
                WHEN dpe.product_code = 'AL' THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_al_flag,
        CASE WHEN SUM (
            CASE
                WHEN dpe.product_code = 'HL' THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_hl_flag,
        CASE WHEN SUM (
            CASE
                WHEN dpe.product_code = 'CEL' THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_csl_flag,
        CASE WHEN SUM (
            CASE
                WHEN dpe.product_code = 'CTS' AND dpe.mutated_pn_number LIKE '%@BR5LBGSOP%' THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_bml_flag
    FROM eom_df_pm_eq dpe
    GROUP BY 1, 2
)
, tagging_spanga AS (
    SELECT
        spanga.app_number,
        CASE WHEN SUM (
            CASE
                WHEN spanga.product_cd = 'AL'
                    AND spanga.app_create_date BETWEEN (SELECT start_6_months FROM date_bound) AND (SELECT end_date FROM date_bound)
                THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_al_application_6m_flag,
        CASE WHEN SUM (
            CASE
                WHEN spanga.product_cd = 'AL'
                    AND spanga.app_create_date BETWEEN (SELECT start_12_months FROM date_bound) AND (SELECT end_date FROM date_bound)
                THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_al_application_12m_flag
    FROM staging_pm_al_non_grid_application_data spanga
    GROUP BY 1
)
, tagging_pa AS (
    SELECT
        pa.app_number,
        CASE WHEN SUM (
            CASE
                WHEN pa.product_code = 'HL'
                    AND pa.app_create_date BETWEEN (SELECT start_12_months FROM date_bound)  AND (SELECT end_date FROM date_bound)
                THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_hl_application_flag
    FROM pm_application pa
    GROUP BY 1
)
, tagging_dpe_pm AS (
    SELECT
        dpe.bbn,
        CASE WHEN SUM (is_with_al_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_al_flag,
        CASE WHEN SUM (is_with_hl_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_hl_flag,
        CASE WHEN SUM (is_with_csl_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_csl_flag,
        CASE WHEN SUM (is_with_bml_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_bml_flag,
        CASE WHEN SUM (is_with_al_application_6m_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_al_application_6m_flag,
        CASE WHEN SUM (is_with_al_application_12m_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_al_application_12m_flag,
        CASE WHEN SUM (is_with_hl_application_flag) > 0 THEN 1 ELSE 0 END AS 	is_with_hl_application_flag
    FROM tagging_df_pm_eq  dpe
    LEFT JOIN tagging_spanga spanga
        ON dpe.app_number = spanga.app_number
    LEFT JOIN tagging_pa pa
        ON dpe.app_number = pa.app_number
    GROUP BY 1
)
, dedup_pl_active_accounts AS (
    SELECT bbn FROM pl_active_accounts GROUP BY 1
)
, dedup_pl_active_accounts_woff AS (
    SELECT bbn FROM pl_active_accounts GROUP BY 1
)
, dedup_salad_accounts AS (
    SELECT bbn FROM salad_accounts GROUP BY 1
)
, tagging_credit_card_processing_tu AS (
    SELECT
        bbn,
        CASE WHEN SUM (
            CASE
                WHEN ccpt.app_create_date BETWEEN (SELECT start_6_months FROM date_bound) AND (SELECT end_date FROM date_bound)
                THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS 	is_with_cc_application_flag
    FROM credit_card_processing_tu ccpt
    GROUP BY 1
)
, df_adb_3m AS (
    SELECT
        account_number,
	    AVG(outstanding_balance_amt) AS adb_3m_avg
	FROM df_adb
	WHERE actual_date BETWEEN (SELECT start_3_months FROM date_bound) AND (SELECT end_date FROM date_bound)
	GROUP BY 1
)
, dedup_winnow_b_score_cc AS (
    SELECT custserno,b_score_cust FROM winnow_b_score_cc GROUP BY 1,2
)
, dedup_df_cards_accounts AS (
    SELECT
        card_number,
        customer_serno,
        card_generalstatus_code,
        card_generalstatus_updatedate,
        CASE WHEN SUM (
            CASE
                WHEN card_generalstatus_code = 'CANC'
                    AND card_generalstatus_updatedate >= (SELECT start_12_months_ts FROM date_bound) THEN 1 ELSE 0
            END
        ) > 0 THEN 1 ELSE 0 END AS is_card_canc_12m_flag
    FROM df_cards_accounts
    WHERE card_number IS NOT NULL
    GROUP BY 1,2,3,4
)
, dca_wbsc AS (
    SELECT
        card_number,
        MAX(b_score_cust) AS b_score_cust,
        CASE WHEN SUM (dedup_df_cards_accounts.is_card_canc_12m_flag) > 0 THEN 1 ELSE 0 END AS is_card_canc_12m_flag
    FROM  dedup_df_cards_accounts
    LEFT JOIN dedup_winnow_b_score_cc
        ON CAST(dedup_df_cards_accounts.customer_serno AS STRING) = dedup_winnow_b_score_cc.custserno
    GROUP BY 1
)
, contact_present_primary AS (
    SELECT * FROM dim_cust_contact
    WHERE TRIM(contact_type_desc) IN ('Present','Permanent', 'Office')
)
, dim_cust_contact_pii_email   AS (
   SELECT * FROM dim_cust_contact_email dcc
   LEFT JOIN dim_cust_contact_pii dccp
    ON dcc.cust_contact_key = dccp.cust_contact_pii_key
)
, contact_summary AS (
    SELECT
        cpp.bbn,
        MAX(
            CASE
                WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Email Address'
                    THEN email_pii.contact_details_text
            END
        ) AS present_email,
        COALESCE(
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Mobile Number 1'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Mobile Number 1' THEN mobile_pii.contact_details_text END)
        ) AS priority_mobile_no1,
        COALESCE(
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Mobile Number 2'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Mobile Number 2' THEN mobile_pii.contact_details_text END)
        ) AS priority_mobile_no2,
        COALESCE(
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Mobile Number 1'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Mobile Number 2'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Mobile Number 3'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Mobile Number 1'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Mobile Number 2'
                THEN mobile_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Mobile Number 3'
                THEN mobile_pii.contact_details_text END)
        ) AS priority_mobile_no,
        COALESCE(
             MAX(CASE WHEN cpp.contact_type_desc = 'Present' AND cpp.contact_category_desc = 'Landline Number 1'
                THEN landline_pii.contact_details_text END),
             MAX(CASE WHEN cpp.contact_type_desc = 'Permanent' AND cpp.contact_category_desc = 'Landline Number 1' THEN landline_pii.contact_details_text END)
        ) AS priority_home_landline1,
        MAX(
            CASE
                WHEN cpp.contact_type_desc = 'Office' AND cpp.contact_category_desc = 'Landline Number 1'
                    THEN landline_pii.contact_details_text
            END
        ) AS office_landline1
    FROM contact_present_primary cpp
    LEFT JOIN dim_cust_contact_pii_email email_pii
        ON cpp.cust_contact_key = email_pii.cust_contact_pii_key
    LEFT JOIN dim_cust_contact_pii_mobile mobile_pii
        ON cpp.cust_contact_key = mobile_pii.cust_contact_pii_key
    LEFT JOIN dim_cust_contact_pii_landline landline_pii
        ON cpp.cust_contact_key = landline_pii.cust_contact_pii_key
    GROUP BY 1
)
, branch_listing_summary AS (
    SELECT
        dcc.bbn,
        dcc.acct_origin_branch_code AS branch_code,
        dcc.acct_origin_branch_name AS branch_name,
        bl.area AS branch_area_code,
        bl.region AS  branch_region_name,
        bl.branchmnemonic AS branch_mnemonic,
        dcc.client_segment_name,
        dcc.cust_category_desc
    FROM dim_cust_class dcc
    LEFT JOIN branch_listing bl
        ON TRIM(dcc.acct_origin_branch_code) = TRIM(bl.branch_code)
)
, address_summary AS (
    SELECT
        dca.bbn,
        MAX(
            CASE
                WHEN dca.address_type_desc = 'Present' THEN dca.zip_code
            END
        ) AS present_zip_code,
        MAX(
            CASE
                WHEN dca.address_type_desc = 'Present'
                THEN CONCAT_WS(' ',
                    dcap.address_line_1_text,
                    dcap.address_line_2_text,
                    dcap.address_line_3_text
                )
            END
        ) AS present_address,
        MAX(
            CASE
                WHEN dca.address_type_desc = 'Permanent'
                THEN CONCAT_WS(' ',
                    dcap.address_line_1_text,
                    dcap.address_line_2_text,
                    dcap.address_line_3_text
                )
            END
        ) AS permanent_address,
        MAX(
            CASE
                WHEN dca.address_type_desc = 'Office'
                THEN CONCAT_WS(' ',
                    dcap.address_line_1_text,
                    dcap.address_line_2_text,
                    dcap.address_line_3_text
                )
            END
        ) AS office_address
    FROM dim_cust_address dca
    LEFT JOIN dim_cust_address_pii dcap
        ON dca.cust_address_key = dcap.cust_address_key
    GROUP BY 1
)
, id_proof_summary AS (
    SELECT
        dcip.bbn,
        MAX(
            CASE
                WHEN dcip.id_category_desc = 'Primary' THEN dcip.id_type_code
            END
        ) AS primary_id_type,
        MAX(
            CASE
                WHEN dcip.id_category_desc = 'Primary' THEN dcipp.id_num_value
            END
        ) AS primary_id_number
    FROM dim_cust_id_proof dcip
    LEFT JOIN dim_cust_id_proof_pii dcipp
        ON dcip.cust_id_proof_key = dcipp.cust_id_proof_key
    GROUP BY 1
)
SELECT
    dca.bbnumber AS bbn,
    dca.account_basic_number AS acct_basic_num,
    dca.accountnumber AS external_acct_num,
    dca.internal_accountnumber AS internal_acct_num,
    dca.deposit_account_number AS depo_acct_num,
    dca.credit_card_card_no AS credit_card_physical_num,
    dcip.full_name,
    dcip.first_name,
    dcip.middle_name,
    dcip.last_name,
    CAST(FLOOR(months_between(DATE'{actual_date}', dcip.birthdate) / 12) AS INT) AS age,
    bl.client_segment_name,
    CAST(COALESCE(dca.credit_card_tag, 0) AS INT) AS is_with_credit_card_flag,
    CAST(COALESCE(tdpe.is_with_al_flag, 0) AS INT) AS is_with_al_flag,
    CAST(COALESCE(tdpe.is_with_hl_flag, 0) AS INT) AS is_with_hl_flag,
    CAST(COALESCE(tdpe.is_with_csl_flag, 0) AS INT) AS is_with_csl_flag,
    CAST(COALESCE(tdpe.is_with_bml_flag, 0) AS INT) AS is_with_bml_flag,
    CASE
        WHEN paa.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_with_pl_flag,
    CASE
        WHEN paaw.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_with_pl_acct_woff_flag,
    CASE
        WHEN sa.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_with_salad_flag,
    CAST(COALESCE(tccp.is_with_cc_application_flag, 0) AS INT) AS is_with_cc_application_flag,
    CAST(COALESCE(tdpe.is_with_al_application_6m_flag, 0) AS INT) AS is_with_al_application_6m_flag,
    CAST(COALESCE(tdpe.is_with_al_application_12m_flag, 0) AS INT) AS is_with_al_application_12m_flag,
    CAST(COALESCE(tdpe.is_with_hl_application_flag, 0) AS INT) AS is_with_hl_application_flag,
    CAST(COALESCE(dca.deposit_months_on_books, 0) AS INT) AS depo_mob_count,
    CAST(COALESCE(dca.credit_card_months_on_books, 0) AS INT) AS cc_mob_count,
    CAST(COALESCE(low_cost_deposit_mtd_adb,high_cost_deposit_mtd_adb) AS DECIMAL(38,6)) AS deposit_mtd_adb,
    CAST(COALESCE(low_cost_deposit_ob,high_cost_deposit_ob) AS DECIMAL(38,6)) AS deposit_ob,
    CAST(COALESCE(da3.adb_3m_avg, 0) AS DECIMAL(18,2)) AS adb_3m_avg,
    CAST(COALESCE(dac.payroll_salary_cr_avg_6m, 0) AS DECIMAL(18,2))  AS payr_sal_6m_avg,
    CAST(COALESCE(dac.trb_amt_m0, 0) AS DECIMAL(18,2))  AS total_relationship_balance_value,
    CAST(COALESCE(dca_wbsc.b_score_cust, 0) AS DOUBLE) AS b_score_cc_value,
    CASE
        WHEN ptbcasatocc.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_with_cc_propensity_flag,
    CAST(COALESCE(dca_wbsc.is_card_canc_12m_flag, 0) AS INT) AS is_card_canc_12m_flag,
    CASE
        WHEN dd.bbn_winner IS NOT NULL THEN 1 ELSE 0
    END AS is_dosri_flag,
    CASE
        WHEN eb.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_enfis_flag,
    CAST(COALESCE(dca.dnc_tag, 0) AS INT) AS is_dnc_flag,
    CASE
        WHEN TRIM(UPPER( bl.cust_category_desc))IN ('PARTNERSHIP', 'CORPORATION') THEN 1 ELSE 0
    END AS is_non_indiv_flag,
    CASE
        WHEN dsv2.new_segment IS NULL THEN 1 ELSE 0
    END AS is_no_segment_flag,
    CASE
        WHEN scpf.scai97 = 'Y' THEN 1 ELSE 0
    END AS is_payroll_acct_flag,
    CASE
        WHEN scpf.scaig3 = 'Y' THEN 1 ELSE 0
    END AS is_acct_payroll_like_flag,
    CASE
        WHEN rri.bbn IS NOT NULL THEN 1 ELSE 0
    END AS is_risk_industry_flag,
    CAST(CASE
        WHEN iev2.label = '(1) Below 305k'
            THEN 305000.0/12.0
        WHEN iev2.label = '(2) 305k to 470k'
            THEN ((305000.0 + 470000.0)/12.0) / 2.0
        WHEN iev2.label = '(3) 470k to 835k'
            THEN ((470000.0 + 835000.0)/12.0) / 2.0
        WHEN iev2.label = '(4) 835k to 1.5M'
            THEN ((835000.0 + 1500000.0)/12.0) / 2.0
        WHEN iev2.label = '(5) 1.5M to 5M'
            THEN ((1500000.0 + 5000000.0)/12.0) / 2.0
        WHEN iev2.label = '(6) Above 5M'
            THEN ((5000000.0 + 5000000.0)/12.0) / 2.0
        ELSE NULL
    END AS DECIMAL(18,2)) AS gmi_value,
    CAST(floor(months_between(DATE'{actual_date}', indiv.cust_create_date_ts))AS INT) AS cust_mob_count,
    CAST(indiv.cust_create_date_ts AS DATE) AS earliest_acct_open_date,
    indiv.gender_code,
    indiv.civil_status_code,
    indiv.employment_type_code,
    CAST(dcip.birthdate AS DATE) AS birthdate,
    dcip.mothers_maiden_name,
    dcip.birth_place_name,
    contact.present_email,
    contact.priority_mobile_no1,
    contact.priority_mobile_no2,
    contact.priority_mobile_no,
    contact.priority_home_landline1,
    contact.office_landline1,
    address.present_zip_code,
    address.present_address,
    address.permanent_address,
    address.office_address,
    id_proof.primary_id_type,
    id_proof.primary_id_number,
    cust_info.tin_comptin,
    employ.employer_name,
    employ.occupation_details_text,
    employ.nature_of_work_code,
    CASE
        WHEN contact.priority_mobile_no IS NULL THEN 0
        WHEN contact.priority_mobile_no = '' THEN 0
        ELSE 1
    END AS is_mobile_valid_flag,
    CASE
        WHEN contact.present_email IS NULL THEN 0
        WHEN contact.present_email = '' THEN 0
        ELSE 1
    END AS is_email_valid_flag,
    bl.branch_code,
    bl.branch_name,
    bl.branch_area_code,
    bl.branch_region_name,
    bl.branch_mnemonic,
    dsv2.new_segment,
    date_add(DATE'{actual_date}' , (7*6)) AS expiry_date,
DATE'{actual_date}' AS actual_date
FROM  dedup_df_customer_account dca
LEFT JOIN dim_cust_indiv_pii dcip
    ON dca.bbnumber = dcip.bbn
LEFT JOIN ptbcasatocc
    ON dca.bbnumber = ptbcasatocc.bbn
LEFT JOIN dosri_distinct dd
    ON dca.bbnumber = dd.bbn_winner
LEFT JOIN branch_listing_summary bl
    ON dca.bbnumber = bl.bbn
LEFT JOIN depo_segmentation_v2 dsv2
    ON dca.bbnumber = dsv2.bbnumber
LEFT JOIN enfis_bbn eb
    ON dca.bbnumber = eb.bbn
LEFT JOIN ref_risk_industry rri
    ON dca.bbnumber = rri.bbn
LEFT JOIN tagging_dpe_pm tdpe
    ON dca.bbnumber = tdpe.bbn
LEFT JOIN scpf
    ON dca.internal_accountnumber = CONCAT(TRIM(scpf.scab),TRIM(scpf.scan),TRIM(scpf.scas))
LEFT JOIN dedup_pl_active_accounts paa
    ON dca.bbnumber = paa.bbn
LEFT JOIN dedup_pl_active_accounts_woff paaw
    ON dca.bbnumber = paaw.bbn
LEFT JOIN dca_wbsc
    ON dca.credit_card_card_no = dca_wbsc.card_number
LEFT JOIN dedup_salad_accounts sa
    ON dca.bbnumber = sa.bbn
LEFT JOIN df_abt_customer dac
    ON dca.bbnumber = dac.bbnumber
LEFT JOIN df_adb_3m da3
    ON dca.accountnumber = da3.account_number
LEFT JOIN tagging_credit_card_processing_tu tccp
    ON dca.bbnumber = tccp.bbn
LEFT JOIN incomeestimatorv2 iev2
    on dca.bbnumber = iev2.bbn
LEFT JOIN dim_cust_indiv indiv
    ON dca.bbnumber = indiv.bbn
LEFT JOIN contact_summary contact
    ON dca.bbnumber = contact.bbn
LEFT JOIN address_summary address
    ON dca.bbnumber = address.bbn
LEFT JOIN id_proof_summary id_proof
    ON dca.bbnumber = id_proof.bbn
LEFT JOIN cust_info
    ON dca.bbnumber = cust_info.bbnumber
LEFT JOIN dim_cust_indiv_employment employ
    ON dca.bbnumber = employ.bbn
