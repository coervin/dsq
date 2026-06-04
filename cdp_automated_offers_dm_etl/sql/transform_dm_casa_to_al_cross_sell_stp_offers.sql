WITH eom_df_pm_eq AS (
    SELECT * FROM df_pm_eq
    WHERE reporting_date = (
            SELECT
                MAX(reporting_date)
            FROM df_pm_eq
            WHERE reporting_date BETWEEN DATE_TRUNC('month', ADD_MONTHS(DATE'{actual_date}', -1))
                AND LAST_DAY(ADD_MONTHS(DATE'{actual_date}', -1))
    )
)
, recent_loan_booking AS (
    SELECT
        bbn
    FROM eom_df_pm_eq
    WHERE booked_yesno = 1
      AND product_code IN ('AL', 'CSL', 'SALAD')
      AND booking_date >= add_months(DATE '{actual_date}', -3)
    GROUP BY 1
)
, active_loans AS (
    SELECT
        bbn,
        product_code,
        booking_date,
        booked_yesno,
        booked_amortization_amount
    FROM eom_df_pm_eq
    WHERE active_yesno = 1
    AND product_code IN ('HL','PL','CTS','BEL','BML')
)
, aggregated_loans AS (
    SELECT
        bbn,
        MAX(CASE WHEN product_code = 'HL' THEN booking_date END) AS dt_hl,
        MAX(CASE WHEN product_code = 'PL' THEN booking_date END) AS dt_pl,
        MAX(CASE WHEN product_code IN ('CTS','BEL','BML') THEN booking_date END) AS dt_oth,
        SUM(COALESCE(booked_amortization_amount,0)) FILTER(WHERE booked_yesno = 1) AS amort_all_loans
    FROM active_loans
    GROUP BY bbn
)
, existing_loans AS (
    SELECT
        bbn,
        amort_all_loans,
        CASE
        WHEN dt_hl  IS NOT NULL AND months_between(DATE '{actual_date}', dt_hl) < 3 THEN 0
        WHEN dt_pl  IS NOT NULL AND months_between(DATE '{actual_date}', dt_pl) < 3 THEN 0
        WHEN dt_oth IS NOT NULL AND months_between(DATE '{actual_date}', dt_oth) < 3 THEN 0
        ELSE 1
        END AS loan_criteria_pass
    FROM aggregated_loans
)
, rates (al_ratecode, term, rate_factor) AS (
    VALUES
        ('XSELL_OMA',      60, 0.2985),
        ('XSELL_OMA',      48, 0.2250),
        ('XSELL_OMA',      36, 0.1636),
        ('XSELL_OMA',      24, 0.1068),
        ('XSELL_OMA',      18, 0.0720),
        ('XSELL_OMA',      12, 0.0457),
        ('XSELL_STANDARD', 60, 0.3101),
        ('XSELL_STANDARD', 48, 0.2355),
        ('XSELL_STANDARD', 36, 0.1733),
        ('XSELL_STANDARD', 24, 0.1158),
        ('XSELL_STANDARD', 18, 0.0802),
        ('XSELL_STANDARD', 12, 0.0533)
)
, casa_to_al_stp_not_yet_expired AS (
    SELECT bbn, offer_code FROM dm_casa_to_al_cross_sell_stp_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date < DATE'{actual_date}'
)
, last_max_offer AS (
    SELECT
        COALESCE(MAX(CAST(substr(offer_code, length(offer_code) -6, 7) AS INTEGER)), 0) AS max_row_num
    FROM casa_to_al_stp_not_yet_expired
    WHERE substring(offer_code, LENGTH(offer_code) -11, 2) = DATE_FORMAT(DATE'{actual_date}', 'MM')
        AND substring(offer_code, LENGTH(offer_code) - 9, 2) = DATE_FORMAT(DATE '{actual_date}', 'yy')
)
, base_casa_to_al_stp AS (
    SELECT
        base.*,
        rates.*,
        el.amort_all_loans
    FROM staging_cross_sell_base base
    CROSS JOIN rates
    LEFT JOIN existing_loans el
        ON base.bbn = el.bbn
    LEFT JOIN recent_loan_booking recent
        ON base.bbn = recent.bbn
    WHERE
        base.is_with_al_flag = 0
        AND base.is_with_salad_flag = 0
        AND base.is_with_csl_flag = 0
        AND (
            (
                base.is_payroll_acct_flag = 0 AND base.is_acct_payroll_like_flag = 0 AND base.depo_mob_count >= 3
            )
            OR
            (
                (base.is_payroll_acct_flag = 1 OR base.is_acct_payroll_like_flag = 1) AND base.depo_mob_count >= 3
            )
        )
        AND base.age BETWEEN 21 AND 62
        AND NOT base.adb_3m_avg < 100000
        AND NOT base.deposit_ob < 10000
        AND el.loan_criteria_pass = 1
        AND base.is_with_pl_acct_woff_flag = 0
        AND base.is_non_indiv_flag = 0
        AND base.is_dosri_flag = 0
        AND base.is_enfis_flag = 0
        AND base.is_risk_industry_flag = 0
        AND base.b_score_cc_value >= 581
        AND base.is_with_pl_flag = 0
        AND LOWER(base.client_segment_name) <> 'wealth'
        AND NOT base.is_with_al_application_6m_flag = 1
        AND NOT base.is_with_cc_application_flag = 1
        AND recent.bbn IS NULL
        AND NULLIF(base.full_name, '') IS NOT NULL
)
, vw_casa_to_al_stp AS (
    SELECT DISTINCT
        term,
        bbn,
        expiry_date,
        full_name,
        last_name,
        first_name,
        middle_name,
        al_ratecode,
        CAST (LEAST(
            5000000.0,
            ROUND(
                (
                    (
                        (COALESCE(adb_3m_avg,0) * 0.4 * 0.75 * 0.4) - COALESCE(amort_all_loans,0)
                    ) * term
                ) / (1 + rate_factor) / 1000.0
            ) * 1000.0
        ) AS INTEGER) AS max_loan_amount,
        'STP' AS offer_type,
        'AL' AS product_crm,
        'Auto Loan STP' AS spiel_id,
        CAST(NULL AS STRING) AS campaign_code,
        earliest_acct_open_date,
        age,
        CAST(NULL AS STRING) AS leads_type,
        CAST(NULL AS STRING) AS promo,
        CAST(NULL AS INTEGER) AS pid_count,
        branch_region_name AS branch_region,
        branch_area_code AS branch_area,
        branch_mnemonic AS branch_mnemonic,
        branch_name AS branch_name,
        CAST(NULL AS STRING) AS product_bundle,
        new_segment,
        depo_acct_num AS accountnumber,
        CAST(NULL AS STRING) AS product_name,
        CAST(NULL AS STRING) AS qualified_casa,
        CAST(NULL AS STRING) AS promo_code,
        CAST(NULL AS STRING) AS ob,
        CAST(NULL AS STRING) AS ob_tier,
        total_relationship_balance_value AS trb,
        CASE
            WHEN total_relationship_balance_value <  200000 THEN 'a.<200k'
            WHEN total_relationship_balance_value <= 250000 THEN 'b.200k-250k'
            WHEN total_relationship_balance_value <= 300000 THEN 'c.250k-300k'
            WHEN total_relationship_balance_value <= 350000 THEN 'd.300k-350k'
            WHEN total_relationship_balance_value <= 400000 THEN 'e.350k-400k'
            WHEN total_relationship_balance_value <= 450000 THEN 'f.400k-450k'
            WHEN total_relationship_balance_value <  500000 THEN 'g.450k-500k'
            WHEN total_relationship_balance_value >= 500000 THEN 'h.>=500k'
            ELSE 'UNKNOWN'
        END AS trb_bucket,
        CASE
            WHEN age BETWEEN 18 AND 29 THEN 'a.18-29'
            WHEN age BETWEEN 30 AND 39 THEN 'b.30-39'
            WHEN age BETWEEN 40 AND 49 THEN 'c.40-49'
            WHEN age >= 50 THEN 'd.50 & up'
            ELSE 'UNKNOWN'
        END AS age_tier,
        CAST(NULL AS STRING) AS casa_acct_php,
        CAST(NULL AS STRING) AS casa_acct_usd,
        CAST(NULL AS STRING) AS deal_acct_php,
        CAST(NULL AS STRING) AS deal_acct_usd,
        CAST(NULL AS INTEGER) AS gold_propensity,
        CASE WHEN age >= 70 THEN 1 ELSE 0 END AS age_70,
        CAST(NULL AS STRING) AS agent_name,
        CAST(NULL AS STRING) AS purpose,
        CAST(NULL AS STRING) AS agent_remarks,
        CAST(NULL AS STRING) AS spiel,
        CAST(NULL AS STRING) AS other_card,
        CAST(NULL AS STRING) AS special_otherremarks,
        'Asia/Manila' AS timezone,
        present_address AS email,
        gender_code AS gender,
        civil_status_code AS civil_status,
        birthdate AS date_of_birth,
        present_zip_code AS zipcode,
        employment_type_code AS employment_type,
        employer_name,
        occupation_details_text AS occupation,
        tin_comptin AS tin,
        birth_place_name AS place_of_birth,
        present_address,
        permanent_address,
        primary_id_type AS id_type,
        primary_id_number AS id_number,
        mothers_maiden_name AS mother_maiden_name,
        employment_type_code AS employment_status,
        office_address AS employer_address,
        nature_of_work_code AS nature_of_business,
        priority_mobile_no1 AS mobile_no,
        priority_mobile_no2 AS mobile_no2,
        priority_home_landline1 AS home_landline1,
        office_landline1,
        CAST(NULL AS STRING) AS length_of_stay,
        CAST(NULL AS STRING) AS no_of_years_employed,
        CAST(NULL AS STRING) AS year_started_working,
        CAST(NULL AS STRING) AS current_industry,
        CAST(NULL AS STRING) AS updated_industry,
        CAST(NULL AS STRING) AS updated_position,
        actual_date
    FROM base_casa_to_al_stp
)

SELECT
    CONCAT(
        'AL',
        'STP',
        DATE_FORMAT(DATE'{actual_date}', 'MM'),
        DATE_FORMAT(DATE'{actual_date}', 'yy'),
        'B',
        lpad(CAST(ROW_NUMBER() OVER (ORDER BY vw_casa_to_al_stp.bbn) + last_max_offer.max_row_num AS STRING), 7, '0')
    ) AS offer_code,
    vw_casa_to_al_stp.*
FROM vw_casa_to_al_stp
CROSS JOIN last_max_offer
LEFT JOIN casa_to_al_stp_not_yet_expired
    ON casa_to_al_stp_not_yet_expired.bbn = vw_casa_to_al_stp.bbn
WHERE max_loan_amount BETWEEN 400000 AND 5000000
    AND casa_to_al_stp_not_yet_expired.bbn IS NULL
