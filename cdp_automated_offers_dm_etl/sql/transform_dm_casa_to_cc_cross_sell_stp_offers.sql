WITH active_loans AS (
    SELECT
        bbn,
        product_code,
        booking_date,
        CAST(days_past_due AS INTEGER) AS dpd
    FROM df_pm_eq
    WHERE active_yesno = 1
        AND product_code IN ('AL','HL','PL','CSL','CTS','BEL','BML','SD')
        AND reporting_date = (
            SELECT
                MAX(reporting_date)
            FROM df_pm_eq
            WHERE reporting_date BETWEEN DATE_TRUNC('month', ADD_MONTHS(DATE'{actual_date}', -1))
                AND LAST_DAY(ADD_MONTHS(DATE'{actual_date}', -1))
            )
)
, latest_loan_dates AS (
    SELECT
        bbn,
        MAX(CASE WHEN product_code IN ('CSL','CTS','BEL','BML','SD') THEN booking_date END) AS dt_grp_6mo,
        MAX(CASE WHEN product_code IN ('AL','PL') THEN booking_date END) AS dt_grp_4mo
    FROM active_loans
    GROUP BY bbn
)
, loan_criteria_pass_per_cust AS (
    SELECT bbn
    FROM
        (SELECT
            bbn,
            CASE
                WHEN dt_grp_6mo IS NOT NULL
                AND MONTHS_BETWEEN(DATE'{actual_date}', dt_grp_6mo) < 6 THEN 0
                WHEN dt_grp_4mo IS NOT NULL
                AND MONTHS_BETWEEN(DATE'{actual_date}', dt_grp_4mo) < 4 THEN 0
                ELSE 1
            END AS loan_criteria_pass
        FROM latest_loan_dates)
    WHERE loan_criteria_pass = 1
)
, non_current_flag AS (
    SELECT bbn
    FROM (
        SELECT
            bbn,
        CASE
            WHEN SUM(CASE WHEN dpd > 0 THEN 1 ELSE 0 END) > 0 THEN 1
            ELSE 0
        END AS non_current_flag
        FROM active_loans
        GROUP BY bbn
    )
    WHERE non_current_flag = 0
)
, src AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bbn ORDER BY bbn) AS rn,
        CASE
            WHEN is_payroll_acct_flag = 1 THEN CAST(payr_sal_6m_avg AS double)
            WHEN is_payroll_acct_flag = 0 THEN 0.4 * CAST(adb_3m_avg AS double)
        END AS raw_cl
    FROM staging_cross_sell_base
    WHERE
        -- ==========================================
        -- INJECTED DYNAMIC JSON RULES
        -- ==========================================
        {dynamic_json_rules}
        -- ==========================================

        -- Join structural filters retained
        AND NOT EXISTS (SELECT 1 FROM loan_criteria_pass_per_cust lcppc WHERE staging_cross_sell_base.bbn = lcppc.bbn)
        AND NOT EXISTS (SELECT 1 FROM non_current_flag ncf WHERE staging_cross_sell_base.bbn = ncf.bbn)
)
, calculated_card_type AS (
    SELECT
        bbn,
        CAST(credit_limit AS INTEGER) AS credit_limit,
        CASE
            WHEN credit_limit < 35000 THEN NULL
            WHEN credit_limit < 150000 THEN 'MasterCard Gold'
            WHEN credit_limit < 1000000 THEN 'MasterCard Platinum'
            ELSE 'MasterCard World'
        END AS card_type
    FROM (
        SELECT
            bbn,
            CASE
                WHEN raw_cl IS NULL OR raw_cl < 35000 THEN NULL
                ELSE LEAST(1000000, ROUND(raw_cl / 1000.0) * 1000.0)
            END AS credit_limit
        FROM src
        WHERE rn = 1
    ) AS credit_calc
)
, casa_to_cc_stp_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
    FROM dm_casa_to_cc_cross_sell_stp_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date < DATE'{actual_date}'
)
, last_month_max_seq AS (
    SELECT
        COALESCE(MAX(CAST(substr(offer_code, length(offer_code) -6, 7) AS INTEGER)), 0) AS last_seq
    FROM casa_to_cc_stp_not_yet_expired
    WHERE CAST(SUBSTR(offer_code, 6, 2) AS INT) = month(DATE '{actual_date}')
        AND CAST(SUBSTR(offer_code, 8, 2) AS INT) = year(DATE '{actual_date}') % 100
)
, bbn_need_new AS (
    SELECT
        bbn,
        ROW_NUMBER() OVER (ORDER BY bbn) AS new_rank
    FROM calculated_card_type c
    WHERE c.card_type IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM casa_to_cc_stp_not_yet_expired a WHERE a.bbn = c.bbn
        )
)
, final_offer AS (
    SELECT
        cct.bbn,
        cct.card_type AS card_type,
        cct.credit_limit AS credit_limit,
        CONCAT(
            'CC',
            'STP',
            date_format(DATE '{actual_date}', 'MM'),
            date_format(DATE '{actual_date}', 'yy'),
            'B',
            LPAD(
                CAST(lms.last_seq + bn.new_rank AS STRING),
                7,
                '0'
            )
        ) AS offer_code
    FROM calculated_card_type cct
    INNER JOIN bbn_need_new bn
        ON cct.bbn = bn.bbn
    CROSS JOIN last_month_max_seq lms
)
SELECT DISTINCT
    fo.offer_code AS offer_code,
    fo.card_type AS card_type,
    src.bbn AS bbn,
    src.full_name AS full_name,
    src.last_name AS last_name,
    src.first_name AS first_name,
    src.middle_name AS middle_name,
    fo.credit_limit,
    'STP' AS offer_type,
    'CC' AS product_crm,
    CASE
        WHEN fo.card_type = 'MasterCard Gold' THEN 'MC Gold'
        WHEN fo.card_type ='MasterCard Platinum' THEN 'MC Platinum'
        WHEN fo.card_type ='MasterCard World' THEN 'MC World'
    END AS spiel_id,
    CAST(NULL AS STRING) AS campaign_code,
    src.earliest_acct_open_date,
    src.age AS age,
    CAST(NULL AS STRING) AS leads_type,
    src.expiry_date,
    CAST(NULL AS STRING) AS promo,
    (
        CASE WHEN src.birthdate IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN src.mothers_maiden_name IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN src.present_address IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN src.office_address IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN src.present_email IS NOT NULL THEN 1 ELSE 0 END
    ) AS pid_count,
    src.branch_region_name AS branch_region,
    src.branch_area_code AS branch_area,
    src.branch_mnemonic,
    src.branch_name,
    CAST(NULL AS STRING) AS product_bundle,
    src.new_segment,
    src.depo_acct_num AS accountnumber,
    CAST(NULL AS STRING) AS product_name,
    CAST(NULL AS STRING) AS qualified_casa,
    CAST(NULL AS STRING) AS promo_code,
    CAST(NULL AS STRING) AS ob,
    CAST(NULL AS STRING) AS ob_tier,
    CAST(src.total_relationship_balance_value AS STRING) AS trb,
    CASE
        WHEN src.total_relationship_balance_value < 200000 THEN 'a.<200k'
        WHEN src.total_relationship_balance_value >= 200000 AND src.total_relationship_balance_value <= 250000
        THEN 'b.200k-250k'
        WHEN src.total_relationship_balance_value > 250000 AND src.total_relationship_balance_value <= 300000
        THEN 'c.250k-300k'
        WHEN src.total_relationship_balance_value > 300000 AND src.total_relationship_balance_value <= 350000
        THEN 'd.300k-350k'
        WHEN src.total_relationship_balance_value > 350000 AND src.total_relationship_balance_value <= 400000
        THEN 'e.350k-400k'
        WHEN src.total_relationship_balance_value > 400000 AND src.total_relationship_balance_value <= 450000
        THEN 'f.400k-450k'
        WHEN src.total_relationship_balance_value > 450000 AND src.total_relationship_balance_value < 500000
        THEN 'g.450k-500k'
        WHEN src.total_relationship_balance_value >= 500000 THEN 'h.>=500k'
        ELSE 'UNKNOWN'
    END AS trb_bucket,
    CASE
        WHEN src.age BETWEEN 18 AND 29 THEN 'a.18-29'
        WHEN src.age BETWEEN 30 AND 39 THEN 'b.30-39'
        WHEN src.age BETWEEN 40 AND 49 THEN 'c.40-49'
        WHEN src.age >= 50 THEN 'd.50 & up'
        ELSE 'UNKNOWN'
    END AS age_tier,
    CAST(NULL AS STRING) AS casa_acct_php,
    CAST(NULL AS STRING) AS casa_acct_usd,
    CAST(NULL AS STRING) AS deal_acct_php,
    CAST(NULL AS STRING) AS deal_acct_usd,
    CAST(NULL AS DECIMAL(11,10)) AS gold_propensity,
    CASE
        WHEN src.age >= 70 THEN 1
        ELSE 0
    END AS age_70,
    CAST(NULL AS STRING) AS agent_name,
    CAST(NULL AS STRING) AS purpose,
    CAST(NULL AS STRING) AS agent_remarks,
    CAST(NULL AS STRING) AS spiel,
    CAST(NULL AS STRING) AS other_card,
    CAST(NULL AS STRING) AS special_otherremarks,
    'Asia/Manila' AS timezone,
    src.present_email AS email,
    CASE
        WHEN src.gender_code = 'M' THEN 'Male'
        WHEN src.gender_code = 'F' THEN 'Female'
    END AS gender,
    CASE
        WHEN src.civil_status_code  = '0' THEN 'Single'
        WHEN src.civil_status_code  = '1' THEN 'Married'
        WHEN src.civil_status_code  = '2' THEN 'Separated'
        WHEN src.civil_status_code  = '3' THEN 'Widowed'
    END AS civil_status,
    src.birthdate AS date_of_birth,
    src.present_zip_code AS zipcode,
    CASE
        WHEN src.employment_type_code= '0' THEN 'Employed'
        WHEN src.employment_type_code = '1' THEN 'Self-employed'
        WHEN src.employment_type_code = '2' THEN 'Unemployed'
    END AS employment_type,
    src.employer_name AS employer_name,
    src.occupation_details_text AS occupation,
    src.tin_comptin AS tin,
    src.birth_place_name AS place_of_birth,
    src.present_address,
    src.permanent_address,
    src.primary_id_type AS id_type,
    src.primary_id_number AS id_number,
    src.mothers_maiden_name AS mother_maiden_name,
    CASE
        WHEN src.employment_type_code = '0' THEN 'Employed'
        WHEN src.employment_type_code = '1' THEN 'Self-employed'
        WHEN src.employment_type_code = '2' THEN 'Unemployed'
    END AS employment_status,
    src.office_address AS employer_address,
    src.nature_of_work_code AS nature_of_business,
    src.priority_mobile_no1 AS mobile_no,
    src.priority_mobile_no1 AS mobile_no2,
    src.priority_home_landline1 AS home_landline1,
    src.office_landline1,
    CAST (NULL AS STRING) AS length_of_stay,
    CAST (NULL AS STRING) AS no_of_years_employed,
    CAST (NULL AS STRING) AS year_started_working,
    CAST (NULL AS STRING) AS current_industry,
    CAST (NULL AS STRING) AS updated_industry,
    CAST (NULL AS STRING) AS updated_position,
    DATE'{actual_date}' AS actual_date
FROM src
INNER JOIN final_offer fo
    ON src.bbn = fo.bbn
WHERE src.rn = 1