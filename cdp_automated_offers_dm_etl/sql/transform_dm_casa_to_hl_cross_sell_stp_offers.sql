WITH active_loans AS (
  SELECT
    bbn,
    product_code,
    booking_date,
    booked_amortization_amount,
    CAST(days_past_due AS INT) AS dpd,
    CAST(
      months_between(
        DATE '{actual_date}', booking_date
      ) AS INT
    ) AS mob_months
  FROM
    df_pm_eq
  WHERE
    active_yesno = 1
    AND booked_yesno = 1
    AND bbn IS NOT NULL
),
loan_rollup AS (
  SELECT
    bbn,
    SUM(
      COALESCE(booked_amortization_amount, 0)
    ) AS amort_all_loans,
    SUM(CASE WHEN dpd > 0 THEN 1 ELSE 0 END) AS non_current_cnt,
    MAX(
      CASE WHEN product_code = 'HL' THEN mob_months END
    ) AS hl_mob,
    MAX(
      CASE WHEN product_code = 'PL' THEN mob_months END
    ) AS pl_mob,
    MAX(
      CASE WHEN product_code = 'AL' THEN mob_months END
    ) AS al_mob,
    MAX(
      CASE WHEN product_code IN ('CTS', 'BEL', 'BML') THEN mob_months END
    ) AS oth_mob,
    MAX(
      CASE WHEN product_code IN ('HL', 'AL', 'PL') THEN 1 ELSE 0 END
    ) AS has_hl_al_pl,
    MAX(
      CASE WHEN product_code = 'CSL' THEN 1 ELSE 0 END
    ) AS has_csl,
    MAX(
      CASE WHEN product_code IN ('CTS', 'BEL', 'BML') THEN 1 ELSE 0 END
    ) AS has_oth
  FROM
    active_loans
  GROUP BY
    bbn
),
base_deduped AS (
  SELECT
    cross_sell.*,
    ROW_NUMBER() OVER (
      PARTITION BY cross_sell.bbn
      ORDER BY
        cross_sell.bbn
    ) AS rn
  FROM
    staging_cross_sell_base cross_sell
  WHERE
    -- ==========================================
    -- INJECTED DYNAMIC JSON RULES
    -- ==========================================
    {dynamic_json_rules}
    -- ==========================================
),
base_calculations AS (
  SELECT
    base.*,
    loans.amort_all_loans,
    (
      GREATEST(
        0.4 * base.adb_3m_avg,
        CAST(base.gmi_value AS DOUBLE)
      ) * 0.75 * 0.5 - COALESCE(loans.amort_all_loans, 0)
    ) AS max_amortization_capacity,
    CAST(
      LEAST(25, 65 - base.age) AS INT
    ) AS avail_loan_term_years
  FROM
    base_deduped base
    LEFT JOIN loan_rollup loans ON base.bbn = loans.bbn
  WHERE
    base.rn = 1
    
    -- Join structural filters retained
    AND COALESCE(loans.has_hl_al_pl, 0) = 0
    AND NOT (
      loans.hl_mob IS NOT NULL
      AND loans.hl_mob < 6
    )
    AND NOT (
      loans.pl_mob IS NOT NULL
      AND loans.pl_mob < 6
    )
    AND NOT (
      loans.oth_mob IS NOT NULL
      AND loans.oth_mob < 6
    )
),
calculated_offers AS (
  SELECT
    *,
    LEAST(
      20000000,
      FLOOR(
        (
          max_amortization_capacity * (
            1 - POW(
              1 + (0.068 / 12.0),
              -12 * avail_loan_term_years
            )
          ) / (0.068 / 12.0)
        ) / 1000.0
      ) * 1000.0
    ) AS raw_max_loan_amount
  FROM
    base_calculations
),
active_offers AS (
  SELECT
    bbn,
    offer_code,
    expiry_date
  FROM
    dm_casa_to_hl_cross_sell_stp_offers_for_month
  WHERE
    expiry_date > DATE '{actual_date}' AND actual_date < DATE'{actual_date}'
),
max_offer_seq AS (
  SELECT
    COALESCE(MAX(CAST(SUBSTRING(offer_code, 11) AS INT)), 0) AS max_seq
  FROM
    active_offers
  WHERE
    date_format(DATE '{actual_date}', 'MMyy') = SUBSTRING(offer_code, 6, 4)
),
dedup_stp_hl AS (
  SELECT
    CASE
      WHEN base.age IS NULL THEN NULL
      WHEN (65 - base.age) < 0 THEN NULL
      ELSE CAST(LEAST(25, 65 - base.age) AS INT)
    END AS max_loan_term,
    base.bbn,
    'DEPO TO HL CROSS SELL' AS program_offer_type,
    base.expiry_date,
    base.full_name,
    base.last_name,
    base.first_name,
    base.middle_name,
    'STP' AS hl_ratecode,
    CASE
      WHEN base.avail_loan_term_years IS NULL OR base.avail_loan_term_years <= 0 THEN NULL
      WHEN base.raw_max_loan_amount < 1000000 THEN NULL
      ELSE CAST(base.raw_max_loan_amount AS STRING)
    END AS max_loan_amount,
    CAST(
      GREATEST(
        base.adb_3m_avg * 0.4,
        CAST(base.gmi_value AS DOUBLE)
      ) * 0.75 * 0.5 - COALESCE(base.amort_all_loans, 0) AS STRING
    ) AS max_amortization,
    'HL' AS product_crm,
    'HL' AS spiel_id,
    CAST(NULL AS STRING) AS campaign_code,
    base.earliest_acct_open_date,
    base.age,
    CAST(NULL AS STRING) AS leads_type,
    CAST(NULL AS STRING) AS promo,
    (
      CASE WHEN base.birthdate IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN base.mothers_maiden_name IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN base.present_address IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN base.office_address IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN base.present_email IS NOT NULL THEN 1 ELSE 0 END
    ) AS pid_count,
    base.branch_region_name AS branch_region,
    base.branch_area_code AS branch_area,
    base.branch_mnemonic AS branch_mnemonic,
    base.branch_name,
    CAST(NULL AS STRING) AS product_bundle,
    base.new_segment,
    base.depo_acct_num AS accountnumber,
    CAST(NULL AS STRING) AS product_name,
    CAST(NULL AS STRING) AS qualified_casa,
    CAST(NULL AS STRING) AS promo_code,
    CAST(NULL AS STRING) AS ob,
    CAST(NULL AS STRING) AS ob_tier,
    CAST(
      base.total_relationship_balance_value AS STRING
    ) AS trb,
    CASE WHEN base.total_relationship_balance_value < 200000 THEN 'a.<200k' WHEN base.total_relationship_balance_value BETWEEN 200000
    AND 250000 THEN 'b.200k-250k' WHEN base.total_relationship_balance_value > 250000
    AND base.total_relationship_balance_value <= 300000 THEN 'c.250k-300k' WHEN base.total_relationship_balance_value > 300000
    AND base.total_relationship_balance_value <= 350000 THEN 'd.300k-350k' WHEN base.total_relationship_balance_value > 350000
    AND base.total_relationship_balance_value <= 400000 THEN 'e.350k-400k' WHEN base.total_relationship_balance_value > 400000
    AND base.total_relationship_balance_value <= 450000 THEN 'f.400k-450k' WHEN base.total_relationship_balance_value > 450000
    AND base.total_relationship_balance_value < 500000 THEN 'g.450k-500k' WHEN base.total_relationship_balance_value >= 500000 THEN 'h.>=500k' ELSE 'UNKNOWN' END AS trb_bucket,
    CASE WHEN base.age BETWEEN 18
    AND 29 THEN 'a.18-29' WHEN base.age BETWEEN 30
    AND 39 THEN 'b.30-39' WHEN base.age BETWEEN 40
    AND 49 THEN 'c.40-49' WHEN base.age >= 50 THEN 'd.50 & up' ELSE 'UNKNOWN' END AS age_tier,
    CAST(NULL AS STRING) AS casa_acct_php,
    CAST(NULL AS STRING) AS casa_acct_usd,
    CAST(NULL AS STRING) AS deal_acct_php,
    CAST(NULL AS STRING) AS deal_acct_usd,
    CAST(NULL AS INT) AS gold_propensity,
    CASE
      WHEN base.age >= 70 THEN 1
      ELSE 0
    END AS age_70,
    CAST(NULL AS STRING) AS agent_name,
    CAST(NULL AS STRING) AS purpose,
    CAST(NULL AS STRING) AS agent_remarks,
    CAST(NULL AS STRING) AS spiel,
    CAST(NULL AS STRING) AS other_card,
    CAST(NULL AS STRING) AS special_otherremarks,
    'Asia/Manila' AS timezone,
    base.present_email AS email,
    CASE
      WHEN base.gender_code = 'M' THEN 'Male'
      WHEN base.gender_code = 'F' THEN 'Female'
      ELSE ''
    END AS gender,
    CASE
      WHEN base.civil_status_code = '0' THEN 'Single'
      WHEN base.civil_status_code = '1' THEN 'Married'
      WHEN base.civil_status_code = '2' THEN 'Separated'
      WHEN base.civil_status_code = '3' THEN 'Widowed'
      ELSE ''
    END AS civil_status,
    base.birthdate AS date_of_birth,
    base.present_zip_code AS zipcode,
    CASE
      WHEN base.employment_type_code = '0' THEN 'Employed'
      WHEN base.employment_type_code = '1' THEN 'Self-employed'
      WHEN base.employment_type_code = '2' THEN 'Unemployed'
      ELSE ''
    END AS employment_type,
    base.employer_name,
    base.occupation_details_text AS occupation,
    base.tin_comptin AS tin,
    base.birth_place_name AS place_of_birth,
    base.present_address,
    base.permanent_address,
    base.primary_id_type AS id_type,
    base.primary_id_number AS id_number,
    base.mothers_maiden_name AS mother_maiden_name,
    CASE
      WHEN base.employment_type_code = '0' THEN 'Employed'
      WHEN base.employment_type_code = '1' THEN 'Self-employed'
      WHEN base.employment_type_code = '2' THEN 'Unemployed'
      ELSE ''
    END AS employment_status,
    base.office_address AS employer_address,
    base.nature_of_work_code AS nature_of_business,
    base.priority_mobile_no1 AS mobile_no,
    base.priority_mobile_no2 AS mobile_no2,
    base.priority_home_landline1 AS home_landline1,
    base.office_landline1,
    CAST(NULL AS STRING) AS length_of_stay,
    CAST(NULL AS STRING) AS no_of_years_employed,
    CAST(NULL AS STRING) AS year_started_working,
    CAST(NULL AS STRING) AS current_industry,
    CAST(NULL AS STRING) AS updated_industry,
    CAST(NULL AS STRING) AS updated_position
  FROM
    calculated_offers base
  LEFT JOIN active_offers existing_offer
    ON base.bbn = existing_offer.bbn
  WHERE
    base.raw_max_loan_amount >= 1000000
    AND existing_offer.bbn IS NULL
)
SELECT
  CONCAT(
    'HL',
    'STP',
    date_format(DATE '{actual_date}', 'MM'),
    date_format(DATE '{actual_date}', 'yy'),
    'B',
    LPAD(
      CAST(
        row_number() OVER (ORDER BY bbn) + max_offer_seq.max_seq AS STRING
      ),
      7,
      '0'
    )
  ) AS offer_code,
  dedup_stp_hl.*,
  DATE '{actual_date}' AS actual_date
FROM
  dedup_stp_hl
CROSS JOIN max_offer_seq