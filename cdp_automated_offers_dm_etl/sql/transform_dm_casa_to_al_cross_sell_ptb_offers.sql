WITH casa_to_al_stp_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
    FROM dm_casa_to_al_cross_sell_stp_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date <= DATE'{actual_date}'
)
, casa_to_cc_ptb_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
    FROM dm_casa_to_cc_cross_sell_ptb_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date <= DATE'{actual_date}'
)
, casa_to_al_ptb_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
        FROM dm_casa_to_al_cross_sell_ptb_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date < DATE'{actual_date}'
)
, src AS (
    SELECT
        base.*,
        ROW_NUMBER() OVER (
            PARTITION BY base.bbn
            ORDER BY base.bbn
        ) AS rn
    FROM staging_cross_sell_base base
    WHERE
        base.bbn IS NOT NULL

        -- ==========================================
        -- MOVED TO DYNAMIC JSON RULES
        -- ==========================================

        AND NOT EXISTS (
            SELECT 1
            FROM casa_to_al_stp_not_yet_expired a
            WHERE a.bbn = base.bbn
        )

        AND NOT EXISTS (
            SELECT 1
            FROM casa_to_cc_ptb_not_yet_expired c
            WHERE c.bbn = base.bbn
        )
)
, last_month_max_seq AS (
    SELECT
        COALESCE(MAX(CAST(substr(offer_code, length(offer_code) -6, 7) AS INTEGER)), 0) AS last_seq
    FROM casa_to_al_ptb_not_yet_expired
    WHERE
        CAST(SUBSTR(offer_code, 6, 2) AS INT) = month(DATE '{actual_date}')
        AND CAST(SUBSTR(offer_code, 8, 2) AS INT) = year(DATE '{actual_date}') % 100
)
, bbn_need_new AS (
    SELECT
        bbn,
        ROW_NUMBER() OVER (ORDER BY bbn) AS new_rank
    FROM src c
    WHERE
        rn = 1
        AND NOT EXISTS (
            SELECT 1 FROM casa_to_al_ptb_not_yet_expired a WHERE a.bbn = c.bbn
        )
)
, output_without_for_branch_and_email_valid_flag AS (
    SELECT
        src.bbn AS bbn,
        CASE
            WHEN dp.propensity >= 0.6  THEN 'HIGH'
            WHEN dp.propensity BETWEEN 0.3 AND 0.6 THEN 'MID'
            WHEN propensity <= 0.3 THEN 'LOW'
        END AS al_propensity_label_desc,
        src.full_name AS full_name,
        src.present_email AS email_address_text,
        src.priority_mobile_no AS mobile_num_value,
        src.client_segment_name AS client_segment_code,
        src.new_segment AS deposit_segment_current_desc,
        src.branch_code,
        CONCAT(
            'AL',
            'PTB',
            date_format(DATE '{actual_date}', 'MM'),
            date_format(DATE '{actual_date}', 'yy'),
            'B',
            LPAD(CAST(lms.last_seq + bn.new_rank AS STRING),
            7,
            '0')
        ) AS offer_code,
        'PTB' AS offer_type_desc,
        'AL' AS product_crm_code,
        'AL Propensity' AS spiel_id_desc,
        CAST(NULL AS STRING) AS campaign_code,
        CASE
            WHEN src.is_dnc_flag = 1 THEN 'Y'
            ELSE 'N'
        END AS is_dnc_flag,
        CASE
            WHEN src.priority_mobile_no IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS is_mobile_valid_flag,

        src.expiry_date,
        DATE'{actual_date}' AS actual_date,

        -- ==========================================
        -- PASS-THROUGH COLUMNS FOR JSON ENGINE
        -- ==========================================
        src.is_non_indiv_flag,
        src.is_with_al_application_12m_flag,
        src.is_dosri_flag,
        src.is_no_segment_flag,
        src.is_with_al_flag,
        src.age

    FROM src
    INNER JOIN bbn_need_new bn
        ON src.bbn = bn.bbn
    CROSS JOIN last_month_max_seq lms
    LEFT JOIN ptbcasatoal dp
        ON src.bbn = dp.bbn
    WHERE rn = 1
)
SELECT
    bbn,
    al_propensity_label_desc,
    full_name,
    email_address_text,
    mobile_num_value,
    client_segment_code,
    deposit_segment_current_desc,
    branch_code,
    offer_code,
    offer_type_desc,
    product_crm_code,
    spiel_id_desc,
    campaign_code,
    CASE
        WHEN deposit_segment_current_desc <> 'MASS MARKET'
            AND is_mobile_valid_flag = 'Y' THEN 'Y'
        ELSE 'N'
    END AS is_for_branch_flag,
    is_dnc_flag,
    is_mobile_valid_flag,
    CASE
        WHEN email_address_text IS NULL
            OR TRIM(email_address_text) = '' THEN 'N'
        WHEN UPPER(TRIM(email_address_text)) IN (
                'NONE','NA','N.A.','0','N/A','---','--------','MA','NODATA',
                'NULL','NODATA@YAHOO.COM','NONE@NONE.COM',
                'NONE@YAHOO.COM','NA@YAHOO.COM'
            ) THEN 'N'
        WHEN email_address_text NOT LIKE '%@%' THEN 'N'
        WHEN email_address_text NOT RLIKE '[A-Za-z]' THEN 'N'
        WHEN LOWER(SPLIT(email_address_text, '@')[0]) IN (
            'raymond.nunez+sbc',
            '+639461896813',
            'pbatorre+sb',
            'm.ilagan.jr+secb',
            'tsdmgz+secb',
            'yen101+'
            ) THEN 'N'
        WHEN email_address_text LIKE '%@%'
            AND TRIM(UPPER(REGEXP_EXTRACT(email_address_text, '@([^.]+)', 1))) NOT IN (
                'GMAIL','YAHOO','YMAIL','HOTMAIL','ICLOUD','ROCKETMAIL','OUTLOOK',
                'ANZ','TASKUS','BMS','LIVE','MANULIFE','IQOR',
                'SYNCHRONYFINANCIAL','SPI-GLOBAL','TRANSCOM',
                'ACCENTURE','GLOBE','ALORICA','CLOUDSTAFF',
                'QQ','RESULTS-CX','TENETHEALTH','PROTONMAIL'
            ) THEN 'N'
        WHEN email_address_text IS NOT NULL
            AND TRIM(email_address_text) <> ''
            AND UPPER(TRIM(email_address_text)) NOT IN (
                'NONE','NA','N.A.','0','N/A','---','--------','MA','NODATA',
                'NULL','NODATA@YAHOO.COM','NONE@NONE.COM',
                'NONE@YAHOO.COM','NA@YAHOO.COM'
            )
            AND email_address_text LIKE '%@%'
            AND email_address_text RLIKE '[A-Za-z]'
            AND LOWER(SPLIT(email_address_text, '@')[0]) NOT IN (
                'raymond.nunez+sbc',
                '+639461896813',
                'pbatorre+sb',
                'm.ilagan.jr+secb',
                'tsdmgz+secb',
                'yen101+'
            )
            AND TRIM(UPPER(REGEXP_EXTRACT(email_address_text, '@([^.]+)', 1))) IN (
                'GMAIL','YAHOO','YMAIL','HOTMAIL','ICLOUD','ROCKETMAIL','OUTLOOK',
                'ANZ','TASKUS','BMS','LIVE','MANULIFE','IQOR',
                'SYNCHRONYFINANCIAL','SPI-GLOBAL','TRANSCOM',
                'ACCENTURE','GLOBE','ALORICA','CLOUDSTAFF',
                'QQ','RESULTS-CX','TENETHEALTH','PROTONMAIL'
            )
            THEN 'Y'
        ELSE 'N'
    END AS is_email_valid_flag,

    -- ==========================================
    -- PASS-THROUGH COLUMNS FOR JSON ENGINE
    -- ==========================================
    is_non_indiv_flag,
    is_with_al_application_12m_flag,
    is_dosri_flag,
    is_no_segment_flag,
    is_with_al_flag,
    age,

    expiry_date,
    actual_date
    FROM output_without_for_branch_and_email_valid_flag