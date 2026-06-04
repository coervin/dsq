WITH active_offers AS (
    SELECT
        bbn,
        offer_code,
        expiry_date
    FROM dm_casa_to_hl_cross_sell_ptb_offers_for_month
    WHERE expiry_date > DATE '{actual_date}' AND actual_date < DATE '{actual_date}'
)
, max_offer_seq AS (
    SELECT
        COALESCE(MAX(CAST(SUBSTRING(offer_code, 11) AS INT)), 0) AS max_seq
    FROM active_offers
    WHERE date_format(DATE '{actual_date}', 'MMyy') = SUBSTRING(offer_code, 6, 4)
)
, base_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bbn ORDER BY bbn) AS rn
    FROM staging_cross_sell_base base
    WHERE 
        -- ==========================================
        -- INJECTED DYNAMIC JSON RULES
        -- ==========================================
        {dynamic_json_rules}
        -- ==========================================
        
        -- Join structural filters retained
        AND NOT EXISTS (
            SELECT 1 FROM dm_casa_to_hl_cross_sell_stp_offers_for_month stp_hl
            WHERE base.bbn = stp_hl.bbn
                AND stp_hl.actual_date <= DATE '{actual_date}'
                AND stp_hl.expiry_date > DATE '{actual_date}'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dm_casa_to_al_cross_sell_ptb_offers_for_month ptb_al
            WHERE base.bbn = ptb_al.bbn
                AND ptb_al.actual_date <= DATE '{actual_date}'
                AND ptb_al.expiry_date > DATE '{actual_date}'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dm_casa_to_cc_cross_sell_ptb_offers_for_month ptb_cc
            WHERE base.bbn = ptb_cc.bbn
                AND ptb_cc.actual_date <= DATE '{actual_date}'
                AND ptb_cc.expiry_date > DATE '{actual_date}'
        )
)
SELECT
    base.bbn,
    CASE
        WHEN ptb.hl_propensity_score >= 0.6 THEN 'HIGH'
        WHEN ptb.hl_propensity_score >  0.3 THEN 'MID'
        WHEN ptb.hl_propensity_score <= 0.3 THEN 'LOW'
    END AS hl_propensity_label_desc,
    base.full_name,
    base.present_email AS email_address_text,
    base.priority_mobile_no AS mobile_num_value,
    base.client_segment_name AS client_segment_code,
    base.new_segment AS deposit_segment_current_desc,
    base.branch_code,
    CONCAT(
        'HL',
        'PTB',
        date_format(DATE '{actual_date}', 'MM'),
        date_format(DATE '{actual_date}', 'yy'),
        'B',
        LPAD(
            CAST(
                row_number() OVER (
                    ORDER BY base.bbn
                ) + max_offer_seq.max_seq AS STRING
            ),
            7,
            '0'
        )
    ) AS offer_code,
    'PTB' AS offer_type_desc,
    'HL' AS product_crm_code,
    'HL Propensity' AS spiel_id_desc,
    CAST(NULL AS STRING) AS campaign_code,
    CASE WHEN base.is_dnc_flag = 1 THEN 'Y' ELSE 'N' END AS is_dnc_flag,
    CASE WHEN base.priority_mobile_no IS NOT NULL THEN 'Y' ELSE 'N' END AS is_mobile_num_valid_flag,
    CASE
        WHEN base.present_email IS NULL
            OR TRIM(base.present_email) = '' THEN 'N'
        WHEN UPPER(TRIM(base.present_email)) IN (
            'NONE','NA','N.A.','0','N/A','---','--------','MA','NODATA',
            'NULL','NODATA@YAHOO.COM','NONE@NONE.COM',
            'NONE@YAHOO.COM','NA@YAHOO.COM'
        ) THEN 'N'
        WHEN base.present_email NOT LIKE '%@%' THEN 'N'
        WHEN NOT RLIKE(base.present_email, '[A-Za-z]') THEN 'N'
        WHEN LOWER(SPLIT(base.present_email, '@')[0]) IN (
            'raymond.nunez+sbc',
            '+639461896813',
            'pbatorre+sb',
            'm.ilagan.jr+secb',
            'tsdmgz+secb',
            'yen101+'
        ) THEN 'N'
        WHEN base.present_email LIKE '%@%'
            AND TRIM(UPPER(REGEXP_EXTRACT(base.present_email, '@([^.]+)', 1))) NOT IN (
                'GMAIL','YAHOO','YMAIL','HOTMAIL','ICLOUD','ROCKETMAIL','OUTLOOK',
                'ANZ','TASKUS','BMS','LIVE','MANULIFE','IQOR',
                'SYNCHRONYFINANCIAL','SPI-GLOBAL','TRANSCOM',
                'ACCENTURE','GLOBE','ALORICA','CLOUDSTAFF',
                'QQ','RESULTS-CX','TENETHEALTH','PROTONMAIL'
            ) THEN 'N'
        WHEN
            base.present_email IS NOT NULL
            AND TRIM(base.present_email) <> ''
            AND UPPER(TRIM(base.present_email)) NOT IN (
                'NONE','NA','N.A.','0','N/A','---','--------','MA','NODATA',
                'NULL','NODATA@YAHOO.COM','NONE@NONE.COM',
                'NONE@YAHOO.COM','NA@YAHOO.COM'
            )
            AND base.present_email LIKE '%@%'
            AND RLIKE(base.present_email, '[A-Za-z]')
            AND LOWER(SPLIT(base.present_email, '@')[0]) NOT IN (
                'raymond.nunez+sbc',
                '+639461896813',
                'pbatorre+sb',
                'm.ilagan.jr+secb',
                'tsdmgz+secb',
                'yen101+'
            )
            AND TRIM(UPPER(REGEXP_EXTRACT(base.present_email, '@([^.]+)', 1))) IN (
                'GMAIL','YAHOO','YMAIL','HOTMAIL','ICLOUD','ROCKETMAIL','OUTLOOK',
                'ANZ','TASKUS','BMS','LIVE','MANULIFE','IQOR',
                'SYNCHRONYFINANCIAL','SPI-GLOBAL','TRANSCOM',
                'ACCENTURE','GLOBE','ALORICA','CLOUDSTAFF',
                'QQ','RESULTS-CX','TENETHEALTH','PROTONMAIL'
            )
        THEN 'Y'
        ELSE 'N'
    END AS is_email_valid_flag,
    CASE
        WHEN base.new_segment <> 'MASS MARKET' AND base.priority_mobile_no IS NOT NULL
        THEN 'Y'
        ELSE 'N'
    END AS is_for_branch_flag,
    base.expiry_date,
    DATE'{actual_date}' AS actual_date
FROM base_deduped base
LEFT JOIN casatohlptb ptb
    ON ptb.bbn = base.bbn
LEFT JOIN active_offers existing_offer
    ON existing_offer.bbn = base.bbn
CROSS JOIN max_offer_seq
WHERE base.rn = 1
    AND existing_offer.bbn IS NULL