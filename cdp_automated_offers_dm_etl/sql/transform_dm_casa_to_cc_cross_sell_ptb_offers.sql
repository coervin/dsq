WITH casa_to_cc_stp_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
    FROM dm_casa_to_cc_cross_sell_stp_offers_for_month
    WHERE expiry_date > DATE'{actual_date}'
)
, casa_to_cc_ptb_not_yet_expired AS (
    SELECT
        bbn,
        offer_code
    FROM dm_casa_to_cc_cross_sell_ptb_offers_for_month
    WHERE expiry_date > DATE'{actual_date}' AND actual_date < DATE'{actual_date}'
)
, last_max_offer AS (
    SELECT
        COALESCE(MAX(CAST(substr(offer_code, length(offer_code) -6, 7) AS INTEGER)), 0) AS max_row_num
    FROM casa_to_cc_ptb_not_yet_expired
    WHERE substring(offer_code, LENGTH(offer_code) -11, 2) = DATE_FORMAT(DATE'{actual_date}', 'MM')
        AND substring(offer_code, LENGTH(offer_code) -9, 2) = DATE_FORMAT(DATE'{actual_date}', 'yy')
)
, ptb_casa_to_cc_base AS (
    SELECT DISTINCT
        base.bbn,
        base.branch_code,
        base.branch_name,
        base.branch_area_code,
        base.branch_region_name,
        base.full_name,
        base.first_name,
        base.last_name,
        base.middle_name,
        base.priority_mobile_no AS mobile_num_value,
        base.present_email AS email_address_text,
        base.total_relationship_balance_value,
        base.cust_mob_count,
        base.expiry_date,
        base.actual_date
    FROM staging_cross_sell_base base
    LEFT JOIN casa_to_cc_stp_not_yet_expired stp_casa_to_cc
        ON base.bbn = stp_casa_to_cc.bbn
    LEFT JOIN casa_to_cc_ptb_not_yet_expired ptb_casa_to_cc
        ON base.bbn = ptb_casa_to_cc.bbn
    WHERE 
        -- ==========================================
        -- INJECTED DYNAMIC JSON RULES
        -- ==========================================
        {dynamic_json_rules}
        -- ==========================================

        -- Join structural filters retained
        AND stp_casa_to_cc.bbn IS NULL
        AND ptb_casa_to_cc.bbn IS NULL
)
, activity_filtered AS (
    SELECT
        activityid,
        createdon,
        relatedtoid,
        relatedtotypename,
        layoutid,
        statusid
    FROM activity
    WHERE  createdon BETWEEN add_months(DATE'{actual_date}', -3) AND DATE'{actual_date}'
        AND ownerid = 608
)
, lookupmaster_filtered AS (
    SELECT
        *
    FROM lookupmaster
    WHERE lookupmaster.groupkey = 16 AND lookupmaster.ownerid = 608
)
, contacted_only AS (
    SELECT * FROM act_ex1 WHERE act_ex1_2 = 'Contacted'
)
, eligible_products AS (
    SELECT * FROM products
    WHERE (UPPER(name) LIKE '%CREDIT CARD%' OR UPPER(name) LIKE '%MASTERCARD%' OR UPPER(name) LIKE '%MC%')
        AND UPPER(name) NOT LIKE '%DIMA%'
        AND UPPER(name) NOT LIKE '%TIME LOAN%'
        AND UPPER(name) NOT LIKE '%CASHLINK%'
)
, lea_ex1_filtered AS (
    SELECT * FROM lea_ex1
    WHERE lea_ex1_38 IS NOT NULL OR lea_ex1_38 <> ''
)
, activity_joined AS (
    SELECT
        lef.lea_ex1_38 AS bbn,
        CASE
            WHEN SUM(CASE
                    WHEN (af.layoutid = 200198 OR UPPER(af.relatedtotypename) = 'OFFER')
                         AND lf.name IN ('Interested','For Call Back','Not Interested')
                         AND co.act_ex1_2 = 'Contacted'
                      THEN 1
                    WHEN UPPER(af.relatedtotypename) = 'LEAD'
                         AND lf.name IN ('Interested','For Call Back','Not Interested')
                      THEN 1
                    ELSE 0
                 END) > 0 THEN 1 ELSE 0
        END AS is_contacted
    FROM activity_filtered af
    LEFT JOIN lookupmaster_filtered lf
        ON af.statusid = lf.lookupid
    LEFT JOIN contacted_only co
        ON CAST(af.activityid AS STRING) = co.act_ex1_id
    LEFT JOIN leads
        ON af.relatedtoid = leads.leadid
    LEFT JOIN eligible_products products
        ON leads.productid = products.productid
    LEFT JOIN lea_ex1_filtered lef
        ON af.relatedtoid = lef.lea_ex1_id
    GROUP BY 1
)

SELECT
    base.bbn,
    base.branch_code,
    base.branch_name,
    base.branch_area_code,
    base.branch_region_name,
    base.full_name,
    base.first_name,
    base.last_name,
    base.middle_name,
    base.mobile_num_value,
    base.email_address_text,
    depo_clusters_v2.cluster_name AS deposit_cluster_desc,
    base.total_relationship_balance_value,
    base.cust_mob_count,
    CONCAT(
        'CC',
        'PTB',
        DATE_FORMAT(DATE'{actual_date}', 'MM'),
        DATE_FORMAT(DATE'{actual_date}', 'yy'),
        'B',
        lpad(CAST(ROW_NUMBER() OVER (ORDER BY base.bbn) + last_max_offer.max_row_num AS STRING), 7, '0')
    ) AS offer_code,
    'PTB' AS offer_type_desc,
    'CC' AS product_crm_code,
    'Cards Propensity' AS spiel_id_desc,
    CAST(NULL AS STRING) AS campaign_code,
    COALESCE(aj.is_contacted, 0) AS is_cust_contacted_3m_flag,
    base.expiry_date,
    base.actual_date
FROM ptb_casa_to_cc_base base
CROSS JOIN last_max_offer
LEFT JOIN depo_clusters_v2
    ON base.bbn = depo_clusters_v2.bbnumber
LEFT JOIN activity_joined aj
    ON base.bbn = aj.bbn