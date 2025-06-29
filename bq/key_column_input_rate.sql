-- 今日のデータがあれば削除してから挿入
DELETE FROM datamart.inputrate_bunjo_email 
WHERE insert_date = CURRENT_DATE('Asia/Tokyo');

INSERT INTO datamart.inputrate_bunjo_email (
  insert_date,
  total_customers,
  email_input_count
)
--CREATE OR REPLACE TABLE datamart.inputrate_bunjo_email as
WITH t1 AS(
  SELECT
    unifiedindividualid__c,
    resfa_pc_mailaddress1__c,
    resfa_pc_mailaddress2__c,
    resfa_pc_mailaddress3__c,
    CURRENT_DATE('Asia/Tokyo') as insert_date,
    re_insert_date__c
  FROM odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_Tableau_Tableau__dll
  WHERE resfa_id__c is not null
)
SELECT 
  insert_date,
  count(unifiedindividualid__c) as total_customers,
  COUNT(
      DISTINCT CASE 
        WHEN resfa_pc_mailaddress1__c IS NOT NULL 
            OR resfa_pc_mailaddress2__c IS NOT NULL 
            OR resfa_pc_mailaddress3__c IS NOT NULL 
        THEN unifiedindividualid__c 
        END) as email_input_count
FROM t1
GROUP BY insert_date;

select * from datamart.inputrate_bunjo_email ;