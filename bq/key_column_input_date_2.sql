CREATE OR REPLACE TABLE datamart.key_column_input_date as
  SELECT
    a.unifiedindividualid__c,
    --resfa（分譲）の入力率
    a.resfa_pc_mailaddress1__c,
    a.resfa_pc_mailaddress2__c,
    a.resfa_pc_mailaddress3__c,
    a.resfa_pc_tel1__c,
    a.resfa_pc_tel2__c,
    a.resfa_pc_tel3__c,
    a.resfa_pc_seikana__c,
    a.resfa_pc_addressall__c,
    min(b.re_insertdatetime__c) over (partition by b.unifiedindividualid__c) as resfa_registry_date,
    --kintone（仲介_買）の入力率
    a.kintone_buy_emailpc__c,
    a.kintone_buy_emailkeitai__c,
    a.kintone_buy_telhonnin__c,
    a.kintone_buy_keitaitelhonnin__c,
    a.kintone_buy_furiganahonnin__c,
    a.kintone_buy_addresshonnin__c,
    a.kintone_buy_tourokuday__c,
    --kitone(仲介_売)の入力率
    a.kintone_sales_emailpc__c,
    a.kintone_sales_telhonnin__c,
    a.kintone_sales_keitaitelhonnin__c,
    a.kintone_sales_furiganahonnin__c,
    a.kintone_sales_addresshonnin__c,
    a.kintone_sales_tourokudate__c,
    --esm（顧客開発）の入力率
    a.esm_mailaddress1__c,
    a.esm_mailaddress2__c,
    a.esm_phone__c,
    a.esm_keitaitel1__c,
    a.esm_keitaitel2__c,
    a.esm_customernamealias__c,
    a.esm_kyojyukeitai__c,
    a.esm_createdat__c,
    --wrs（ハウジング）の入力率
    a.wrs_mail__c,
    a.wrs_telkanyu1__c,
    a.wrs_kokana__c,
    a.wrs_add1__c,
    a.wrs_add2__c,
    a.wrs_indate__c,
    --CURRENT_DATE('Asia/Tokyo') as insert_date
    null as insert_date
  FROM odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_Tableau_Tableau__dll a
    LEFT JOIN odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Hankyo_Tableau__dll b
        ON a.unifiedindividualid__c = b.unifiedindividualid__c;
