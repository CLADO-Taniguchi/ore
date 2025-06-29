CREATE TABLE IF NOT EXISTS datamart.input_history (
  unifiedindividualid__c STRING,
  key_column STRING,         -- 例: 'esm_mailaddress1__c'
  input_date DATE,             -- 入力が確認された日
  registry_date DATE          -- 基幹システムごとの基準日
);

-- 日次バッチで新規入力分のみ履歴テーブルに追加するクエリ例
INSERT INTO datamart.input_history (unifiedindividualid__c, key_column, input_date)
SELECT unifiedindividualid__c, key_column, input_date, registry_date
FROM (
  WITH today AS (
    SELECT
      unifiedindividualid__c,
      resfa_pc_mailaddress1__c,
      resfa_pc_mailaddress2__c,
      resfa_pc_mailaddress3__c,
      resfa_pc_tel1__c,
      resfa_pc_tel2__c,
      resfa_pc_tel3__c,
      resfa_pc_seikana__c,
      resfa_pc_addressall__c,
      kintone_buy_emailpc__c,
      kintone_buy_emailkeitai__c,
      kintone_buy_telhonnin__c,
      kintone_buy_keitaitelhonnin__c,
      kintone_buy_furiganahonnin__c,
      kintone_buy_addresshonnin__c,
      kintone_buy_tourokuday__c,
      kintone_sales_emailpc__c,
      kintone_sales_telhonnin__c,
      kintone_sales_keitaitelhonnin__c,
      kintone_sales_furiganahonnin__c,
      kintone_sales_addresshonnin__c,
      kintone_sales_tourokudate__c,
      esm_mailaddress1__c,
      esm_mailaddress2__c,
      esm_phone__c,
      esm_keitaitel1__c,
      esm_keitaitel2__c,
      esm_customernamealias__c,
      esm_kyojyukeitai__c,
      esm_createdat__c,
      wrs_mail__c,
      wrs_telkanyu1__c,
      wrs_kokana__c,
      wrs_add1__c,
      wrs_add2__c,
      wrs_indate__c,
      CASE WHEN resfa_registry_date IS NOT NULL THEN resfa_registry_date
            WHEN kintone_buy_registry_date IS NOT NULL THEN kintone_buy_registry_date
            WHEN kintone_sales_registry_date IS NOT NULL THEN kintone_sales_registry_date
            WHEN esm_registry_date IS NOT NULL THEN esm_registry_date
            WHEN wrs_registry_date IS NOT NULL THEN wrs_registry_date
    FROM odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_Tableau_Tableau__dll
  ),
  unpivoted AS (
    SELECT unifiedindividualid__c, 'resfa_pc_mailaddress1__c' AS key_column, CAST(resfa_pc_mailaddress1__c AS STRING) AS value FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_mailaddress2__c', CAST(resfa_pc_mailaddress2__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_mailaddress3__c', CAST(resfa_pc_mailaddress3__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_tel1__c', CAST(resfa_pc_tel1__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_tel2__c', CAST(resfa_pc_tel2__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_tel3__c', CAST(resfa_pc_tel3__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_seikana__c', CAST(resfa_pc_seikana__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'resfa_pc_addressall__c', CAST(resfa_pc_addressall__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_emailpc__c', CAST(kintone_buy_emailpc__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_emailkeitai__c', CAST(kintone_buy_emailkeitai__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_telhonnin__c', CAST(kintone_buy_telhonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_keitaitelhonnin__c', CAST(kintone_buy_keitaitelhonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_furiganahonnin__c', CAST(kintone_buy_furiganahonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_addresshonnin__c', CAST(kintone_buy_addresshonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_buy_tourokuday__c', CAST(kintone_buy_tourokuday__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_emailpc__c', CAST(kintone_sales_emailpc__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_telhonnin__c', CAST(kintone_sales_telhonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_keitaitelhonnin__c', CAST(kintone_sales_keitaitelhonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_furiganahonnin__c', CAST(kintone_sales_furiganahonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_addresshonnin__c', CAST(kintone_sales_addresshonnin__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'kintone_sales_tourokudate__c', CAST(kintone_sales_tourokudate__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_mailaddress1__c', CAST(esm_mailaddress1__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_mailaddress2__c', CAST(esm_mailaddress2__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_phone__c', CAST(esm_phone__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_keitaitel1__c', CAST(esm_keitaitel1__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_keitaitel2__c', CAST(esm_keitaitel2__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_customernamealias__c', CAST(esm_customernamealias__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_kyojyukeitai__c', CAST(esm_kyojyukeitai__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'esm_createdat__c', CAST(esm_createdat__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_mail__c', CAST(wrs_mail__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_telkanyu1__c', CAST(wrs_telkanyu1__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_kokana__c', CAST(wrs_kokana__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_add1__c', CAST(wrs_add1__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_add2__c', CAST(wrs_add2__c AS STRING) FROM today UNION ALL
    SELECT unifiedindividualid__c, 'wrs_indate__c', CAST(wrs_indate__c AS STRING) FROM today
  ),
  new_inputs AS (
    SELECT
      u.unifiedindividualid__c,
      u.key_column,
      CURRENT_DATE('Asia/Tokyo') AS input_date
    FROM unpivoted u
    LEFT JOIN datamart.input_history h
      ON u.unifiedindividualid__c = h.unifiedindividualid__c
      AND u.key_column = h.key_column
    WHERE u.value IS NOT NULL
      AND h.input_date IS NULL
  )
  SELECT unifiedindividualid__c, key_column, input_date FROM new_inputs
); 