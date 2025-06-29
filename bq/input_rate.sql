--create table datamart.active as
select
    distinct unifiedindividualid__c
    ,max(ct_contactdate__c) over (partition by unifiedindividualid__c) as reference_date
    ,pc_pid__c as coresystem_id
    ,'bunjou_baikyaku' as department
from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Contact_Tableau__dll
where
	--コンタクト種別が以下対象
	unifiedindividualid__c in
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Contact_Tableau__dll
			where
				ct_contactstype_code__c = '1020' or
				ct_contactstype_code__c = '1060' or
				ct_contactstype_code__c = '1070' or
				ct_contactstype_code__c = '1075' or
				ct_contactstype_code__c = '2050'
		)
	--各ステータスのコンタクト日より５日以内
	and ct_contactdate__c is not null
	--メールアドレス1あり
	and unifiedindividualid__c  in
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
			where pc_mailaddress1__c is not null)
	--メールアドレス1フラグ
	and unifiedindividualid__c  in
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
			where pc_mailaddress1flg__c = '可')
	--居住形態
	and unifiedindividualid__c in 
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
			where bc_residencestatus_code__c in ('0015','0010'))
	--居住形態
	and unifiedindividualid__c in
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Hankyo_Tableau__dll
			where re_changeschedule_code__c = '00')
	--居住地
	and unifiedindividualid__c in 
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
			where pc_address1__c in ('東京都','神奈川県'))
	--対象物件
	and unifiedindividualid__c in 
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Hankyo_Tableau__dll
			where re_objectcode__c in ('12150000','12130000','11169000','11170000','11171000'))
	--リーフィア会員
	and unifiedindividualid__c in 
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
			where bc_insertdatetime__c is not null)
	--JV物件除く
	and unifiedindividualid__c not in
		(select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_RecM_BukkenMst_Tableau__dll
			where re_businessflg_1_code__c = '0020')

UNION ALL

select
    distinct unifiedindividualid__c
    ,max(ct_contactdate__c) over (partition by unifiedindividualid__c) as reference_date
    ,pc_pid__c as coresystem_id
    ,'bunjou_noaction' as department
from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Contact_Tableau__dll
where
        --半年間以下コンタクトなし
	unifiedindividualid__c in
        (
            select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Hankyo_Tableau__dll
                where not (cast(re_contactdate__c as date) >= '2023-08-19'
                    and re_contacttype_code__c not in ('1020','1030','1040','1050','1060','1070','1075','1080','1090','2020','2030','2040','2045','2050','2060')))
        --メールアドレス1あり
        and unifiedindividualid__c in 
            (select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
                where pc_mailaddress1__c is not null)
        --メールアドレス1フラグ
        and unifiedindividualid__c  in
            (select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
                where pc_mailaddress1flg__c = '可')
        --リーフィア会員
        and unifiedindividualid__c in
                (select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_ReSFA_Kokyaku_Tableau__dll
                        where bc_insertdatetime__c is not null)
        --JV物件除く
        and unifiedindividualid__c not in
                (select distinct unifiedindividualid__c from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.Bunjyo_RecM_BukkenMst_Tableau__dll
                        where re_businessflg_1_code__c = '0020')

UNION ALL

select
    unifiedindividualid__c
    ,cast(esm_ietonyuukaiDay__c as timestamp) as esm_ietonyuukaiDay__c
    ,esm_ietomemberno__c
    ,'esm' as department
from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_final__dll
where
	esm_ietomemberno__c is not null
	and esm_ietonyuukaiflag__c = 'あり'
	and esm_ietotaikaiday__c is not null
	and cast(esm_ietonyuukaiday__c as date) < '2021-02-19'
	and esm_dmkahi__c is null
	and esm_ietodmkahi__c = '可'
	and esm_mailaddress1__c is not null

UNION ALL

select
    unifiedindividualid__c
    ,cast(kintone_buy_tourokuday__c as timestamp) as kintone_buy_tourokuday__c
    ,kintone_buy_id__c
    ,'chukai_buy' as department
from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_final__dll
where kintone_Buy_KojinOrHoujin__c = '個人'
	and (kintone_buy_emailpc__c is not null or kintone_buy_emailkeitai__c is not null)
	and kintone_Buy_Tenpo__c not in ('本社','ソリューション','法人営業')
	and kintone_buy_mailmagazinefuka__c is null
UNION ALL
select
    unifiedindividualid__c
    ,cast(kintone_sales_tourokudate__c as timestamp) as kintone_sales_tourokudate__c
    ,kintone_sale_id__c
    ,'chukai_sale' as department
from odakyu-sfcdp-export.listing_bigquery_cld_dev_bigquery_cld_dev.UnifiedContact_final__dll
where kintone_sales_KojinOrHoujin__c = '個人'
        and (kintone_sales_emailpc__c is not null or kintone_sales_emailkeitai__c is not null)
        and kintone_sales_Tenpo__c not in ('本社','ソリューション','法人営業')
        and kintone_sales_mailmagazinefuka__c is null
;