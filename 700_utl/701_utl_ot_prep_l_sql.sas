

/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ot_prep_sql();

execute(
    create or replace temporary view &taskprefix._ot_prep_clm_l as
    select
        /*unique keys and other identifiers*/
         submtg_state_cd
        ,tmsis_rptg_prd
        ,msis_ident_num
       /*standard measures calculated over all claim headers*/
        ,max(case when hcpcs_srvc_cd = '1' then 1 else 0 end) as ot_hcpcs_1
        ,max(case when hcpcs_srvc_cd = '2' then 1 else 0 end) as ot_hcpcs_2
		,max(case when hcpcs_srvc_cd = '5' then 1 else 0 end) as ot_hcpcs_5
        ,max(case when %nmsng(hcpcs_txnmy_cd,5) then 1 else 0 end) as ot_val_hcpcs_txnmy
    from &temptable..&taskprefix._base_cll_ot
    where claim_cat_l = 1
	and childless_header_flag = 0
	group by submtg_state_cd, tmsis_rptg_prd, msis_ident_num
	) by tmsis_passthrough;

execute(
    insert into &utl_output
    select

    submtg_state_cd
    , 'all2_1'
    , '701'
	, all2_1
	, null
	, null
	, null
	, null

	from (
	select submtg_state_cd
    ,count(distinct hcpcs_txnmy_cd) as all2_1
    from &temptable..&taskprefix._base_cll_ot
    where claim_cat_l = 1 and
	      childless_header_flag = 0 and
	    hcpcs_txnmy_cd in (
                '01010','02011','02012','02013','02021','02022',
				'02023','02031','02032','02033','03010','03021',
				'03022','03030','04010','04020','04030','04040',
				'04050','04060','04070','04080','05010','05020',
				'06010','07010','08010','08020','08030','08040',
				'08050','08060','09011','09012','09020','10010',
				'10020','10030','10040','10050','10060','10070',
				'10080','10090','11010','11020','11030','11040',
				'11050','11060','11070','11080','11090','11100',
				'11110','11120','11130','12010','12020','13010',
				'14010','14020','14031','14032','15010','16010',
				'17010','17020','17030','17990')    
    group by submtg_state_cd) a

	) by tmsis_passthrough;

    %insert_msr(msrid=all2_1);

%mend;



