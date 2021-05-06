
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_lt_ab_ac_sql();

%macro utl_lt_ab_ac_clm(clmcat, tblnum);

execute(
create or replace temporary view &taskprefix._lt_prep_clm_&clmcat. as
    select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm

    from &temptable..&taskprefix._base_clh_lt
	where claim_cat_&clmcat. = 1
	group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind    
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._lt_clm_&clmcat. as
    select
         submtg_state_cd
        ,sum(xover_clm) as all&tblnum._1_numer
        ,count(submtg_state_cd) as all&tblnum._1_denom
        ,round((sum(xover_clm) / count(submtg_state_cd)),2) as all&tblnum._1

    from &taskprefix._lt_prep_clm_&clmcat.
    group by submtg_state_cd
	) by tmsis_passthrough;

%mend utl_lt_ab_ac_clm;

%utl_lt_ab_ac_clm(ab, 7);
%utl_lt_ab_ac_clm(ac, 10);

execute(
    insert into &utl_output

    select
         submtg_state_cd
            , 'all7_1'
            , '705'
        ,all7_1_numer
        ,all7_1_denom
        ,all7_1
            , null
            , null
    from #temp.&taskprefix._lt_clm_ab

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
         submtg_state_cd
            , 'all10_1'
            , '705'
        ,all10_1_numer
        ,all10_1_denom
        ,all10_1
           , null
            , null
    from #temp.&taskprefix._lt_clm_ac
    
	) by tmsis_passthrough;

     %insert_msr(msrid=all7_1);
     %insert_msr(msrid=all10_1);

%mend utl_lt_ab_ac_sql;



