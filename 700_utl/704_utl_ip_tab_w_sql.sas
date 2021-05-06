
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ip_w_sql();

execute(
    create or replace temporary view &taskprefix._ip_prep_w as
    select

        /*unique keys and other identifiers*/
        submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,orgnl_line_num
        ,adjstmt_line_num
        ,bnft_type_cd
        /*standard measures calculated over all claim lines*/
        /*denominator is at claim header level, but numer at claim line level*/

        ,case when (bnft_type_cd in ('001','014','052')) then 1 else 0 end as all3_1
        ,case when (hosp_type_cd in ('03','04','05','07','08')) then 1 else 0 end as all3_2

    from &temptable..&taskprefix._base_cll_ip
    where claim_cat_w = 1
	) by tmsis_passthrough;


    /*rolling up to unique claim header level*/
    /*therefore, taking max value of indicator across claim lines*/
execute(
    create or replace temporary view &taskprefix._ip_rollup_w as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max(all3_1) as all3_1
        ,max(all3_2) as all3_2

    from &taskprefix._ip_prep_w
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,adjstmt_ind 
	) by tmsis_passthrough;

    /*now summing to get values for state and month*/

execute(
    create or replace temporary view &taskprefix._utl_ip_w as
    select
         submtg_state_cd
        ,sum(all3_1) as all3_1_numer
        ,count(submtg_state_cd) as all3_1_denom
        ,round((sum(all3_1) / count(submtg_state_cd)),2) as all3_1
        ,sum(all3_2) as all3_2_numer
        ,count(submtg_state_cd) as all3_2_denom
        ,round((sum(all3_2) / count(submtg_state_cd)),2) as all3_2

    from &taskprefix._ip_rollup_w
    group by submtg_state_cd
	) by tmsis_passthrough;

        execute(
            insert into &utl_output
            
            select            
            submtg_state_cd
            , 'all3_1'
            , '704'
            ,all3_1_numer
            ,all3_1_denom
            ,all3_1
            , null
            , null
            from #temp.&taskprefix._utl_ip_w
        
			 ) by tmsis_passthrough; 

execute ( 
            insert into &utl_output 

            select
            submtg_state_cd
            , 'all3_2'
            , '704'
            ,all3_2_numer
            ,all3_2_denom
            ,all3_2
            , null
            , null            
            from #temp.&taskprefix._utl_ip_w
 
            ) by tmsis_passthrough;

     %insert_msr(msrid=all3_1);
     %insert_msr(msrid=all3_2);
        
%mend;
