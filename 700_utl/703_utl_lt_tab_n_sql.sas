/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_lt_n_sql();

execute(
    create or replace temporary view &taskprefix._lt_prep_line as
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
        ,case when (bnft_type_cd in ('006')) then 1 else 0 end as rstrct_all1_2
        ,case when (bnft_type_cd in ('037','038','039','040','049','050')) then 1 else 0 end as rstrct_all1_5
        ,case when (bnft_type_cd = '041') then 1 else 0 end as all1_10
        ,case when (bnft_type_cd in ('006','037','038','039','040','049','050')) then 1 else 0 end as all1_13

    from &temptable..&taskprefix._base_cll_lt
    where claim_cat_n = 1
	) by tmsis_passthrough;


    /*rolling up to unique claim header level*/
    /*therefore, taking max value of indicator across claim lines*/
execute(
    create or replace temporary view &taskprefix._lt_rollup_line as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max(all1_10) as all1_10
        ,max(all1_13) as all1_13

    from &taskprefix._lt_prep_line
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	) by tmsis_passthrough;


    /*now summing to get values for state and month*/
execute(
    create or replace temporary view &taskprefix._lt_clm_n as
    select
         submtg_state_cd
        ,sum(all1_10) as all1_10_numer
        ,count(submtg_state_cd) as all1_10_denom
        ,round((sum(all1_10) / count(submtg_state_cd)),2) as all1_10
        ,sum(all1_13) as all1_13_numer
        ,count(submtg_state_cd) as all1_13_denom
        ,round((sum(all1_13) / count(submtg_state_cd)),2) as all1_13

    from &taskprefix._lt_rollup_line
    group by submtg_state_cd
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._lt_all1_2 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_2
    from &taskprefix._lt_prep_line
    where rstrct_all1_2 = 1 and
	      %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
    order by submtg_state_cd
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._lt_all1_5 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_5

    from &taskprefix._lt_prep_line
    where rstrct_all1_5 = 1 and
	      %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
    order by submtg_state_cd
	) by tmsis_passthrough;

execute(
    insert into &utl_output
    
    select
    submtg_state_cd
        , 'all1_10'
        , '703'
    ,all1_10_numer
    ,all1_10_denom
    ,all1_10
        , null
        , null
    from #temp.&taskprefix._lt_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_13'
        , '703'
    ,all1_13_numer
    ,all1_13_denom
    ,all1_13
        , null
        , null
    from #temp.&taskprefix._lt_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
        , 'all1_2'
        , '703'
    ,all1_2
        , null
        , null
        , null
        , null
    from #temp.&taskprefix._lt_all1_2

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_5'
        , '703'
    ,all1_5
        , null
        , null
        , null
        , null
    from #temp.&taskprefix._lt_all1_5
    
    ) by tmsis_passthrough;

    %insert_msr(msrid=all1_2);
    %insert_msr(msrid=all1_5);
    %insert_msr(msrid=all1_10);
    %insert_msr(msrid=all1_13);
%mend;

