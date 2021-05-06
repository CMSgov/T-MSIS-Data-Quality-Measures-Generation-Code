

/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ip_n_sql();

execute(
    create or replace temporary view &taskprefix._ip_prep_line as
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
        ,case when (bnft_type_cd in ('001','014')) then 1 else 0 end as rstrct_all1_3
        ,case when (bnft_type_cd in ('052')) then 1 else 0 end as rstrct_all1_6
        ,case when (bnft_type_cd = '007') then 1 else 0 end as all1_11
        ,case when (bnft_type_cd in ('001','014','052') ) then 1 else 0 end as all1_14

    from &temptable..&taskprefix._base_cll_ip
    where claim_cat_n = 1
	) by tmsis_passthrough;


    /*rolling up to unique claim header level*/
    /*therefore, taking max value of indicator across claim lines*/
execute(
    create or replace temporary view &taskprefix._ip_rollup_line as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max(all1_11) as all1_11
        ,max(all1_14) as all1_14

    from &taskprefix._ip_prep_line
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	) by tmsis_passthrough;


    /*now summing to get values for state and month*/
    execute(
    create or replace temporary view &taskprefix._ip_clm_n as
    select
         submtg_state_cd
        ,sum(all1_11) as all1_11_numer
        ,count(submtg_state_cd) as all1_11_denom
        ,round((sum(all1_11) / count(submtg_state_cd)),2) as all1_11
        ,sum(all1_14) as all1_14_numer
        ,count(submtg_state_cd) as all1_14_denom
        ,round((sum(all1_14) / count(submtg_state_cd)),2) as all1_14
    from &taskprefix._ip_rollup_line
    group by submtg_state_cd
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ip_all1_3 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_3

    from &taskprefix._ip_prep_line
    where rstrct_all1_3 = 1 and
	      %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ip_all1_6 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_6

    from &taskprefix._ip_prep_line
    where rstrct_all1_6 = 1 and
	      %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select
        submtg_state_cd
        , 'all1_11' 
        , '703' 
        , all1_11_numer
        , all1_11_denom
        , all1_11
        , null
        , null
        from #temp.&taskprefix._ip_clm_n

         ) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all1_14' 
        , '703' 
        , all1_14_numer
        , all1_14_denom
        , all1_14
        , null
        , null
        from #temp.&taskprefix._ip_clm_n

         ) by tmsis_passthrough; 

     execute (
        insert into &utl_output

        select
        submtg_state_cd
        , 'all1_3'
        , '703'
        , all1_3
        , null
        , null
        , null
        , null
        from #temp.&taskprefix._ip_all1_3
       
         ) by tmsis_passthrough; 

      execute (
        insert into &utl_output

        select
        submtg_state_cd
        , 'all1_6'
        , '703'
        , all1_6
        , null
        , null
        , null
        , null
        from #temp.&taskprefix._ip_all1_6

        ) by tmsis_passthrough;

    %insert_msr(msrid=all1_3);
    %insert_msr(msrid=all1_6);
    %insert_msr(msrid=all1_11);
    %insert_msr(msrid=all1_14);
%mend;
