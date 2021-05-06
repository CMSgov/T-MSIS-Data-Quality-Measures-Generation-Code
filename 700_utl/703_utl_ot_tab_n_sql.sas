
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ot_n_sql();

execute(
    create or replace temporary view &taskprefix._ot_prep_line as
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
		,line_adjstmt_ind
        ,bnft_type_cd
	
        /*standard measures calculated over all claim lines*/
        /*denominator is at claim header level, but numer at claim line level*/
        ,case when (bnft_type_cd in ('002','003','004','005','008','009','010',
                    '011','012','013','015','016','017')) then 1 else 0 end as rstrct_all1_1
        ,case when (bnft_type_cd in ('018','019','020',
                    '021','022','023','024','025','026','027','028','029','030',
                    '031','032','033','034','035','036',
                    '042','043','044','045','046','047',
                    '051','053','054','055')) then 1 else 0 end as rstrct_all1_4

        ,case when (bnft_type_cd = '002') then 1 else 0 end as all1_7
        ,case when (bnft_type_cd = '010') then 1 else 0 end as all1_8
        ,case when (bnft_type_cd in('007','041')) then 1 else 0 end as all1_9
        ,case when (bnft_type_cd in ('002','003','004','005','008','009','010',
                    '011','012','013','015','016','017','018','019','020',
                    '021','022','023','024','025','026','027','028','029','030',
                    '031','032','033','034','035','036',
                    '042','043','044','045','046','047','048',
                    '051','053','054','055')) then 1 else 0 end as all1_12

    from &temptable..&taskprefix._base_cll_ot
    where claim_cat_n = 1
	and childless_header_flag = 0
	) by tmsis_passthrough;


    /*rolling up to unique claim header level*/
    /*therefore, taking max value of indicator across claim lines*/
	execute(
    create or replace temporary view &taskprefix._ot_rollup_line as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,orgnl_line_num 
        ,adjstmt_line_num
        ,line_adjstmt_ind
        ,max(all1_7) as all1_7
        ,max(all1_8) as all1_8
        ,max(all1_9) as all1_9
        ,max(all1_12) as all1_12

    from &taskprefix._ot_prep_line
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
    ) by tmsis_passthrough;


    /*now summing to get values for state and month*/
execute(
    create or replace temporary view &taskprefix._ot_clm_n as
    select
         submtg_state_cd
        ,sum(all1_7) as all1_7_numer
        ,count(submtg_state_cd) as all1_7_denom
        ,round((sum(all1_7) / count(submtg_state_cd)),2) as all1_7
        ,sum(all1_8) as all1_8_numer
        ,count(submtg_state_cd) as all1_8_denom
        ,round((sum(all1_8) / count(submtg_state_cd)),2) as all1_8
        ,sum(all1_9) as all1_9_numer
        ,count(submtg_state_cd) as all1_9_denom
        ,round((sum(all1_9) / count(submtg_state_cd)),2) as all1_9
        ,sum(all1_12) as all1_12_numer
        ,count(submtg_state_cd) as all1_12_denom
        ,round((sum(all1_12) / count(submtg_state_cd)),2) as all1_12


    from &taskprefix._ot_rollup_line
    group by submtg_state_cd
	) by tmsis_passthrough;

 /**new measure - header count **/
execute(
    create or replace temporary view &taskprefix._ot_rollup_hdr as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max( case when (pgm_type_cd in ('01','02','04')) then 1 else 0 end) as all12_1

    from &temptable..&taskprefix._base_clh_ot
	where claim_cat_n = 1
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ot_clm_hdr_n as
    select
         submtg_state_cd
        ,sum(all12_1) as all12_1_numer
        ,count(submtg_state_cd) as all12_1_denom
        ,round((sum(all12_1) / count(submtg_state_cd)),2) as all12_1

    from &taskprefix._ot_rollup_hdr 
    group by submtg_state_cd
	) by tmsis_passthrough;


execute(
    create or replace temporary view &taskprefix._ot_all1_1 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_1

    from &taskprefix._ot_prep_line
    where rstrct_all1_1 = 1 and
          %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ot_all1_4 as
    select
         submtg_state_cd
        ,count(distinct bnft_type_cd) as all1_4

    from &taskprefix._ot_prep_line
    where rstrct_all1_4 = 1 and
	      %nmsng(bnft_type_cd,3)
    group by submtg_state_cd
	) by tmsis_passthrough;


execute(
    insert into &utl_output
    
    select
    submtg_state_cd
        , 'all1_7'
        , '703'
    ,all1_7_numer
    ,all1_7_denom
    ,all1_7
        , null
        , null
    from #temp.&taskprefix._ot_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_8'
        , '703'
    ,all1_8_numer
    ,all1_8_denom
    ,all1_8
        , null
        , null
    from #temp.&taskprefix._ot_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_9'
        , '703'
    ,all1_9_numer
    ,all1_9_denom
    ,all1_9
        , null
        , null
    from #temp.&taskprefix._ot_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_12'
        , '703'
    ,all1_12_numer
    ,all1_12_denom
    ,all1_12
        , null
        , null
    from #temp.&taskprefix._ot_clm_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all12_1'
        , '703'
    ,all12_1_numer
    ,all12_1_denom
    ,all12_1
        , null
        , null
    from #temp.&taskprefix._ot_clm_hdr_n

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
        , 'all1_1'
        , '703'
    ,all1_1 
        , null
        , null
        , null
        , null
    from #temp.&taskprefix._ot_all1_1

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
        , 'all1_4'
        , '703'
    ,all1_4 
        , null
        , null
        , null
        , null
    from #temp.&taskprefix._ot_all1_4

    ) by tmsis_passthrough;

    %insert_msr(msrid=all1_1);
    %insert_msr(msrid=all1_4);
    %insert_msr(msrid=all1_7);
    %insert_msr(msrid=all1_8);
    %insert_msr(msrid=all1_9);
    %insert_msr(msrid=all1_12);
    %insert_msr(msrid=all12_1);

%mend;


