/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro process_utl_all_pymnt(ftype, measure);


execute(
    create or replace temporary view &taskprefix._pymnt_hdr_recs_&ftype._aj as
    select

        /*unique keys and other identifiers*/
        submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
        ,adjstmt_ind
        ,max(case when (pymt_lvl_ind=1) then 1 else 0 end) as &measure.

    from &temptable..&taskprefix._base_clh_&ftype.
    where claim_cat_aj = 1
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,adjstmt_ind 
	) by tmsis_passthrough;

    /*now summing to get values for state and month*/

        execute(

            insert into &utl_output
            select
            
            submtg_state_cd
            , %tslit(&measure)
            , '713'
            ,sum(&measure.)
            ,count(submtg_state_cd)
            ,round((sum(&measure.) / count(submtg_state_cd)),2)
            , null
            , null
            
            from #temp.&taskprefix._pymnt_hdr_recs_&ftype._aj
            group by submtg_state_cd
            ) by tmsis_passthrough;

        %insert_msr(msrid=&measure);

%mend;

%macro process_w_clm_counts;

    execute(

        insert into &utl_output
        
        select
        submtg_state_cd
        , 'all27_1'
        , '713'
        ,sum(case when (clm_type_cd='4') then 1 else 0 end) 
        , null
        , null
        , null
        , null
        from &temptable..&taskprefix._base_clh_ot
        where claim_cat_w = 1
        group by submtg_state_cd
		) by tmsis_passthrough;

    execute(

        insert into &utl_output

        select
        submtg_state_cd
        , 'all27_2'
        , '713'        
        ,sum(case when (clm_type_cd='5') then 1 else 0 end) 
        , null
        , null
        , null
        , null
        from &temptable..&taskprefix._base_clh_ot
        where claim_cat_w = 1
        group by submtg_state_cd
        ) by tmsis_passthrough;

        %insert_msr(msrid=all27_1);
        %insert_msr(msrid=all27_2);
    
%mend;

%macro utl_all_pymnt_aj_w_sql();
  %process_utl_all_pymnt(ip, all26_1);
  %process_utl_all_pymnt(lt, all26_2);
  %process_utl_all_pymnt(ot, all26_3);
  %process_utl_all_pymnt(rx, all26_4);

  %process_w_clm_counts;
%mend;
