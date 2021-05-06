/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_all_clms_ah_sql;

%macro utl_tab_ah_sql(fl,n);

execute(
    create or replace temporary view &taskprefix._&fl._prep_line_ah as
    select

        /*unique keys and &fl.her identifiers*/
        submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,orgnl_line_num
        ,adjstmt_line_num
		,line_adjstmt_ind
        ,xix_srvc_ctgry_cd 
		,xxi_srvc_ctgry_cd 
		,case when (%nmisslogic(xix_srvc_ctgry_cd) and %nmisslogic(xxi_srvc_ctgry_cd)) then 1 else 0 end as nmsng_xix_xxi_srvc_cd
	        
    from &temptable..&taskprefix._base_cll_&fl.
    where claim_cat_AH = 1
	and childless_header_flag = 0
	) by tmsis_passthrough;

    /*rolling up to unique claim header level*/
    /*therefore, taking max value of indicator across claim lines*/
	execute(
    create or replace temporary view &taskprefix._&fl._rollup_line_ah as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,orgnl_line_num 
        ,adjstmt_line_num
        ,line_adjstmt_ind
        ,max(nmsng_xix_xxi_srvc_cd) as ALL20_&n._numer0
    from &taskprefix._&fl._prep_line_ah
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
    ) by tmsis_passthrough;

    /*now summing to get values for state and month*/
execute(
    create or replace temporary view &taskprefix._&fl._clm_ah as
    select
		 a.*
	    ,case when ALL20_&n._denom >0 then round((ALL20_&n._numer / ALL20_&n._denom),3) 
               else null end as  ALL20_&n.
        
	from
		(select
			 submtg_state_cd
	        ,count(submtg_state_cd) as ALL20_&n._denom
			,sum(ALL20_&n._numer0)  as ALL20_&n._numer	
	       
		
	    from &taskprefix._&fl._rollup_line_ah
	    group by submtg_state_cd
		) a
	) by tmsis_passthrough;


execute(
    insert into &utl_output
    select
    submtg_state_cd
    , %tslit(all20_&n)
    , '709'
    ,ALL20_&n._numer
    ,ALL20_&n._denom
    ,ALL20_&n.
    , null
    , null        
    
    from      #temp.&taskprefix._&fl._clm_ah
    
    ) by tmsis_passthrough;

%insert_msr(msrid=all20_&n);


%mend;

%utl_tab_ah_sql(ip,1);
%utl_tab_ah_sql(lt,2);
%utl_tab_ah_sql(ot,3);
%utl_tab_ah_sql(rx,4);

%mend;
