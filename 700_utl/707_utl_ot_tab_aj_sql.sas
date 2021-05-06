/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ot_aj_sql();

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
        ,bill_type_cd
		,prcdr_cd
		,srvc_plc_cd
		,rev_cd
		,hcpcs_srvc_cd
		,wvr_id
		,case when %nmisslogic(srvc_plc_cd) then 1 else 0 end  as nmsng_pos /**8/9 fills are not considered missing. Is that what we want?**/
	    ,case when (%nmisslogic(srvc_plc_cd) and  %misslogic(prcdr_cd)) then 1 else 0 end as nmsng_pos_msng_prcdr_cd
	    ,case when (%nmisslogic(bill_type_cd) and %nmisslogic(srvc_plc_cd)) then 1 else 0 end as nmsng_pos_bill_type
	    ,case when (%misslogic(bill_type_cd)  and %misslogic(srvc_plc_cd)) then 1 else 0 end as msng_bill_pos_type
	    ,case when %nmisslogic(bill_type_cd) then 1 else 0 end  as nmsng_bill 
	    ,case when (%nmisslogic(bill_type_cd) and %misslogic(rev_cd)) then 1 else 0 end as nmsng_bill_msng_rev 
	    ,case when %nmisslogic(rev_cd) then 1 else 0 end  as nmsng_rev 
	    ,case when (%nmisslogic(rev_cd) and %misslogic(bill_type_cd)) then 1 else 0 end as nmsng_rev_msng_bill_type 
	    ,case when (%misslogic(prcdr_cd) and %misslogic(rev_cd)) then 1 else 0 end as msng_prcdr_rev
       	,case when hcpcs_srvc_cd ='4'                           then 1 else 0 end as hcbs_eq4
       	,case when hcpcs_srvc_cd ='4' and (%misslogic(wvr_id) ) then 1 else 0 end as hcbs_eq4_msng_wvr_id

    from &temptable..&taskprefix._base_cll_ot
    where claim_cat_AJ = 1
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
        ,max(nmsng_pos) as ALL15_1_denom0
        ,max(nmsng_pos_msng_prcdr_cd) as ALL15_1_numer0
		,max(nmsng_pos_bill_type) as ALL15_2_numer0
		,max(msng_bill_pos_type) as ALL15_3_numer0
        ,max(nmsng_bill) as ALL15_4_denom0
		,max(nmsng_bill_msng_rev) as ALL15_4_numer0
 		,max(nmsng_rev) as ALL15_5_denom0
		,max(nmsng_rev_msng_bill_type) as ALL15_5_numer0
 		,max(msng_prcdr_rev) as ALL15_6_numer0
       
    from &taskprefix._ot_prep_line
    group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
    ) by tmsis_passthrough;

    /*now summing to get values for state and month*/
execute(
    create or replace temporary view &taskprefix._ot_clm_aj as
    select
		 a.*
	    ,case when ALL15_1_denom >0 then round((ALL15_1_numer / ALL15_1_denom),3) 
               else null end as  ALL15_1
        ,case when ALL15_2_denom >0 then round((ALL15_2_numer / ALL15_2_denom),3)
              else null end as ALL15_2
        ,case when ALL15_3_denom >0 then round((ALL15_3_numer / ALL15_3_denom),3)
              else null end as ALL15_3
	    ,case when ALL15_4_denom >0 then round((ALL15_4_numer / ALL15_4_denom),3) 
               else null end as  ALL15_4
	    ,case when ALL15_5_denom >0 then round((ALL15_5_numer / ALL15_5_denom),3) 
               else null end as  ALL15_5
        ,case when ALL15_6_denom >0 then round((ALL15_6_numer / ALL15_6_denom),3) 
               else null end as  ALL15_6
	from
		(select
			 submtg_state_cd
	        ,sum(ALL15_1_denom0) as ALL15_1_denom
			,sum(ALL15_1_numer0) as ALL15_1_numer
	        ,count(submtg_state_cd) as ALL15_2_denom
			,sum(ALL15_2_numer0) as ALL15_2_numer	
	        ,count(submtg_state_cd) as ALL15_3_denom
			,sum(ALL15_3_numer0) as ALL15_3_numer	
	        ,sum(ALL15_4_denom0) as ALL15_4_denom
			,sum(ALL15_4_numer0) as ALL15_4_numer
	        ,sum(ALL15_5_denom0) as ALL15_5_denom
			,sum(ALL15_5_numer0) as ALL15_5_numer
	    	,count(submtg_state_cd) as ALL15_6_denom
	        ,sum(ALL15_6_numer0) as ALL15_6_numer
	    from &taskprefix._ot_rollup_line
	    group by submtg_state_cd
		) a
	) by tmsis_passthrough;



/******Header level measure 19.1 ******/

execute(
    create or replace temporary view &taskprefix._ot_hcpcs_clm_aj as
    select 
	     submtg_state_cd
		 ,sum(hcbs_eq4)             as ALL19_1_denom
         ,sum(hcbs_eq4_msng_wvr_id) as ALL19_1_numer
	     ,case when sum(hcbs_eq4) >0 then round((sum(hcbs_eq4_msng_wvr_id) /sum(hcbs_eq4)),3) 
               else null end as  ALL19_1
      
   from
     /*unique keys and other identifiers*/
       (select
			submtg_state_cd
        	,tmsis_rptg_prd
        	,orgnl_clm_num
        	,adjstmt_clm_num
        	,adjdctn_dt
			,adjstmt_ind
			,max(hcbs_eq4) as hcbs_eq4
        	,max(hcbs_eq4_msng_wvr_id) as hcbs_eq4_msng_wvr_id

    	from &taskprefix._ot_prep_line
       group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, 
                adjstmt_clm_num, adjdctn_dt, adjstmt_ind
            ) a

	group by submtg_state_cd
	) by tmsis_passthrough;


execute(

    insert into &utl_output

    select
    submtg_state_cd
    , 'all15_1'
    , '707'
    ,ALL15_1_numer
    ,ALL15_1_denom
    ,ALL15_1
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
    , 'all15_2'
    , '707'
    ,ALL15_2_numer
    ,ALL15_2_denom
    ,ALL15_2
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj
		
     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all15_3'
    , '707'
    ,ALL15_3_numer
    ,ALL15_3_denom
    ,ALL15_3
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj
		
     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all15_4'
    , '707'
    ,ALL15_4_numer
    ,ALL15_4_denom
    ,ALL15_4
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj
		
     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all15_5'
    , '707'
    ,ALL15_5_numer
    ,ALL15_5_denom
    ,ALL15_5
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj
		
     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all15_6'
    , '707'
    ,ALL15_6_numer
    ,ALL15_6_denom
    ,ALL15_6
    , null
    , null    
    from      #temp.&taskprefix._ot_clm_aj

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all19_1'
    , '707'
    ,ALL19_1_numer
    ,ALL19_1_denom
    ,ALL19_1
    , null
    , null    
    from #temp.&taskprefix._ot_hcpcs_clm_aj

    ) by tmsis_passthrough;

    %insert_msr(msrid=all15_1);
    %insert_msr(msrid=all15_2);
    %insert_msr(msrid=all15_3);
    %insert_msr(msrid=all15_4);
    %insert_msr(msrid=all15_5);
    %insert_msr(msrid=all15_6);
    %insert_msr(msrid=all19_1);

%mend;


