/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 905_claims_avg_count_macro.sas  
 Project: MACBIS Task 2
 Purpose: Defines and calls average count macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          
 
 Author:  Richard Chapman
 Date Created: 3/1/2017
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.

 ******************************************************************************************/

  
%macro average_count_cll_to_clh(
  measure_id=,
  claim_cat=,
  line_constraint=,  
  claim_type=
  );
  
  /*step 1: roll up from line level to header level.
            cannot just use the pre-created header level file
            because the constraints require us to check to
            see if the claim has at least one line that
            meets certain contraints

            ok to take the max of the claim_cat binaries and the
            variable that we are going to average because these
            values do not vary within claim. (Could similarly take
            min or avg if we wanted to and would get same result).  */
  
  
  execute(
    create or replace temporary view &taskprefix._clh as

	select tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,max(claim_cat_&claim_cat.) as claim_cat_&claim_cat.
	  ,sum(case when (&line_constraint.) and claim_cat_&claim_cat.=1 then 1 else 0 end) as numer_line_count      	  

    from &temptable..&taskprefix._base_cll_&claim_type.

	group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;

  /*step 2: take average at the header level among claims
            that meet all denominator criteria*/

  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'905' as submodule,
    	coalesce(numer,0) as numer,
    	coalesce(denom,0) as denom,
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
	     sum(case when (numer_line_count>=1) and (claim_cat_&claim_cat. = 1) then 1                else 0 end) as denom  
	    ,sum(case when (numer_line_count>=1) and (claim_cat_&claim_cat. = 1) then numer_line_count else 0 end) as numer				
	  from #temp.&taskprefix._clh
	) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend average_count_cll_to_clh;

/******************************************************************************************

 ******************************************************************************************/
 

%macro run_905_all_avg_count;    

* ffs1.32, ffs3.20, mcr1.20, mcr3.20;
  %average_count_cll_to_clh(measure_id=ffs1_32, claim_cat=A, line_constraint=%str(SUBSTRING(REV_CD,1,2)='01'  or   SUBSTRING(REV_CD,1,3)     in ('020','021')), claim_type=IP);
  %average_count_cll_to_clh(measure_id=ffs3_20, claim_cat=F, line_constraint=%str(SUBSTRING(REV_CD,1,2)='01'  or   SUBSTRING(REV_CD,1,3)     in ('020','021')), claim_type=IP);                                                                                                                                                                                 
  %average_count_cll_to_clh(measure_id=mcr1_20, claim_cat=P, line_constraint=%str(SUBSTRING(REV_CD,1,2)='01'  or   SUBSTRING(REV_CD,1,3)     in ('020','021')), claim_type=IP);
  %average_count_cll_to_clh(measure_id=mcr3_20, claim_cat=R, line_constraint=%str(SUBSTRING(REV_CD,1,2)='01'  or   SUBSTRING(REV_CD,1,3)     in ('020','021')), claim_type=IP);
  
* ffs1.33, ffs3.21, mcr1.21, mcr3.21;  
  %average_count_cll_to_clh(measure_id=ffs1_33, claim_cat=A, line_constraint=%str(REV_CD between '0220' and '0998'), claim_type=IP);
  %average_count_cll_to_clh(measure_id=ffs3_21, claim_cat=F, line_constraint=%str(REV_CD between '0220' and '0998'), claim_type=IP);
  %average_count_cll_to_clh(measure_id=mcr1_21, claim_cat=P, line_constraint=%str(REV_CD between '0220' and '0998'), claim_type=IP);  
  %average_count_cll_to_clh(measure_id=mcr3_21, claim_cat=R, line_constraint=%str(REV_CD between '0220' and '0998'), claim_type=IP);

%mend run_905_all_avg_count;

