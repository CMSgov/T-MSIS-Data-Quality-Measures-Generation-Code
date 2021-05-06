/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 913_claims_pct_amt_macro.sas  
 Project: MACBIS Task 12 v1.6
 Purpose: %of claims where the sum of Medicaid paid aount from the lines does not equal total
          Medicaid paid amount from the header for module 900 (ffs and managed care)
          Designed to be %included in module level driver         

 
 Author:  Jacqueline Agufa
 Date Created: 1/24/2019
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.

 Modifications:
 1/24/19  
 ******************************************************************************************/
 
 /******************************************************************************************
 
  ******************************************************************************************/
  
%macro pct_count_cll_to_clh(
  measure_id=,
  claim_cat=,
  var_l=,  
  var_h=,  
  claim_type=
  );
  
  /*step 1: Sum amounts across line level records 
   */
  execute(
    create or replace temporary view &taskprefix._cll as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	  ,sum(case when (&var_l. is not null) then &var_l. else 0 end) as sum_&var_l.      	  

    from &temptable..&taskprefix._base_cll_&claim_type.
    where claim_cat_&claim_cat. =1
	and childless_header_flag = 0
	group by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;

  /*step 2: Get amount from the header level
  */
  execute(
    create or replace temporary view &taskprefix._clh as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	  ,max( coalesce(&var_h. ,0) ) as &var_h.      	  

    from &temptable..&taskprefix._base_clh_&claim_type.
    where claim_cat_&claim_cat. =1
	group by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;
  

  /*step 3: take average at the header level among claims that meet all denominator criteria
   */

  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'913' as submodule,
		coalesce(numer,0),
		coalesce(denom,0), 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
	     sum(1) as denom  
	    ,sum(case when (sum_&var_l. <> &var_h.)  then 1 else 0 end) as numer	
          from #temp.&taskprefix._clh a inner join
               #temp.&taskprefix._cll b
           on a.orgnl_clm_num  =b.orgnl_clm_num and
              a.adjstmt_clm_num=b.adjstmt_clm_num and
              a.adjdctn_dt     =b.adjdctn_dt and
              a.adjstmt_ind    =b.adjstmt_ind
        ) c
     ) by tmsis_passthrough;

	*add measure to list of measures in SAS dataset;
  	insert into dqout.measures_900 
  	values("&measure_id.",null);

%mend pct_count_cll_to_clh;

/******************************************************************************************

 ******************************************************************************************/


%macro run_913_claims_pct_amt;

  %pct_count_cll_to_clh(measure_id=ffs49_1, claim_cat=AE, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=IP);
  %pct_count_cll_to_clh(measure_id=mcr59_1, claim_cat=AF, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=IP);

  %pct_count_cll_to_clh(measure_id=ffs49_2, claim_cat=AE, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=LT);
  %pct_count_cll_to_clh(measure_id=mcr59_2, claim_cat=AF, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=LT);

  %pct_count_cll_to_clh(measure_id=ffs49_3, claim_cat=AE, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=OT);
  %pct_count_cll_to_clh(measure_id=mcr59_3, claim_cat=AF, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=OT);

  %pct_count_cll_to_clh(measure_id=ffs49_4, claim_cat=AE, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=RX);
  %pct_count_cll_to_clh(measure_id=mcr59_4, claim_cat=AF, var_l=MDCD_PD_AMT, var_h=TOT_MDCD_PD_AMT, claim_type=RX);

%mend run_913_claims_pct_amt;
