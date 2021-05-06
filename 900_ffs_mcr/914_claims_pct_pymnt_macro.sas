/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 914_mcr_claims_pct_pymnt_macro.sas  
 Project: MACBIS Task 12 v1.6
 Purpose: %of claim lines with PAYMENT-LEVEL-IND=2 and Medicaid paid amount 
          greater than the allowed amount
          for module 900 (ffs and managed care)
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
  
%macro pct_pymnt(
  measure_id=,
  claim_cat=,
  claim_type=
  );

  
  /*step 1: Line level records  
     (NOTE: Keep at line level, do not group by keys to get to claim header level)
   */
  
  execute(
    create or replace temporary view &taskprefix._cll as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,case when (mdcd_pd_amt > alowd_amt ) then 1 else 0 end as has_cll_numer
	  ,1 as has_cll_denom      	  
    from &temptable..&taskprefix._base_cll_&claim_type.
	where (mdcd_pd_amt is not null and alowd_amt is not null  and alowd_amt <> 0) 
		and claim_cat_&claim_cat. =1
		and childless_header_flag = 0

  )by tmsis_passthrough;

  /*step 2: header level with Payment-Level-Ind=2
  */ 
  execute(
    create or replace temporary view &taskprefix._clh as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,1 as has_clh_denom

    from &temptable..&taskprefix._base_clh_&claim_type.
	where pymt_lvl_ind='2' and claim_cat_&claim_cat. =1

  )by tmsis_passthrough;
  

  /*step 3: take pct
   */  
  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'914' as submodule, 
		coalesce(numer,0),
		coalesce(denom,0), 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
	     sum(case when (has_cll_denom = 1 and has_clh_denom = 1 ) then 1        else 0 end) as denom  
	    ,sum(case when (has_cll_denom = 1 and has_clh_denom = 1 and has_cll_numer = 1 ) then 1 else 0 end) as numer
				
	  from #temp.&taskprefix._clh as b 
          inner join #temp.&taskprefix._cll as a
           on a.orgnl_clm_num  =b.orgnl_clm_num and
              a.adjstmt_clm_num=b.adjstmt_clm_num and
              a.adjdctn_dt     =b.adjdctn_dt and
              a.adjstmt_ind    =b.adjstmt_ind

        ) c
     ) by tmsis_passthrough;

	*add measure to list of measures in SAS dataset;
  	insert into dqout.measures_900 
  	values("&measure_id.",null);

%mend pct_pymnt;

/******************************************************************************************

 ******************************************************************************************/
 

%macro run_914_claims_pct_pymnt;

  %pct_pymnt(measure_id=ffs49_9, claim_cat=AE, claim_type=IP);
  %pct_pymnt(measure_id=mcr59_9, claim_cat=AF, claim_type=IP);

  %pct_pymnt(measure_id=ffs49_10, claim_cat=AE, claim_type=LT);
  %pct_pymnt(measure_id=mcr59_10, claim_cat=AF, claim_type=LT);

  %pct_pymnt(measure_id=ffs49_11, claim_cat=AE, claim_type=OT);
  %pct_pymnt(measure_id=mcr59_11, claim_cat=AF, claim_type=OT);

  %pct_pymnt(measure_id=ffs49_12, claim_cat=AE, claim_type=RX);
  %pct_pymnt(measure_id=mcr59_12, claim_cat=AF, claim_type=RX);

%mend run_914_claims_pct_pymnt;

