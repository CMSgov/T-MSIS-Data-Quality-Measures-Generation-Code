/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 917_mcr_claims_pct_bill_type_cd.sas  
 Project: MACBIS Task 12 v1.6
 Purpose: % of claims where TYPE-OF-BILL does not begin with ..... specified codes like 
          011(inpatient hospital)
          for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          
 
 Author:  Jacqueline Agufa
 Date Created: 5/29/2019
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
  
%macro pct_calc(
  measure_id=,
  bill_type_cond=,
  claim_cat=,
  claim_type=
  );
 
  
  execute(
    create or replace temporary view &taskprefix._cll as
	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,1 as has_cll_denom      	  

    from &temptable..&taskprefix._base_cll_&claim_type.
	where claim_cat_&claim_cat. =1
	and childless_header_flag = 0

  )by tmsis_passthrough;

  /*step 2: header level with Payment-Level-Ind=2
  */

  execute(
    create or replace temporary view &taskprefix._clh as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,case when (&bill_type_cond. ) then 1 else 0 end as has_clh_numer
	  ,1 as has_clh_denom

    from &temptable..&taskprefix._base_clh_&claim_type.
	where %not_missing_1(BILL_TYPE_CD,4) and claim_cat_&claim_cat. =1

  )by tmsis_passthrough;
  

  /*step 3: take pct
   */
 
  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'917' as submodule, 
		coalesce(numer,0),
		coalesce(denom,0), 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
	     sum(case when (has_cll_denom = 1 and has_clh_denom = 1 ) then 1        else 0 end) as denom  
	    ,sum(case when (has_cll_denom = 1 and has_clh_denom = 1 and has_clh_numer = 1 ) then 1 else 0 end) as numer
				
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

%mend pct_calc;

/******************************************************************************************

 ******************************************************************************************/
 

%macro run_917_claims_pct_blltypcd_OT;

  %pct_calc(measure_id=ffs52_7,  
            bill_type_cond=%quote(substring(BILL_TYPE_CD,1,2) not in ('03','07', '08') and
                                  substring(BILL_TYPE_CD,1,3) not in ('012', '013', '014', '022', '023', '024')), 
            claim_cat=AK, claim_type=ot);

  %pct_calc(measure_id=mcr62_7,  
            bill_type_cond=%quote(substring(BILL_TYPE_CD,1,2) not in ('03','07', '08') and
                                  substring(BILL_TYPE_CD,1,3) not in ('012', '013', '014', '022', '023', '024')), 
            claim_cat=AL, claim_type=ot);

%mend run_917_claims_pct_blltypcd_OT;

