/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 916_claims_luhn_check.sas  
 Project: MACBIS Task 12 v1.7
 Purpose: %of records that have an invalid BILLING PROV TAXONOMY
          for module 900 (ffs and managed care)
          Designed to be %included in module level driver         

 
 Author:  Jacqueline Agufa
 Date Created: 5/2/2019
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.
 

 Modifications:
 5/2/19  
 ******************************************************************************************/
 
 /******************************************************************************************
 
  ******************************************************************************************/
  
%macro run_luhn(
  measure_id=,
  claim_cat=,
  claim_type=
  );
  
  /*step 1: */
  
  execute(
    create or replace temporary view &taskprefix._denom as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,blg_prvdr_npi_num
      ,1 as has_denom      	  

    from &temptable..&taskprefix._base_clh_&claim_type.
	where %not_missing_1(blg_prvdr_npi_num,10) and claim_cat_&claim_cat. =1

  )by tmsis_passthrough;

 
  /*step 2: */

  
  execute(
    insert into &wrktable..&taskprefix._clms_900b
	  select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'916' as submodule,
		coalesce(numer,0),
		coalesce(denom,0),
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	    select
	            sum(case when (has_denom=1)                   then 1 else 0 end) as denom
	           ,sum(case when (has_denom=1) and (has_numer=1) then 1 else 0 end) as numer
	    from 
          (
        	select *
            ,case when substring(blg_prvdr_npi_num,1,1)<>'1' or &permview..mpr_valid_npi(blg_prvdr_npi_num)=0 then 1 else 0 end as has_numer      	  
           from 
           #temp.&taskprefix._denom 
         ) a            
	  )	b
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend run_luhn;

/******************************************************************************************
  macro containing all calls to create measures. this macro gets run in the module driver
 ******************************************************************************************/
 

%macro run_916_claims_luhn_check;

  *ffs50.5, ffs51.5, mcr60.5, mcr61.5;
  %run_luhn(measure_id=ffs50_5,  claim_cat=c, claim_type=ip);
  %run_luhn(measure_id=ffs51_5,  claim_cat=i, claim_type=ip);
  %run_luhn(measure_id=mcr60_5,  claim_cat=o, claim_type=ip);
  %run_luhn(measure_id=mcr61_5,  claim_cat=u, claim_type=ip);
  
  *ffs50.6, ffs51.6, mcr60.6, mcr61.6;
  %run_luhn(measure_id=ffs50_6,  claim_cat=c, claim_type=lt);
  %run_luhn(measure_id=ffs51_6,  claim_cat=i, claim_type=lt);
  %run_luhn(measure_id=mcr60_6,  claim_cat=o, claim_type=lt);
  %run_luhn(measure_id=mcr61_6,  claim_cat=u, claim_type=lt);
  
  *ffs50.7, ffs51.7, mcr60.7, mcr61.7;
  %run_luhn(measure_id=ffs50_7,  claim_cat=c, claim_type=ot);
  %run_luhn(measure_id=ffs51_7,  claim_cat=i, claim_type=ot);
  %run_luhn(measure_id=mcr60_7,  claim_cat=o, claim_type=ot);
  %run_luhn(measure_id=mcr61_7,  claim_cat=u, claim_type=ot);
  
  *ffs50.8, ffs51.8, mcr60.8, mcr61.8;
  %run_luhn(measure_id=ffs50_8,  claim_cat=c, claim_type=rx);
  %run_luhn(measure_id=ffs51_8,  claim_cat=i, claim_type=rx);
  %run_luhn(measure_id=mcr60_8,  claim_cat=o, claim_type=rx);  
  %run_luhn(measure_id=mcr61_8,  claim_cat=u, claim_type=rx);


%mend run_916_claims_luhn_check;
