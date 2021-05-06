/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 903_claims_avg_per_unit_macro.sas
 Project: MACBIS Task 2
 Purpose: Defines and calls average per unit macros for module 900 (ffs and managed care)
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
 
/******************************************************************************************
  Macro: average per unit 
 ******************************************************************************************/

%macro avg_per_unit(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,
  denom_count=,
  numer_sum=,
  level=,
  claim_type=
  );

  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 	  
		%str(%')&measure_id.%str(%') as measure_id,
		'903' as submodule, 
    	coalesce(numer,0) as numer,
    	coalesce(denom,0) as denom,
	    case when coalesce(denom,0) <> 0 then numer/denom else null end as mvalue,
		null as valid_value
    from (
		select
	     sum(case when (&denom_count.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as denom
	    ,sum(case when (&denom_count.) and (claim_cat_&claim_cat. = 1) then &numer_sum. else 0 end) as numer
	  from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	    where childless_header_flag = 0
	  %end;
	) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
 

%mend avg_per_unit;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/
 
%macro run_903_avg_per_unit;

  *ffs5.29, ffs7.19, mcr7.19, mcr5.20;
  %avg_per_unit(measure_id=ffs5_29,claim_cat=A,denom_count=%quote(&ltc_days. rlike '[1-9]'),numer_sum=&ltc_days.,level=CLH, claim_type=LT);
  %avg_per_unit(measure_id=ffs7_19,claim_cat=F,denom_count=%quote(&ltc_days. rlike '[1-9]'),numer_sum=&ltc_days.,level=CLH, claim_type=LT);
  %avg_per_unit(measure_id=mcr5_20,claim_cat=P,denom_count=%quote(&ltc_days. rlike '[1-9]'),numer_sum=&ltc_days.,level=CLH, claim_type=LT);
  %avg_per_unit(measure_id=mcr7_19,claim_cat=R,denom_count=%quote(&ltc_days. rlike '[1-9]'),numer_sum=&ltc_days.,level=CLH, claim_type=LT);
%mend run_903_avg_per_unit;


