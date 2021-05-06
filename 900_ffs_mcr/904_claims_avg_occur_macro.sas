/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 904_claims_avg_occur_macro.sas  
 Project: MACBIS Task 2
 Purpose: Defines and calls average occurence macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          
 
 Author:  Richard Chapman
 Date Created: 3/1/2017
 Current Programmer: Jacqueline Agufa
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.

 Modifications:

 ******************************************************************************************/
 
 
/******************************************************************************************
  Macro for calculating average occurance
 ******************************************************************************************/
 
%macro avg_occur(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,
  denom=,
  count_var_pre=,
  count_var_post=,
  count_num=,
  count_len=,
  level=,
  claim_type=
  );


  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'904' as submodule,
    	coalesce(numer,0) as numer,
    	coalesce(denom,0) as denom,
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
	     sum(case when (&denom.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as denom
	    ,sum(case when (&denom.) and (claim_cat_&claim_cat. = 1) then 
		  %do i = 1 %to &count_num.;		
            case when %quote(%not_missing_1(&count_var_pre.&i.&count_var_post.,&count_len.)) then 1 else 0 end
			%if &i. < &count_num. %then %do;
              +
			%end;
		  %end;
          else 0 end) as numer
	  from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	    where childless_header_flag = 0
	  %end;
	) a
	
  )by tmsis_passthrough; 

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
 

%mend avg_occur;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/

%macro run_904_all_avg_occur;

 	* ffs1.28, ffs3.16, mcr1.16, mcr3.16;
  	%avg_occur(measure_id=ffs1_28, claim_cat=A, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=12, count_len=7, level=CLH, claim_type=IP);
	%avg_occur(measure_id=ffs3_16, claim_cat=F, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=12, count_len=7, level=CLH, claim_type=IP);
  	%avg_occur(measure_id=mcr1_16, claim_cat=P, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=12, count_len=7, level=CLH, claim_type=IP);
	%avg_occur(measure_id=mcr3_16, claim_cat=R, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=12, count_len=7, level=CLH, claim_type=IP);

	* ffs1.29, ffs3.17, mcr1.17, mcr3.17;
	%avg_occur(measure_id=ffs1_29, claim_cat=A, denom=%str(1=1), count_var_pre=prcdr_, count_var_post=_cd, count_num=6,  count_len=8, level=CLH, claim_type=IP);
	%avg_occur(measure_id=ffs3_17, claim_cat=F, denom=%str(1=1), count_var_pre=prcdr_, count_var_post=_cd, count_num=6,  count_len=8, level=CLH, claim_type=IP);
	%avg_occur(measure_id=mcr1_17, claim_cat=P, denom=%str(1=1), count_var_pre=prcdr_, count_var_post=_cd, count_num=6,  count_len=8, level=CLH, claim_type=IP);
	%avg_occur(measure_id=mcr3_17, claim_cat=R, denom=%str(1=1), count_var_pre=prcdr_, count_var_post=_cd, count_num=6,  count_len=8, level=CLH, claim_type=IP);

	* ffs5.28, ffs7.18, mcr5.19, mcr7.18;
	%avg_occur(measure_id=ffs5_28, claim_cat=A, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=5,  count_len=7, level=CLH, claim_type=LT);
	%avg_occur(measure_id=ffs7_18, claim_cat=F, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=5,  count_len=7, level=CLH, claim_type=LT);
	%avg_occur(measure_id=mcr5_19, claim_cat=P, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=5,  count_len=7, level=CLH, claim_type=LT);
	%avg_occur(measure_id=mcr7_18, claim_cat=R, denom=%str(1=1), count_var_pre=dgns_,  count_var_post=_cd, count_num=5,  count_len=7, level=CLH, claim_type=LT);

%mend run_904_all_avg_occur; 

