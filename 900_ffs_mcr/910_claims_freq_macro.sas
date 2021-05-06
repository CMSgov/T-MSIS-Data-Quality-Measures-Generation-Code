/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 910_claims_freq_macro.sas   
 Project: MACBIS Task 2
 Purpose: Defines and calls ratio macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         

 
 Author:  Richard Chapman
 Date Created: 8/18/2017
 Current Programmer: Jacqueline Agufa
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.

 Modifications:
       Dec 2017-JMA-V1.3
                   -Changed code to show N,0,1,2,3,4,5,6,9,T 
                   -N missing, null and invalid responses 
                   -T total count 
 
 ******************************************************************************************/
 
 
/******************************************************************************************

 ******************************************************************************************/
 
%macro frq(
  measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_type=,  /*Which claim type. IP, LT, RX, OT*/
  level=,       /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_cat=,   /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  var=,         /*variable on which to run the freq*/
  constraint=,  /*constraint to apply prior to freq*/  
  );

  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'910' as submodule,
		null as numer,
		null as denom,
		mvalue,
		valid_value
	from (
    select &var. as valid_value, count(1) as mvalue
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint. and &var. in ('0','1','2','3','4','5','6','9')) and claim_cat_&claim_cat. = 1
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
    group by valid_value ) a
    )by tmsis_passthrough;	

 execute(
    insert into &wrktable..&taskprefix._clms_900b
	select
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'910' as submodule,
		null as numer,
		null as denom,
		mvalue,
		'N' as valid_value
	from (
    select count(1) as mvalue
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint. and (&var. not in ('0','1','2','3','4','5','6','9') or &var. is null)) and claim_cat_&claim_cat. = 1	
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end; ) a
    )by tmsis_passthrough;	

 execute(
    insert into &wrktable..&taskprefix._clms_900b
	select
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'910' as submodule,
		null as numer,
		null as denom,
		mvalue,
		'T' as valid_value
	from (
    select count (1) as mvalue
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint.) and claim_cat_&claim_cat. = 1
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end; ) a

  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.","0")
  values("&measure_id.","1")
  values("&measure_id.","2")
  values("&measure_id.","3")
  values("&measure_id.","4")
  values("&measure_id.","5")
  values("&measure_id.","6")
  values("&measure_id.","9")
  values("&measure_id.","N")
  values("&measure_id.","T")
  ;

%mend frq;

/******************************************************************************************
  Macro that contains all the calls to create each measure
  
  This macro gets called in the module driver
 ******************************************************************************************/

%macro run_910_all_freq;

*ffs27.1, ffs35.1, mcr33.1, mcr43.1;
  %frq(measure_id=ffs27_1,   claim_type=IP, level=CLH, claim_cat=C, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs35_1,   claim_type=IP, level=CLH, claim_cat=I, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr33_1,   claim_type=IP, level=CLH, claim_cat=O, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr43_1,   claim_type=IP, level=CLH, claim_cat=U, var=adjstmt_ind, constraint=%str(1=1));
  
* ffs28.1, ffs36.1, mcr34.1, mcr44.1;
  %frq(measure_id=ffs28_1,   claim_type=LT, level=CLH, claim_cat=C, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs36_1,   claim_type=LT, level=CLH, claim_cat=I, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr34_1,   claim_type=LT, level=CLH, claim_cat=O, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr44_1,   claim_type=LT, level=CLH, claim_cat=U, var=adjstmt_ind, constraint=%str(1=1));
  
  
* ffs29.1, ffs37.1, mcr35.1, mcr36.1, mcr45.1, mcr46.1;
  %frq(measure_id=ffs29_1,   claim_type=OT, level=CLH, claim_cat=C, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs37_1,   claim_type=OT, level=CLH, claim_cat=I, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr35_1,   claim_type=OT, level=CLH, claim_cat=Y, var=adjstmt_ind, constraint=%str(1=1));    
  %frq(measure_id=mcr36_1,   claim_type=OT, level=CLH, claim_cat=O, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr45_1,   claim_type=OT, level=CLH, claim_cat=Z, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr46_1,   claim_type=OT, level=CLH, claim_cat=U, var=adjstmt_ind, constraint=%str(1=1));
  
* ffs30.1, ffs38.1, mcr37.1, mcr48.1;
  %frq(measure_id=ffs30_1,   claim_type=RX, level=CLH, claim_cat=C, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs38_1,   claim_type=RX, level=CLH, claim_cat=I, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr37_1,   claim_type=RX, level=CLH, claim_cat=O, var=adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr48_1,   claim_type=RX, level=CLH, claim_cat=U, var=adjstmt_ind, constraint=%str(1=1));

* ffs31.1, ffs39.1, mcr38.1, mcr47.1;
  %frq(measure_id=ffs31_1,   claim_type=IP, level=CLL, claim_cat=C, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs39_1,   claim_type=IP, level=CLL, claim_cat=I, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr38_1,   claim_type=IP, level=CLL, claim_cat=O, var=line_adjstmt_ind, constraint=%str(1=1));  
  %frq(measure_id=mcr47_1,   claim_type=IP, level=CLL, claim_cat=U, var=line_adjstmt_ind, constraint=%str(1=1));
    
* ffs32.1, ffs40.1, mcr39.1, mcr49.1;
  %frq(measure_id=ffs32_1,   claim_type=LT, level=CLL, claim_cat=C, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs40_1,   claim_type=LT, level=CLL, claim_cat=I, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr39_1,   claim_type=LT, level=CLL, claim_cat=O, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr49_1,   claim_type=LT, level=CLL, claim_cat=U, var=line_adjstmt_ind, constraint=%str(1=1));
  
* ffs33.1, ffs41.1, mcr40.1, mcr41.1, mcr50.1, mcr51.1;
  %frq(measure_id=ffs33_1,   claim_type=OT, level=CLL, claim_cat=C, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs41_1,   claim_type=OT, level=CLL, claim_cat=I, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr40_1,   claim_type=OT, level=CLL, claim_cat=Y, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr41_1,   claim_type=OT, level=CLL, claim_cat=O, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr50_1,   claim_type=OT, level=CLL, claim_cat=Z, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr51_1,   claim_type=OT, level=CLL, claim_cat=U, var=line_adjstmt_ind, constraint=%str(1=1));
  
* ffs34.1, ffs42.1, mcr42.1, mcr52.1;  
  %frq(measure_id=ffs34_1,   claim_type=RX, level=CLL, claim_cat=C, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=ffs42_1,   claim_type=RX, level=CLL, claim_cat=I, var=line_adjstmt_ind, constraint=%str(1=1));
  %frq(measure_id=mcr42_1,   claim_type=RX, level=CLL, claim_cat=O, var=line_adjstmt_ind, constraint=%str(1=1));   
  %frq(measure_id=mcr52_1,   claim_type=RX, level=CLL, claim_cat=U, var=line_adjstmt_ind, constraint=%str(1=1));

%mend run_910_all_freq;
