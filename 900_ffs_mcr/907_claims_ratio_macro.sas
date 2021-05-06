/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 907_claims_ratio_macro.sas   
 Project: MACBIS Task 2
 Purpose: Defines and calls ratio macros for module 900 (ffs and managed care)
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

 ******************************************************************************************/
 
%macro ratio(
  measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_type=,  /*Which claim type. IP, LT, RX, OT*/
  level=,       /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_cat1=,  /*Which claim category the measure should use for set 1. (A,B,C, etc.). Defined in specs. */
  var1=,
  constraint1=,
  claim_cat2=,  /*Which claim category the measure should use for set 2. (A,B,C, etc.). Defined in specs. */
  var2=,
  constraint2=
  );

  execute(
    create or replace temporary view &taskprefix._avgs as
    select 
		numer_sum1, 
		denom_count1, 
	    case when denom_count1 <> 0 then numer_sum1/denom_count1 else NULL end as avg1,
		numer_sum2,
		denom_count2, 
	    case when denom_count2 <> 0 then numer_sum2/denom_count2 else NULL end as avg2
	
	from (
		select
		 sum(case when (&constraint1.) and (claim_cat_&claim_cat1. = 1) then 1      else 0 end) as denom_count1
	    ,sum(case when (&constraint1.) and (claim_cat_&claim_cat1. = 1) then &var1. else 0 end) as numer_sum1
	    ,sum(case when (&constraint2.) and (claim_cat_&claim_cat2. = 1) then 1      else 0 end) as denom_count2
	    ,sum(case when (&constraint2.) and (claim_cat_&claim_cat2. = 1) then &var2. else 0 end) as numer_sum2
	  from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	    where childless_header_flag = 0
	  %end;
	  ) a
	
  )by tmsis_passthrough;

  /*ratio of averages: return NULL if either numer avg or denom avg is NULL, or if denom avg = 0*/
  execute(
    insert into &wrktable..&taskprefix._clms_900b
    select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'907' as submodule,
		avg1 as numer, 
		avg2 as denom, 
	    case when (avg1 is not NULL and avg2 is not NULL and avg2 <> 0) then avg1/avg2 
		   		else NULL end as mvalue,
		null as valid_value
    from #temp.&taskprefix._avgs b
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend ratio;

/******************************************************************************************
  Macro that contains all the calls to create each measure
  
  This macro gets called in the module driver
 ******************************************************************************************/

%macro run_907_all_ratio;

  %ratio(measure_id=mcr2_13,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '001'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '001'));

  %ratio(measure_id=mcr2_17,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '058'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '058'));
  %ratio(measure_id=mcr2_18,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '060'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '060'));
  %ratio(measure_id=mcr2_19,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '084'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '084'));
  %ratio(measure_id=mcr2_20,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '086'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '086'));
  %ratio(measure_id=mcr2_21,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '090'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '090'));
  %ratio(measure_id=mcr2_22,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '091'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '091'));
  %ratio(measure_id=mcr2_23,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '092'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '092'));
  %ratio(measure_id=mcr2_24,   claim_type=IP, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '093'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '093'));

  %ratio(measure_id=mcr6_9,    claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '044'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '044'));
  %ratio(measure_id=mcr6_10,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '045'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '045'));
  %ratio(measure_id=mcr6_11,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '046'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '046'));
  %ratio(measure_id=mcr6_12,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '047'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '047'));
  %ratio(measure_id=mcr6_13,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '048'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '048'));
  %ratio(measure_id=mcr6_14,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '050'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '050'));
  %ratio(measure_id=mcr6_15,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '059'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '059'));
  %ratio(measure_id=mcr6_16,   claim_type=LT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '009'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '009'));

  %ratio(measure_id=mcr12_79,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '010'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '010'));
  %ratio(measure_id=mcr12_80,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '011'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '011'));
  %ratio(measure_id=mcr12_81,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '115'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '115'));
  %ratio(measure_id=mcr12_82,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '012'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '012'));
  %ratio(measure_id=mcr12_83,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '127'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '127'));
  %ratio(measure_id=mcr12_84,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '013'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '013'));
  %ratio(measure_id=mcr12_85,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '014'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '014'));
  %ratio(measure_id=mcr12_86,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '015'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '015'));
  %ratio(measure_id=mcr12_87,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '016'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '016'));
  %ratio(measure_id=mcr12_88,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '017'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '017'));
  %ratio(measure_id=mcr12_89,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '018'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '018'));

  %ratio(measure_id=mcr12_90,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '019'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '019'));
  %ratio(measure_id=mcr12_91,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '002'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '002'));
  %ratio(measure_id=mcr12_92,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '020'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '020'));
  %ratio(measure_id=mcr12_93,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '021'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '021'));
  %ratio(measure_id=mcr12_94,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '022'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '022'));
  %ratio(measure_id=mcr12_95,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '023'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '023'));
  %ratio(measure_id=mcr12_96,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '024'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '024'));
  %ratio(measure_id=mcr12_97,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '025'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '025'));
  %ratio(measure_id=mcr12_98,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '026'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '026'));
  %ratio(measure_id=mcr12_99,  claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '027'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '027'));

  %ratio(measure_id=mcr12_100, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '028'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '028'));
  %ratio(measure_id=mcr12_101, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '029'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '029'));
  %ratio(measure_id=mcr12_102, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '003'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '003'));
  %ratio(measure_id=mcr12_103, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '030'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '030'));
  %ratio(measure_id=mcr12_104, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '031'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '031'));
  %ratio(measure_id=mcr12_105, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '032'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '032'));
  %ratio(measure_id=mcr12_106, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '035'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '035'));
  %ratio(measure_id=mcr12_107, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '036'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '036'));
  %ratio(measure_id=mcr12_108, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '037'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '037'));
  %ratio(measure_id=mcr12_109, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '038'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '038'));

  %ratio(measure_id=mcr12_110, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '039'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '039'));
  %ratio(measure_id=mcr12_111, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '004'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '004'));
  %ratio(measure_id=mcr12_112, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '040'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '040'));
  %ratio(measure_id=mcr12_113, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '041'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '041'));
  %ratio(measure_id=mcr12_114, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '042'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '042'));
  %ratio(measure_id=mcr12_115, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '043'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '043'));
  %ratio(measure_id=mcr12_116, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '049'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '049'));
  %ratio(measure_id=mcr12_117, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '005'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '005'));
  %ratio(measure_id=mcr12_118, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '050'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '050'));
  %ratio(measure_id=mcr12_119, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '051'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '051'));

  %ratio(measure_id=mcr12_120, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '052'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '052'));
  %ratio(measure_id=mcr12_121, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '053'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '053'));
  %ratio(measure_id=mcr12_122, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '054'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '054'));
  %ratio(measure_id=mcr12_123, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '055'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '055'));
  %ratio(measure_id=mcr12_124, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '056'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '056'));
  %ratio(measure_id=mcr12_125, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '057'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '057'));
  %ratio(measure_id=mcr12_126, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '006'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '006'));
  %ratio(measure_id=mcr12_127, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '061'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '061'));
  %ratio(measure_id=mcr12_128, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '062'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '062'));
  %ratio(measure_id=mcr12_129, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '063'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '063'));
  %ratio(measure_id=mcr12_130, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '064'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '064'));
  %ratio(measure_id=mcr12_131, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '065'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '065'));
  %ratio(measure_id=mcr12_132, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '066'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '066'));
  %ratio(measure_id=mcr12_133, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '067'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '067'));
  %ratio(measure_id=mcr12_134, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '068'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '068'));
  %ratio(measure_id=mcr12_135, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '069'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '069'));
  %ratio(measure_id=mcr12_136, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '007'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '007'));
  %ratio(measure_id=mcr12_137, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '070'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '070'));
  %ratio(measure_id=mcr12_138, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '071'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '071'));
  %ratio(measure_id=mcr12_139, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '072'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '072'));

  %ratio(measure_id=mcr12_140, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '073'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '073'));
  %ratio(measure_id=mcr12_141, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '074'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '074'));
  %ratio(measure_id=mcr12_142, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '075'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '075'));
  %ratio(measure_id=mcr12_143, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '076'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '076'));
  %ratio(measure_id=mcr12_144, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '077'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '077'));
  %ratio(measure_id=mcr12_145, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '078'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '078'));
  %ratio(measure_id=mcr12_146, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '079'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '079'));
  %ratio(measure_id=mcr12_147, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '008'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '008'));
  %ratio(measure_id=mcr12_148, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '080'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '080'));
  %ratio(measure_id=mcr12_149, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '081'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '081'));

  %ratio(measure_id=mcr12_150, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '082'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '082'));
  %ratio(measure_id=mcr12_151, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '083'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '083'));
  %ratio(measure_id=mcr12_152, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '085'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '085'));
  %ratio(measure_id=mcr12_153, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '087'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '087'));
  %ratio(measure_id=mcr12_154, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '088'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '088'));
  %ratio(measure_id=mcr12_155, claim_type=OT, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '089'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '089'));

  %ratio(measure_id=mcr18_9,   claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '011'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '011'));
  %ratio(measure_id=mcr18_10,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '127'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '127'));
  %ratio(measure_id=mcr18_11,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '018'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '018'));
  %ratio(measure_id=mcr18_12,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '033'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '033'));
  %ratio(measure_id=mcr18_13,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '034'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '034'));
  %ratio(measure_id=mcr18_14,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '036'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '036'));
  %ratio(measure_id=mcr18_15,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '085'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '085'));
  %ratio(measure_id=mcr18_16,  claim_type=RX, level=CLL, claim_cat1=Q, var1=MDCD_FFS_EQUIV_AMT, constraint1=%str(STC_CD= '089'), claim_cat2=M, var2=MDCD_PD_AMT, constraint2=%str(STC_CD= '089'));

%mend run_907_all_ratio;
