/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 912_mcr_other_measures_PCCM_macro.sas   
 Project: MACBIS Task 2
 Purpose: Defines and calls macros for all other measures (special, non-standard measures) for module 900 (ffs and managed care)
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

 Modifications:
 08/14/17 RSC Updates for v1.2:
 12/19/17 JMA Updates for v1.3
 ******************************************************************************************/
 
%macro extract_claims(claim_cat);

%dropwrktables(clms_&claim_cat.);
execute(
	create table &wrktable..&taskprefix._clms_&claim_cat. as
      select msis_ident_num, plan_id_num, blg_prvdr_num, blg_prvdr_npi_num
      from &temptable..&taskprefix._base_cll_ot
         where plan_id_num is not null and
               claim_cat_&claim_cat.=1 and 
			   childless_header_flag = 0 and
               stc_cd = '120'
	) by tmsis_passthrough;

%mend;

/******************************************************************************************
  MCR 9.18, 13.18
  ******************************************************************************************/


%macro PCCM_9_18_13_18(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
 );

execute(
    create or replace temporary view &taskprefix._denom as
      select 
			a.*, 
			1 as has_denom_line,
            case when((a.msis_ident_num=b.msis_ident_num and
						a.plan_id_num=b.mc_plan_id and 
						b.evr_pccm_mc_plan_type=0) /**never a pccm plan id*/
                     or
			           (a.plan_id_num is not null and /**plan id not in MC file */
					    b.msis_ident_num is null and
                        b.mc_plan_id is null )
                       )
                  then 1 else 0 end as has_numer_line
      from &wrktable..&taskprefix._clms_&claim_cat. as a 
         left join &wrktable..&taskprefix._mc_data as b
		 on (a.msis_ident_num=b.msis_ident_num and
             a.plan_id_num=b.mc_plan_id)
  ) by tmsis_passthrough;

 execute(
     insert into &wrktable..&taskprefix._clms_900b
      select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id. %str(%') as measure_id, 
		'912' as submodule,
		coalesce(numer,0),
		coalesce(denom,0),
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
      from (
	      select 
                sum(case when has_denom_line=1 then 1 else 0 end) as denom
  	           ,sum(case when has_numer_line=1 then 1 else 0 end) as numer
	      from #temp.&taskprefix._denom
            ) a	
  ) by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
  
%mend PCCM_9_18_13_18;


/******************************************************************************************
  MCR 9.19, 13.19
  ******************************************************************************************/


%macro PCCM_9_19_13_19(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
 );

execute(
    create or replace temporary view &taskprefix._denom as
      select *, 1 as has_denom_line, 
             case when((plan_id_num is not null) and
                       (plan_id_num =  blg_prvdr_num 
                              OR
                        plan_id_num = blg_prvdr_npi_num) )
                  then 1 else 0 end as has_numer_line
      from &wrktable..&taskprefix._clms_&claim_cat.

	) by tmsis_passthrough;

 execute(
    insert into &wrktable..&taskprefix._clms_900b
    select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'912' as submodule,
		coalesce(numer,0),
		coalesce(denom,0), 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
      from (
	      select
                sum(case when has_denom_line=1 then 1 else 0 end) as denom
  	           ,sum(case when has_numer_line=1 then 1 else 0 end) as numer
	      from #temp.&taskprefix._denom
             ) a	
  ) by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend PCCM_9_19_13_19;

 
%macro run_912_other_measures_PCCM;

  %dropwrktables(mc_data);
  execute(
    create table &wrktable..&taskprefix._mc_data as
      select mc_plan_id, msis_ident_num, 
             max(case when (enrld_mc_plan_type_cd = '02' ) then 1 else 0 end) 
                as evr_pccm_mc_plan_type
      from &temptable..&taskprefix._tmsis_mc_prtcptn_data  
	  where mc_plan_id is not null 
	  group by mc_plan_id, msis_ident_num
	  ) by tmsis_passthrough;

	  %extract_claims(claim_cat=D);
	  %extract_claims(claim_cat=J);

  %PCCM_9_18_13_18(measure_id=mcr9_18,  claim_cat=D);
  %PCCM_9_18_13_18(measure_id=mcr13_18, claim_cat=J);

  %PCCM_9_19_13_19(measure_id=mcr9_19,  claim_cat=D);
  %PCCM_9_19_13_19(measure_id=mcr13_19, claim_cat=J);
  
%mend run_912_other_measures_PCCM;
