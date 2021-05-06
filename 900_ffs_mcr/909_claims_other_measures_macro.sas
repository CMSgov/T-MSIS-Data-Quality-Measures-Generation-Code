/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 909_claims_other_measures_macro.sas
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

 ******************************************************************************************/


/******************************************************************************************
  MCR 28-1
 ******************************************************************************************/

%macro mcr28_1;

%dropwrktables(ids enrollees cap);

  execute(
    create table &wrktable..&taskprefix._ids as
        select distinct state_plan_id_num
    from &temptable..&taskprefix._tmsis_mc_mn_data
        where (state_plan_id_num is not NULL) and
          not (mc_plan_type_cd in ('02','03'))
  ) by tmsis_passthrough;


  execute(
    create table &wrktable..&taskprefix._enrollees as
        select mc_plan_id, count(1) as enrollment
        from (
      select distinct mc_plan_id, msis_ident_num
      from &temptable..&taskprefix._tmsis_mc_prtcptn_data
          where mc_plan_id is not NULL and
                msis_ident_num is not NULL
    ) a
        group by mc_plan_id
  ) by tmsis_passthrough;


  execute(
    create table &wrktable..&taskprefix._cap as
        select plan_id_num,
               count(1) as capitation
    from &temptable..&taskprefix._base_cll_ot
        where plan_id_num is not NULL and
              mdcd_pd_amt >0 and
                  clm_type_cd in ('2','B') and
                  adjstmt_ind = '0'
        group by plan_id_num
  ) by tmsis_passthrough;
  
  execute(
    create or replace temporary view &taskprefix._full as
        select a.state_plan_id_num
               ,coalesce(b.enrollment,0) as enrollment
               ,coalesce(c.capitation,0) as capitation
               ,case when coalesce(b.enrollment,0) <> 0 then coalesce(c.capitation,0)/coalesce(b.enrollment,0) else NULL end as capitation_ratio
    from &wrktable..&taskprefix._ids a
        left join &wrktable..&taskprefix._enrollees  b on a.state_plan_id_num = b.mc_plan_id
        left join &wrktable..&taskprefix._cap c on a.state_plan_id_num = c.plan_id_num
  ) by tmsis_passthrough;

  
  execute(
    insert into &wrktable..&taskprefix._clms_900b
        select 
			%str(%')&state.%str(%') as submtg_state_cd, 
			'mcr28_1' as measure_id, 
			'909' as submodule,
			coalesce(numer,0), 
			coalesce(denom,0),
            case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
			null as valid_value
    from (select count(1) as denom
               ,sum(case when capitation_ratio <0.9 or capitation_ratio > 1.1 then 1 else 0 end) as numer
             from #temp.&taskprefix._full
        ) a
  ) by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("mcr28_1",null);
  

%mend mcr28_1;

/******************************************************************************************
  summcr 27
 ******************************************************************************************/

%macro summcr27;
  
  execute(
    insert into &wrktable..&taskprefix._clms_900b
          select  
			%str(%')&state.%str(%') as submtg_state_cd, 
			'summcr_27' as measure_id,
			'909' as submodule,
			null as numer,
			null as denom,
            coalesce(count(distinct state_plan_id_num),0) as mvalue,
			null as valid_value
    from &temptable..&taskprefix._tmsis_mc_mn_data
  ) by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("summcr_27",null);

%mend summcr27;

/******************************************************************************************
  Macro to run all special measures (gets run in module driver)
 ******************************************************************************************/

%macro run_909_other_measures;
  %mcr28_1;
  %summcr27;
%mend run_909_other_measures;

