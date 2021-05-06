/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
 /******************************************************************************************
 Program: 206_claims_count_macro.sas 
 Project: MACBIS Task 2
 Purpose: Defines and calls count macros for module 200 exp
          Designed to be %included in module level driver         
          
 
 Author:  Jacqueline Agufa
 Date Created: 5/1/2020
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call creates a single measure, and extracts that measure into a SAS work dataset
         from AREMAC. These tables are named EXP##.#. Most are a single observation, except for
         frequency measures or other measures that have one observation per oberved value.
 
 Modifications:
 
 ******************************************************************************************/
 
/******************************************************************************************
   Macro: Counts
   
   Can be run on either measures that are strictly line level or strictly header level
******************************************************************************************/
 
%macro countt(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  constraint=, /*Logical constraints that restrict which observations get included in the count*/ 
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );


  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'206' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer else NULL end as mvalue
    from (
	    select 
			 sum(case when                    (claim_cat_&claim_cat. = 1) then 1 else 0 end) as denom
	        ,sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as numer
	    from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	  	  where childless_header_flag = 0
	    %end;
	) a
	
  )by tmsis_passthrough; 

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");

%mend countt;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/

%macro run_206_all_countt;

%countt(measure_id=exp43_1,  claim_cat=W, 
        constraint=%quote(srvc_trkng_type_cd rlike '[a-zA-Z1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=IP);
%countt(measure_id=exp43_2,  claim_cat=W, 
        constraint=%quote(srvc_trkng_type_cd rlike '[a-zA-Z1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=LT);
%countt(measure_id=exp43_3,  claim_cat=W, 
        constraint=%quote(srvc_trkng_type_cd rlike '[a-zA-Z1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=OT);
%countt(measure_id=exp43_4,  claim_cat=W, 
        constraint=%quote(srvc_trkng_type_cd rlike '[a-zA-Z1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=RX);
%countt(measure_id=exp43_5,  claim_cat=W, 
        constraint=%quote(srvc_trkng_pymt_amt rlike '[1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=IP);
%countt(measure_id=exp43_6,  claim_cat=W, 
        constraint=%quote(srvc_trkng_pymt_amt rlike '[1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=LT);
%countt(measure_id=exp43_7,  claim_cat=W, 
        constraint=%quote(srvc_trkng_pymt_amt rlike '[1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=OT);
%countt(measure_id=exp43_8,  claim_cat=W, 
        constraint=%quote(srvc_trkng_pymt_amt rlike '[1-9]' and clm_type_cd not in ('4','D','X')), 
        level=CLH, claim_type=RX);


/*extract measures from AREMAC into sas*/
create table exp_200_extract as
select * from connection to tmsis_passthrough
(select * from &wrktable..&taskprefix._exp_200);

%dropwrktables(exp_200);

%mend run_206_all_countt;

