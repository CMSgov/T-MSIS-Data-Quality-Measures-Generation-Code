/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

 /******************************************************************************************
 Program: 911_mcr_ever_elig_macro.sas 
 Project: MACBIS Task 2
 Purpose: Defines and calls ever eligible macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          New for v1.2
 
 Author:  Richard Chapman
 Date Created: 9/7/2017
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.

 Modifications:
 12/08/17 JMA Updates for v1.3:
              -updated 31.1-31.10, 
              -added non-missing date constraint
              
 ******************************************************************************************/
 
 /******************************************************************************************
   Macro #1: Claims Percentage
   
   Can be run on either measures that are strictly line level or strictly header level
   (But not ones that require identifying claims with at least one line that meets a
    certain criteria -- use the next macro for those measures)
  ******************************************************************************************/
  
 
 %macro any_span(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );
  
  execute(
    create or replace temporary view &taskprefix._ids as
    select b.ever_eligible
	from 
      (select distinct msis_ident_num
       from &temptable..&taskprefix._base_&level._&claim_type.
       where claim_cat_&claim_cat. = 1
		%if %lowcase(&level.) = cll %then %do;
	  	  and childless_header_flag = 0
		%end;
	  ) a
	left join
	  (select distinct msis_ident_num, ever_eligible
	   from &temptable..&taskprefix._ever_elig 
	   where ever_eligible=1) b
	on a.msis_ident_num = b.msis_ident_num
				
  )by tmsis_passthrough;

  
  execute(
    insert into &wrktable..&taskprefix._clms_900b
    select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'911' as submodule,
		coalesce(numer,0) as numer,
		coalesce(denom,0) as denom,
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select  
	     sum(1) as denom
	    ,sum(case when ever_eligible=1 then 1 else 0 end) as numer
	  from #temp.&taskprefix._ids
	) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend any_span;


 %macro span_on_date(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=,  /*Which claim type. IP, LT, RX, OT*/
  date_var=  
  );

 execute(
    create or replace temporary view &taskprefix._ids as
    select  a.msis_ident_num
		   ,max(case when a.&date_var. is not null      
                 then 1 else 0 end) as denom_flag
		   ,max(case when b.ever_eligible=1 
                      and a.&date_var. is not null      
                      and a.&date_var. >= b.enrlmt_efctv_dt
					  and (a.&date_var. <= b.enrlmt_end_dt
					       or b.enrlmt_end_dt is NULL						   
                           )
					  then 1 else 0 end) as numer_flag
    from &temptable..&taskprefix._base_&level._&claim_type. a
	left join &temptable..&taskprefix._ever_elig b
    on  a.msis_ident_num = b.msis_ident_num
	where a.claim_cat_&claim_cat. = 1
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
	group by a.msis_ident_num
			
  )by tmsis_passthrough;

   execute(
    insert into &wrktable..&taskprefix._clms_900b
    select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'911' as submodule,
		coalesce(numer,0) as numer,
		coalesce(denom,0) as denom, 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	  select 
		 sum(denom_flag) as denom
	    ,sum(numer_flag) as numer
	  from #temp.&taskprefix._ids
	) a
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
  

%mend span_on_date;
/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/
 

%macro run_911_all_ever_elig;

* mcr30.1, ffs24.1, mcr30.6, ffs24.5;
  %any_span(measure_id = ffs24_1, claim_cat=C, level=CLH, claim_type = IP);
  %any_span(measure_id = ffs24_5, claim_cat=I, level=CLH, claim_type = IP);
  %any_span(measure_id = mcr30_1, claim_cat=O, level=CLH, claim_type = IP);
  %any_span(measure_id = mcr30_6, claim_cat=U, level=CLH, claim_type = IP);

* mcr31.1, ffs25.1, mcr31.6, ffs25.5;
  %span_on_date(measure_id = ffs25_1, claim_cat=C, level=CLH, claim_type = IP, date_var=admsn_dt);
  %span_on_date(measure_id = ffs25_5, claim_cat=I, level=CLH, claim_type = IP, date_var=admsn_dt);                                                                                                 
  %span_on_date(measure_id = mcr31_1, claim_cat=O, level=CLH, claim_type = IP, date_var=admsn_dt);
  %span_on_date(measure_id = mcr31_6, claim_cat=U, level=CLH, claim_type = IP, date_var=admsn_dt);
  
* mcr30.2, ffs24.2, mcr30.7, ffs24.6;  
  %any_span(measure_id = ffs24_2, claim_cat=C, level=CLH, claim_type = LT);
  %any_span(measure_id = ffs24_6, claim_cat=I, level=CLH, claim_type = LT);
  %any_span(measure_id = mcr30_2, claim_cat=O, level=CLH, claim_type = LT);
  %any_span(measure_id = mcr30_7, claim_cat=U, level=CLH, claim_type = LT);
   
  
* mcr31.2, ffs25.2, mcr31.7, ffs25.6;  
  %span_on_date(measure_id = ffs25_2, claim_cat=C, level=CLH, claim_type = LT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = ffs25_6, claim_cat=I, level=CLH, claim_type = LT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = mcr31_2, claim_cat=O, level=CLH, claim_type = LT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = mcr31_7, claim_cat=U, level=CLH, claim_type = LT, date_var=srvc_bgnng_dt);
  
* mcr31.3, mcr31.4, ffs25.3, mcr31.8, mcr31.9, ffs25.7;
  %span_on_date(measure_id = ffs25_3, claim_cat=C, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = ffs25_7, claim_cat=I, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = mcr31_3, claim_cat=Y, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = mcr31_4, claim_cat=O, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);  
  %span_on_date(measure_id = mcr31_8, claim_cat=Z, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);
  %span_on_date(measure_id = mcr31_9, claim_cat=U, level=CLH, claim_type = OT, date_var=srvc_bgnng_dt);
  
* mcr30.3, mcr30.4, ffs24.3, mcr30.8, mcr30.9, ffs24.7;   
  %any_span(measure_id = ffs24_3, claim_cat=C, level=CLH, claim_type = OT);
  %any_span(measure_id = ffs24_7, claim_cat=I, level=CLH, claim_type = OT);
  %any_span(measure_id = mcr30_3, claim_cat=Y, level=CLH, claim_type = OT);
  %any_span(measure_id = mcr30_4, claim_cat=O, level=CLH, claim_type = OT);
  %any_span(measure_id = mcr30_9, claim_cat=U, level=CLH, claim_type = OT);
  %any_span(measure_id = mcr30_8, claim_cat=Z, level=CLH, claim_type = OT);

* mcr30.5, ffs24.4, mcr30.10, ffs24.8;
  %any_span(measure_id = ffs24_4,  claim_cat=C, level=CLH, claim_type = RX);
  %any_span(measure_id = ffs24_8,  claim_cat=I, level=CLH, claim_type = RX);
  %any_span(measure_id = mcr30_5,  claim_cat=O, level=CLH, claim_type = RX);
  %any_span(measure_id = mcr30_10, claim_cat=U, level=CLH, claim_type = RX);
    
* mcr31.5, ffs25.4, mcr31.10, ffs25.8;
  %span_on_date(measure_id = ffs25_4,  claim_cat=C, level=CLH, claim_type = RX, date_var=rx_fill_dt);
  %span_on_date(measure_id = ffs25_8,  claim_cat=I, level=CLH, claim_type = RX, date_var=rx_fill_dt);
  %span_on_date(measure_id = mcr31_5,  claim_cat=O, level=CLH, claim_type = RX, date_var=rx_fill_dt);
  %span_on_date(measure_id = mcr31_10, claim_cat=U, level=CLH, claim_type = RX, date_var=rx_fill_dt);                                                                                                      

%mend run_911_all_ever_elig;
