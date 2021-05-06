/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/*****************************************************************
Project:     50139 MACBIS Data Analytics Task 2
Programmer:  Richard Chapman

Measures: None
Inputs:   Parameters for state, month, and (optional) run ID

Modifications:
08/14/17 RSC Updates for v1.2:
             -removed stuff for EXP26.1 at end of program

*****************************************************************/




/*************************************************************
Create a SAS table to hold a list of measures as they are 
created. This is how we will know to zero fill any measures
that do not appear in the AREMAC output. This replaces the
empty table macro for the 900 series.
**************************************************************/

%macro create_measures_table;
proc sql;
create table dqout.measures_200
(measure_id char(10));
quit;
run;
%mend;
%create_measures_table;

/*-----------------------------------------------------------------------------------------*/
%macro not_missing_1(var, length);      
     (&var. is not NULL and 
      not &var. like repeat(8,&length) and
	  not &var. like repeat(9,&length) and	  
	  &var. rlike '[a-zA-Z1-9]')     
 %mend not_missing_1;

/*-----------------------------------------------------------------------------------------*/

proc sql;
%tmsis_connect;


%cache_claims_tables();

 
sysecho "Running 200 modules";
/*include macros and macro calls for all measures, spread across 5 groups*/
%include "&progpath./200_exp/201_exp_claims_pct_macro.sas";
%include "&progpath./200_exp/202_exp_avg_macro.sas";
/*all measures in 203 are obsolete*/
%include "&progpath./200_exp/204_exp_sum_macro.sas";
%include "&progpath./200_exp/205_exp_other_measures_macro.sas";
%include "&progpath./200_exp/206_exp_claims_count_macro.sas";


/*run each set of measures - needs to be done prior to tmsis disconnect*/
sysecho "running 201"; %timestamp_log;
%run_201_all_claims_pct;
sysecho "running 202"; %timestamp_log;
%run_202_all_avg;
/*JMA 10/07/2020 -- All measures in this program have been removed
 sysecho "running 203"; %timestamp_log;
 %run_203_all_avg_per_unit;
 */
sysecho "running 204"; %timestamp_log;
%run_204_all_summ;


sysecho "running 205"; %timestamp_log;
%run_205_all_other_exp;

sysecho "running 206"; %timestamp_log;
%run_206_all_countt;


/*----------------------------------------------------------------------------------------*
  End of AREMAC processing - all measures are now extracted at the measure level
  into SAS. Disconnect from tmsis.
 *----------------------------------------------------------------------------------------*/

	 %tmsis_disconnect;
quit;
%status_check;


/******************************************************************************************
  Set Together all measures into a single sas dataset
 ******************************************************************************************/

proc sort data=exp_200_extract;
by measure_id;
run;

proc sort data=dqout.measures_200 nodupkey out=measures;
by measure_id;
run;

proc sql noprint;
create table all_measures as
select
	 "&state." as submtg_state_cd
	,a.measure_id
	,numer
	,denom
	,mvalue
	,submodule
from measures a
left join exp_200_extract b on a.measure_id = b.measure_id
;
quit;
run;


data exp_200;
format msr_id $20.;
length msr_id $20;
set all_measures;

/**V2.1 - round to 3 decimal places for these two measures*/
if measure_id in ('exp16_1','exp2_1') then statistic = round(mvalue,.001);
else if upcase(measure_id) in ('EXP28_1','EXP25_1','EXP23_1') then statistic = round(mvalue,.1);
else if upcase(measure_id) in ('EXP11_83','EXP26_2','EXP26_3') then statistic = round(mvalue,.01);

else do;
   if submodule = '201' then statistic = round(mvalue,.01);
   else if submodule = '202' then statistic = round(mvalue,.1);
   else if submodule in ('204','206') then statistic = round(mvalue,1);
   else statistic = mvalue;
end;

if submodule in ('204','206') then do;
	numer = .;
	denom = .;
end;

if missing(numer) and missing(denom) and missing(mvalue) then statistic = 0;

msr_id = tranwrd(upcase(measure_id),'_','.');

drop measure_id submodule mvalue;
rename 
    msr_id = measure_id
   	;

run;



%reshape(exp,,);

%timestamp_log;

