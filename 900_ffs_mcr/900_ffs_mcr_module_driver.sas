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
             -Removing code for MCR27_1 and MCR27_2
*****************************************************************/

/*-----------------------------------------------------------------------------------------*/
%macro not_missing_1(var, length);      
     (&var. is not NULL and 
      not &var. like repeat(8,&length) and
	  not &var. like repeat(9,&length) and	  
	  &var. rlike '[a-zA-Z1-9]')     
%mend not_missing_1;

%macro is_missing_2(var, length);
     (&var. is NULL or
      abs(&var.) like repeat(8,&length.) or
          abs(&var.) like repeat (9,&length.) or
          not &var. rlike '[1-9]')
 %mend is_missing_2;

%macro is_missing_3(var, length, dec);
     (&var. is NULL or
      abs(&var.) like concat_ws('.',repeat(8,&length.),repeat(8,&dec.)) or
          abs(&var.) like concat_ws('.',repeat(9,&length.),repeat(9,&dec.)) or
          not &var. rlike '[1-9]')
 %mend is_missing_3;
/*----------------------------------------------------------------------------------------*
   macro variable set ltc_days that gets used in multiple measures,
   to avoid rewriting same definition
 *----------------------------------------------------------------------------------------*/
  %let ltc_days  = coalesce(lve_days_cnt,0) + coalesce(ICF_IID_DAYS_CNT,0) + coalesce(NRSNG_FAC_DAYS_CNT,0) + coalesce(MDCD_CVRD_IP_DAYS_CNT,0);
  %let ltc_days1 = coalesce(lve_days_cnt,0) + coalesce(nrsng_fac_days_cnt,0);
  %let ltc_days2 = coalesce(lve_days_cnt,0) + coalesce(mdcd_cvrd_ip_days_cnt,0);
  %let ltc_days3 = coalesce(lve_days_cnt,0) + coalesce(icf_iid_days_cnt,0);

/*----------------------------------------------------------------------------------------*
   Create measures one at a time in AREMAC, then extract each measure into sas
 *----------------------------------------------------------------------------------------*/

/*include macros and macro calls for all measures, spread across 8 groups*/

%include "&progpath./&module./901_claims_pct_macro.sas";
%include "&progpath./&module./902_claims_count_macro.sas";
%include "&progpath./&module./903_claims_avg_per_unit_macro.sas";
%include "&progpath./&module./904_claims_avg_occur_macro.sas";
%include "&progpath./&module./905_claims_avg_count_macro.sas";
%include "&progpath./&module./906_ffs_clms_ad.sas";
%include "&progpath./&module./907_claims_ratio_macro.sas";
*Note: 908 was dropped because all measures are inactive;
%include "&progpath./&module./909_claims_other_measures_macro.sas";
%include "&progpath./&module./910_claims_freq_macro.sas";
%include "&progpath./&module./911_claims_ever_elig_macro.sas";
%include "&progpath./&module./912_claims_other_measures_PCCM_macro.sas";
%include "&progpath./&module./913_claims_pct_amt_macro.sas";
%include "&progpath./&module./914_claims_pct_pymnt_macro.sas";
%include "&progpath./&module./915_claims_provider_taxonomy.sas";
%include "&progpath./&module./916_claims_luhn_check.sas";
%include "&progpath./&module./917_claims_pct_bill_type_cd_OT.sas";
%include "&progpath./&module./918_claims_sum_cll_mdcr_amt.sas";
%include "&progpath./&module./919_claims_schip_aq_ar.sas";

%macro run_submodule(num,name);

	proc sql;

	    %tmsis_connect;

        sysecho "run program &num.";
		%run_&num._&name.;	

		%tmsis_disconnect;

	quit;

	%status_check;			
	%timestamp_log;

%mend;


/******************************************************************************************
  Set Together all measures into a single dataset
******************************************************************************************/

%let round4 = "MCR1_2", "MCR1_6", "MCR5_9", "MCR5_11", "MCR5_12", "MCR3_2", "MCR3_6",
              "MCR7_9", "MCR7_11","MCR7_12",
              "SUMMCR_2", "SUMMCR_5", "SUMMCR_8", "SUMMCR_10",
              "SUMMCR_13", "SUMMCR_16", "SUMMCR_19", "SUMMCR_21",
              "FFS1_2", "FFS3_2", "FFS1_18", "FFS3_6", "FFS5_9", "FFS7_9", "FFS5_11", "FFS7_11",
			  "FFS5_12", "FFS7_12", "SUMFFS_2", "SUMFFS_13", "SUMFFS_5", "SUMFFS_16", 
			  "SUMFFS_8", "SUMFFS_19", "SUMFFS_10", "SUMFFS_21",
			  "FFS10_3","FFS1_17", "FFS18_2", "MCR1_5", "MCR21_2", "FFS1_4", "FFS10_84", "FFS5_24" , "FFS5_25",
			  "FFS9_2", "FFS9_98", "MCR1_4", "MCR10_19", "MCR10_2" , "FFS9_9", "MCR10_9"
               ;

%let round3 = "MCR14_9" ,  "MCR32_1" ,  "MCR32_2" ,  "MCR32_4" ,  "MCR32_5" ,
              "MCR32_6" ,  "MCR32_7" ,  "MCR32_8" ,  "MCR32_9" ,  "MCR32_10" ,  "MCR32_11" ,  "MCR32_12" ,
              "MCR32_13" , "MCR32_14" , "MCR32_16" ,"MCR32_18" ,  "MCR32_20" ,
              "MCR9_18", "MCR9_19", "MCR13_18", "MCR13_19", "MCR62_4",
			  "FFS11_9", "FFS26_1", "FFS26_2", "FFS26_3", "FFS26_4", "FFS26_5", "FFS26_6", "FFS26_7", 
              "FFS26_8", "FFS26_9", "FFS26_10", "FFS26_11", "FFS26_12", "FFS26_13", "FFS26_14", "FFS26_15", 
			  "FFS26_16", "FFS52_4", "FFS19_1", "MCR32_15", "MCR32_17" , "MCR32_19"
              "FFS53_1",  "MCR63_1", "FFS53_2", "MCR63_2",
              "FFS53_3",  "MCR63_3", "FFS53_4", "MCR63_4"

              ;

%let round2 = "MCR28_1", "MCR56_1", "MCR57_1", "FFS47_1", "FFS48_1"
                ;

/*************************************************************
Create a SAS table to hold a list of measures as they are 
created. This is how we will know to zero fill any measures
that do not appear in the AREMAC output. This replaces the
empty table macro for the 900 series.
**************************************************************/

%macro create_measures_table;
proc sql;
create table dqout.measures_900
(measure_id char(10), valid_value char(1));
quit;
run;
%mend;



/**************************************************************
  Module 900a - just program 901
  Note: This was split off from the rest to try to improve
	    processing. If this one succeeds and then there is
		a subsequent failure in a later submodule, we will 
		not	lose this data.
**************************************************************/

%macro run_901();

	%if &restart. <= 901 %then %do;

		%create_measures_table;

		%run_submodule(901,all_claims_pct);

		data dqout.claims_901;
			set batch_900a;
		run;

		%status_check;
		%let restart = 902;

	%end;

%mend;
%run_901;


/**************************************************************
  Module 900b - programs 902 and later
  Note: This was split off from the rest to try to improve
	    processing. If program 901 succeeds and then there is
		a subsequent failure in a later submodule, we will 
		not	lose the 901 data.
**************************************************************/

%run_submodule(902,all_countt);
%run_submodule(903,avg_per_unit);
%run_submodule(904,all_avg_occur);
%run_submodule(905,all_avg_count);
%run_submodule(906,ffs_clms_ad);
%run_submodule(907,all_ratio);       
*Note: 908 was dropped because all measures are inactive;
%run_submodule(909,other_measures);
%run_submodule(910,all_freq);
%run_submodule(911,all_ever_elig);
%run_submodule(912,other_measures_PCCM);
%run_submodule(913,claims_pct_amt);
%run_submodule(914,claims_pct_pymnt);
%run_submodule(915,claims_provider_taxonomy);
%run_submodule(916,claims_luhn_check);
%run_submodule(917,claims_pct_blltypcd_OT);
%run_submodule(918,claims_cll_amt);
%run_submodule(919,claims_schip_aq_ar);


data dqout.claims_999;
set batch_900b;
run;


proc sort data=dqout.claims_999 out=claims_999;
by measure_id valid_value;
run;

proc sort data=dqout.claims_901 out=claims_901;
by measure_id;
run;

proc sort data=dqout.measures_900 nodupkey out=measures;
by measure_id valid_value;
run;

proc sql noprint;
create table all_measures as
select
	"&state." as submtg_state_cd
	,a.measure_id
	,a.valid_value
	,coalesce(b.numer,c.numer) as numer
	,coalesce(b.denom, c.denom) as denom
	,coalesce(b.mvalue, c.mvalue) as mvalue
	,coalesce(c.submodule,'901') as submodule
from measures a
left join claims_901 b on a.measure_id = b.measure_id
left join claims_999 c on a.measure_id = c.measure_id and a.valid_value = c.valid_value
;
quit;
run;

data ffs_mcr_900;
    length msr_id $20;
    format msr_id $20.;
        set
			all_measures
			;

		msr_id = upcase(measure_id);

		if not missing(mvalue) then do;
        	if msr_id in (&round4.) then mvalue = round(mvalue,.0001);
        	else if msr_id in (&round3.) then mvalue = round(mvalue,.001);
			else if msr_id in (&round2.) then mvalue = round(mvalue,.01);
        	else if submodule in ('903','904','905','907','908','910') then mvalue = round(mvalue,.1);
			else mvalue = round(mvalue,.01);
		end;
		else do;
			if not missing(valid_value) then mvalue = 0;
			else do;
				denom=0;
				numer=0;
			end;
		end;

  		msr_id = tranwrd(msr_id,'_','.');

		drop measure_id submodule;
		rename 
		    msr_id = measure_id
			numer=numerator
		 	denom=denominator
		  	mvalue=statistic
		   	;
run;


proc datasets lib=dqout;
  delete claims_901 claims_999 measures_900;
quit;
run;


%reshape(mcr,valid_value,valid_desc);


/******************************************************************************************
  End of program - write timestamp to log
 ******************************************************************************************/

%timestamp_log;





