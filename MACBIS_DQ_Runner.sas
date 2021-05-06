/*****************************************************************************************
* Copyright (C) Mathematica Policy Research, Inc.
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica Policy Research, Inc.
******************************************************************************************/ 
 
/**************************************************************
* Primary DQ Measures Controller Program: All Modules
***************************************************************
Project:     50139 MACBIS Data Analytics Task 12
Program:     MACBIS_DQ_Runner
Programmer:  Kerianne Hourihan

Inputs:   Parameters for state, month
          Optional parameters for run ID and limit

Note:   All parameters are specified using the table located at:
        &progpath./dq_run_tbl.sas7bdat

Modifications:
 (1) Run missingness measures conditionally
 (2) Recode Report_State=PA_M/PA_C when running these PA modules

*****************************************************************/

%let AREMACpath = /sasdata/users/&sysuserid./tmsisshare/prod/01_AREMAC;
%let progpath = &AREMACpath./Task_12/DQ_SAS;
%let specvrsn = %str(V2.5); /*change this for each version */

*statements for accessing T-MSIS data in AREMAC;
%include "&AREMACpath./global/task12_databricks_connection.sas";

*SAS options;
options nocenter ls=255 fmtsearch = (frmt.states) dlcreatedir;
libname frmt "&progpath./002_lookups";

*macros used in all modules;
%include "&progpath./001_includes/universal_macros.sas";

*Lookup Tables;
libname thresh xlsx "&progpath./002_lookups/thresholds.xlsx";
libname apdxc  xlsx "&progpath./002_lookups/AppendixC.xlsx";
libname splans xlsx "&progpath./002_lookups/MMCDCS_StatePlans Lookup Table2.xlsx";
libname sauths xlsx "&progpath./002_lookups/MMCDCS_StateAuthorities Lookup Table2.xlsx";
libname schip  xlsx "&progpath./002_lookups/SCHIP_Lookup.xlsx";
*Missingness Tables;
libname missVar xlsx "&progpath./002_lookups/State_DQ_Missingness_Measures.xlsx";
*V1.7 additions;
libname prgncy    xlsx "&progpath./002_lookups/MIH Ever Pregnant Code Set.xlsx";
libname prvtxnmy  xlsx "&progpath./002_lookups/ProviderTaxonomy.xlsx";
libname fmg       xlsx "&progpath./002_lookups/Expansion Eligibility Groups_2018.xlsx";
libname stc_cd    xlsx "&progpath./002_lookups/TypeOfService.xlsx";

*Call macros to define additional parameters;
%define_state(&rpt_state.);
%define_dates(&rpt_mnth.);
%define_runtyp;

*output directory;
libname stfldr "&progpath./DQ_Output/&stabbrev.";
libname dqout "&progpath./DQ_Output/&stabbrev./&rpt_fldr.";
%let txtout = &progpath./DQ_Output/&stabbrev./&rpt_fldr.;
%let logout = &progpath./DQ_Output/&stabbrev./&rpt_fldr.;

*capture or generate timestamp as table prefix;
%global pgmstart;
%if "&timeprefix" ne "" %then %do;
	%let pgmstart = &timeprefix;
%end;
%else %do;
	%let pgmstart_date = %sysfunc(putn(%sysfunc(date()),yymmdd6.));
	%let pgmstart_time = %sysfunc(putn(%sysfunc(time()),tod5.));
	%let pgmstart = %sysfunc(catx(_,&pgmstart_date,
						%sysfunc(substr(&pgmstart_time,1,2)),
						%sysfunc(substr(&pgmstart_time,4,2))));
%end;

*AREMAC database names;
%let permview = macbis_t12_perm;
%let temptable = macbis_t12_temp_%lowcase(&stabbrev.&typerun.);
%let wrktable = macbis_t12_wrk_%lowcase(&stabbrev.&typerun.);
%let taskprefix = %lowcase(&stabbrev.&typerun.)&rpt_fldr._&pgmstart.;


*dq_run_tbl path (for Excel export with each run);
Libname dqruntbl "&progpath.";

*set up global restartnum parameter;
%global restart;

*Switch to state/month/run-specific log;
%macro print_log;

%if &separate_entity.=1 or &separate_entity.=2 %then %do;
	proc printto log="&progpath./MACBIS_DQ_control_&stabbrev._&typerun._&rpt_mnth._run&run_id..log"
			   print="&progpath./MACBIS_DQ_control_&stabbrev._&typerun._&rpt_mnth._run&run_id..lst" 
	new;
	run;
%end;
%else %do;
	proc printto log="&progpath./MACBIS_DQ_control_&stabbrev._&rpt_mnth._run&run_id..log" 
			   print="&progpath./MACBIS_DQ_control_&stabbrev._&rpt_mnth._run&run_id..lst" 
	new;
	run;
%end;
%mend;
%print_log;

%macro printvars;

	%if &start_at = . %then %let start_at = 0;
	%let restart = &start_at;

	*just printing the parameters to the log for confirmation;
	%put rpt_state=&rpt_state.;
	%put rpt_mnth=&rpt_mnth.;
	%put specific_run_id=&specific_run_id.;
	%put specific_run_id2=&specific_run_id2.; 
	%put limit=&limit.;
	%put separate_entity=&separate_entity.;	
	%put msng_dq_run_flag=&msng_dq_run_flag.;
	%put start_at=&start_at.;
	%put timeprefix=&timeprefix.;

%mend;
%printvars;

%macro create_tables;
	%if &restart = 0 %then %do;
		%create_elig_tables(monthind=current);
		%create_elig_tables(monthind=prior);
		%create_prov_tables;
		%create_mcplan_tables;
		%create_tpl_tables;
		%create_claims_tables;
		*****DO NOT USE *******%create_perfom_ind_tables;
		%status_check;
	%end;
%mend;
%create_tables;


%macro run_module(modnum,module,driver);
	%if &restart <= %eval(&modnum. + 99) %then %do;
		%let restart = %sysfunc(max(&modnum,&restart));
		%put starting module &modnum.;
		%put restart parameter set to &restart.;
		%include "&progpath./&module./&driver..sas";
		%timestamp_log;
		%status_check;
	%end;
%mend;

%macro run_all_modules;

%run_module(100,100_elg,100_elg_driver_sql);
%run_module(200,200_exp,200_exp_module_driver);
%run_module(500,500_prv,500_prov_driver_sql);
%run_module(600,600_tpl,600_tpl_driver_sql);
%run_module(700,700_utl,700_utl_driver_sql);

%if &msng_dq_run_flag.=1 %then %do;
 	%run_module(800,800_msn,800_miss_driver_sql);
%end;

*KH moved FFS/MCR to be last due to longest processing;
%run_module(900,900_ffs_mcr,900_ffs_mcr_module_driver);

%mend;
%run_all_modules;

*KH moved exportexcel macro def to universal_macros;
%exportexcel; 

proc printto;
run;


/*Export dq_run_tbl to Excel*/
proc export data= dqruntbl.dq_run_tbl outfile="&progpath./DQ_run_tbl" dbms=xlsx replace;
run;






