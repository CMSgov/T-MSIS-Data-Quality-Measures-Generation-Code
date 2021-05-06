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

%timestamp_log;

/*print parameters to log for easy review*/
%put Report State=&rpt_state.;
%put Report Month=&rpt_mnth.;
%put Report Run ID=&specific_run_id;
%put Data Limit=&limit;

%let module = 500_prv;

%macro nmsng(var, length);      
     (&var. is not NULL and 
      not &var. like repeat(8,&length) and
	  not &var. like repeat(9,&length) and	  
	  &var. rlike '[a-zA-Z1-9]')       
 %mend nmsng;


proc sql;
%tmsis_connect;

     sysecho "Running program 501";
	 %include "&progpath./&module./501_prvdr_pct_sql.sas";
	 %prvdr_pct_sql;	 %timestamp_log;

     sysecho "Running program 501";     
	 %include "&progpath./&module./502_prvdr_cnt_sql.sas";
	 %prvdr_cnt_sql;	%timestamp_log;

     sysecho "Running program 501";
	 %include "&progpath./&module./503_prvdr_freq_sql.sas";
	 %prvdr_freq_sql;	%timestamp_log;
     
     
     %dropwrktables(prvdr_nonfreq);
     
	 execute(
	 create table &wrktable..&taskprefix._prvdr_nonfreq as
	 select a.*
	 		,sumprv_1
			,sumprv_2
			,sumprv_3
	from &wrktable..&taskprefix._prvdr_pct a
	left join &wrktable..&taskprefix._sumprv_tab b
	on a.submtg_state_cd = b.submtg_state_cd
	) by tmsis_passthrough;

	create table prvdr_nonfreq as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._prvdr_nonfreq);

	create table prvdr_freq as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._prvdr_freq)
	 order by submtg_state_cd, prvdr_clsfctn_type_cd;

	 create table prvdr_freq_tot as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._prvdr_freq_tot)
	 order by submtg_state_cd;
     
     %dropwrktables(prvdr_pct sumprv_tab prvdr_freq prvdr_freq_tot prvdr_nonfreq);
 	
	 %tmsis_disconnect;
quit;
%status_check;

data prvdr_freq_t_A (rename=(prvdr_clsfctn_any=&m_col.));
set prvdr_freq_tot;
prvdr_clsfctn_type_cd = "A";
keep submtg_state_cd prvdr_clsfctn_type_cd 
     prvdr_clsfctn_any;
run;
data prvdr_freq_t_N(rename=(prvdr_clsfctn_none=&m_col.));
set prvdr_freq_tot;
prvdr_clsfctn_type_cd = "N";

keep submtg_state_cd prvdr_clsfctn_type_cd 
     prvdr_clsfctn_none;
run;

data prvdr_freq_t_T (rename=(prvdr_clsfctn_tot=&m_col.));
set prvdr_freq_tot;
prvdr_clsfctn_type_cd = "T";

keep submtg_state_cd prvdr_clsfctn_type_cd 
     prvdr_clsfctn_tot;
run;


data prvdr_freq_V(rename=(PRV4_1=&m_col.));
set prvdr_freq;
run;

data prvdr_freq_t;
set prvdr_freq_V
    prvdr_freq_t_A
	prvdr_freq_t_N
	prvdr_freq_t_T
 ;
 length msr_name $16.;
msr_name = 'prv4_1';
run;


data prvdr_freq_values (drop=i);
 length msr_name $16.;
msr_name = 'prv4_1';
do i = 1 to 4;
	prvdr_clsfctn_type_cd = put(i,1.);
	output;
end;
prvdr_clsfctn_type_cd = 'A';
output;
prvdr_clsfctn_type_cd = 'N';
output;
prvdr_clsfctn_type_cd = 'T';
output;
run;

proc sort data=prvdr_freq_values;
by msr_name prvdr_clsfctn_type_cd;
run;

proc sort data=prvdr_freq_t;
by msr_name prvdr_clsfctn_type_cd;
run;

data prvdr_freq_t;
merge prvdr_freq_t 
      prvdr_freq_values(in=a);
by msr_name prvdr_clsfctn_type_cd;
if a;
if &m_col. = . then &m_col. = 0;
submtg_state_cd = "&state.";

length prvdr_clsfctn_type_lbl $36.;

if prvdr_clsfctn_type_cd = '1' then prvdr_clsfctn_type_lbl='TAXONOMY CODE';
else if prvdr_clsfctn_type_cd = '2' then prvdr_clsfctn_type_lbl='PROVIDER SPECIALTY CODE';
else if prvdr_clsfctn_type_cd = '3' then prvdr_clsfctn_type_lbl='PROVIDER TYPE CODE';
else if prvdr_clsfctn_type_cd = '4' then prvdr_clsfctn_type_lbl='AUTHORIZED CATEGORY OF SERVICE CODE';
else if prvdr_clsfctn_type_cd = 'A' then prvdr_clsfctn_type_lbl='ANY VALID VALUE';
else if prvdr_clsfctn_type_cd = 'N' then prvdr_clsfctn_type_lbl='NO VALID VALUE';
else if prvdr_clsfctn_type_cd = 'T' then prvdr_clsfctn_type_lbl='TOTAL';
run;



proc transpose  data=prvdr_nonfreq
				out=prvdr_nonfreq_t (drop=_label_ rename=(col1=&m_col.))
				name=msr_name;
by submtg_state_cd;
run;

data prv_msrs;
set prvdr_freq_t
	prvdr_nonfreq_t;
format &m_col. 16.4;
run;

proc sort data=prv_msrs;
by msr_name prvdr_clsfctn_type_cd;
run;

%reshape(prv,prvdr_clsfctn_type_cd,prvdr_clsfctn_type_lbl);

%timestamp_log;

/*this forces the timestamp to be printed*/
data _null_;
run;

