/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


/* code borrowed and modified from 200 module */
 %macro ssn_nmisslogic(var, length);      
     case when &var. like repeat(8,&length.) then 0
	   	  when &var. like repeat(9,&length.) then 0
		  when &var. is null then 0 
	      when &var. rlike '[1-9]' then 1
		  else 0 end
 %mend ssn_nmisslogic;
 %macro misslogic(var, length);      
     case when &var. like repeat(8,&length.) then 1
	   	  when &var. like repeat(9,&length.) then 1
		  when &var. is null then 1
       	  when &var. rlike '[A-Za-z1-9]' then 0 
		  else 1 end    
 %mend misslogic;
 %macro nmisslogic(var, length);      
     case when &var. like repeat(8,&length.) then 0
	      when &var. like repeat(9,&length.) then 0
		  when &var. is null then 0
		  when &var. rlike '[A-Za-z1-9]' then 1
		  else 0 end     
 %mend nmisslogic; 

%macro misslogicprv_id(var, length);      
     case when &var. like repeat(8,&length.) then 1
	   	  when &var. like repeat(9,&length.) then 1
		  when &var. is null then 1
		  when &var = '0' then 1
       	  when &var. rlike '[A-Za-z1-9]' then 0 
		  else 1 end    
 %mend ;

/******* 2. process the data in AREMAC SQL tables and extract each measure to a sas dataset *******/

sysecho "running 101_el_pct"; %timestamp_log;
%include "&progpath./&module./101_el_pct_sql.sas"  ; %_101;

sysecho "running 102_el_cnt"; %timestamp_log;
%include "&progpath./&module./102_el_cnt_sql.sas"  ; %_102;

sysecho "running 103_el_index"; %timestamp_log;
%include "&progpath./&module./103_el_index_sql.sas"; %_103;

sysecho "running 104_el_freq"; %timestamp_log;
%include "&progpath./&module./104_el_freq_sql.sas" ; %_104;

sysecho "running 105_el_oth1"; %timestamp_log;
%include "&progpath./&module./105_el_oth1_sql.sas" ;  %_105;

sysecho "running 106_el_oth2"; %timestamp_log;
%include "&progpath./&module./106_el_oth2_sql.sas" ;  %_106;

sysecho "running 107_el_oth3"; %timestamp_log;
%include "&progpath./&module./107_el_oth3_sql.sas" ;  %_107;

sysecho "running 108_el_oth4"; %timestamp_log;
%include "&progpath./&module./108_el_oth4_sql.sas" ;  %_108;

sysecho "running 109_el_oth5"; %timestamp_log;
%include "&progpath./&module./109_el_oth5_sql.sas" ;  %_109;

/******* 3. finish processing in SAS *******/


data el_freq_values (drop=i);

length valid_value $2 measure $6;

measure = 'el2.1';
do i = 1 to 3, 8, 9;
	valid_value = put(i,1.);
	output;
end;
valid_value = 'A';
output;
valid_value = 'N';
output;
valid_value = 'T';
output;

measure = 'el4.1';
do i = 1 to 2, 9;
	valid_value = put(i,1.);
	output;
end;
valid_value = 'A';
output;
valid_value = 'N';
output;
valid_value = 'T';
output;

measure = 'el12.1';
do i = 1 to 75;
	valid_value = put(i,z2.);
	if i not in (10,57,58) then output;
end;
valid_value = 'A';
output;
valid_value = 'N';
output;
valid_value = 'T';
output;

measure = 'el6.24';
array values(14) $ _temporary_ ('1','2','3','4','5','6','7','A','B','C','D','A_','N_','T_');
do i = 1 to 14;
	valid_value = values(i);
	output;
end;

run;

proc sort data=el_freq_values;
by measure valid_value;
run;

proc sort data=el_freq;
by measure valid_value;
run;

data el_freq;
merge el_freq el_freq_values;
by measure valid_value;
if pct = . then pct = 0;
run;

data elg_100;
length measure_id $10. measure $8.;
set el_pct
    el_cnt
	el_index
	el_freq
	el_oth1
	el_oth2
	el503
	el_13_1
	/*
	el_pct_15_1
	el_pct_15_2
	*/
	;
    submtg_state_cd = "&state.";  
	valid_desc = '';
	measure_id = upcase(trim(measure));
	rename pct = statistic;
	drop measure;
run;

%reshape(elg,valid_value,valid_desc);


/******************************************************************************************
  reshape special measures from wide to long format
 ******************************************************************************************/
/*-----------------------------------------------------------------------------------------*
  el 8.2: use plan id format
 *-----------------------------------------------------------------------------------------*/

 data plan_ids;
  set el_802 (in=a)
	  el901;
  length statistic_type $ 30;
  length statistic $30; 

	measure_id = upcase(trim(measure));
	drop enc_tot plan_type;

	
    *for each plan id, output one observation per statistic type;
    %macro el802_to_long(type,var);
      statistic_type = "&type.";

		%if &var. ne capitation_type and &var. ne encounter_type %then %do ;
			if abs(&var.) > 999 then statistic = put(&var.,comma18.);
		  	else statistic = put(round(&var.,.01),best18.);
		%end ;

	 if "&var." in ("cap_ratio","ip_ratio","lt_ratio","ot_ratio","rx_ratio") and missing(&var.) then
	  	statistic = "div by 0";

	 %if &var. = capitation_type or &var. = encounter_type %then %do ;
	  statistic = &var. ;
	 %end;

	  submtg_state_cd="&state.";
      output;
	  drop &var.;
	%mend el802_to_long;

	if a then do;
		%el802_to_long(Enrollment,enrollment);
		%el802_to_long(HMO capitation,cap_hmo);
		%el802_to_long(PHP capitation,cap_php);
		%el802_to_long(PCCM capitation,cap_pccm);
		%el802_to_long(PHI capitation,cap_phi);
		%el802_to_long(Other capitation,cap_oth);
		%el802_to_long(Total capitation,cap_tot);
		%el802_to_long(Capitation Ratio,cap_ratio);
		%el802_to_long(IP encounters,enc_ip);
		%el802_to_long(LT encounters,enc_lt);
		%el802_to_long(OT encounters,enc_ot);
		%el802_to_long(RX encounters,enc_rx);
		%el802_to_long(IP ratio,ip_ratio);
		%el802_to_long(LT ratio,lt_ratio);
		%el802_to_long(OT ratio,ot_ratio);
		%el802_to_long(RX ratio,rx_ratio);
		%el802_to_long(Capitation Type,capitation_type);
		%el802_to_long(Encounter Type,encounter_type);

	end;
	else do;
      statistic_type = "Enrollment";
	  statistic = put(pct,comma18.);
	  submtg_state_cd="&state.";
      output;
	  drop enrollment pct;	
	end;
run;

* as per request from Kayshin sgo added below imputation for plan_ids for v1.2 updates;
data plan_ids;
set plan_ids;
if missing(plan_id) then plan_id = ".";
run;

%reshape(elgplan,,);

/*-----------------------------------------------------------------------------------------*
  el7.1 use special format
 *-----------------------------------------------------------------------------------------*/


%macro out_el_7_1;
	data 	
      %if &separate_entity.=1 or &separate_entity.=2 %then %do;
          dqout.elg71_&stabbrev._&typerun._&rpt_month._run&run_id.;
	  %end;
	  %else %do;
		 dqout.elg71_&stabbrev._&rpt_month._run&run_id.;
	  %end;

	set el701;
	length statistic_type $ 30;

	    measure_id = "EL-7-001-1";
		statistic_type = "enrollment";
	  	statistic = put(pct,comma18.);
	  	submtg_state_cd="&state.";
		report_state = "&stabbrev.";
		month_added = put(today(),yymmn6.);
		statistic_year_month = "&rpt_month.";
		drop pct measure;
run;
%mend;
%out_el_7_1;


