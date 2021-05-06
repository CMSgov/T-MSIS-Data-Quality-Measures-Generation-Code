/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro nmisslogic(var);      
     (&var. is not NULL and 
      &var. rlike '[a-zA-Z1-9]'
      )     
 %mend nmisslogic;
%macro misslogic(var);      
     ( not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )  
 %mend misslogic;
%macro nmsng(var, length);      
     (&var. is not NULL and 
      not &var. like repeat(8,&length) and
	  not &var. like repeat(9,&length) and	  
	  &var. rlike '[a-zA-Z1-9]')       
 %mend nmsng;

* insert freq measures;    
%macro insert_freq_msr(msrid=, list1=, list2=);
    %local n1 n2;
    %let n1=%sysfunc(countw(&list1));
    %let n2=%sysfunc(countw(&list2));
    insert into dqout.measures_700
        %do i=1 %to %sysfunc(countw(&list1));
    values("&msrid", %scan(&list1, &i))
        %end;
    %do j=1 %to %sysfunc(countw(&list2));
        values("&msrid", %scan(&list2, &j))
            %end;
        values("&msrid", 'T_');
    %mend;

%macro insert_msr(msrid=);
    insert into dqout.measures_700
        values("&msrid", null);
    %mend;    
    
proc sql;
    create table dqout.measures_700
        (measure_id char(10), valid_value char(10));
quit;

proc sql;
	 create table pcx as
            select quote(trim(code),"'") as code
            , case when Type in ("CPT", "HCPCS Level II") then "prc"
            when Type = "ICD-10-CM" then "cm"
            when Type = "ICD-10-PCS" then "pcs"
            else ' '
            end as prgcd
            from prgncy.'1 - Ever pregnant'n
            ;
quit;

proc sql;
%temptable_connect;
	execute (drop table if exists &temptable..&taskprefix._pcx ) by tmsis_temptable;
     create table TMPTBLDB.&taskprefix._pcx  as 
	select * from pcx;
	execute (drop table if exists &temptable..&taskprefix._prgncy_codes ) by tmsis_temptable;
	execute (create table &temptable..&taskprefix._prgncy_codes as
	        select a.*, %str(%')&state.%str(%') as submtg_state_cd
        from (

        select
        collect_set(case when prgcd='cm' then code else null end) as code_cm
        , collect_set(case when prgcd='pcs' then code else null end) as code_pcs
        , collect_set(case when prgcd='prc' then code else null end) as code_prc
        from &temptable..&taskprefix._pcx) a
		) by tmsis_temptable;

%temptable_disconnect;
quit;

proc sql;
    %tmsis_connect;
    %cache_claims_tables();
    %dropwrktables(utl_700 prgncy_codes);

    %let utl_output=&wrktable..&taskprefix._utl_700;
    execute(
        create table &utl_output. (        
        submtg_state_cd STRING
        ,measure_id STRING
        ,submodule STRING
        ,numer DOUBLE
        ,denom DOUBLE
        ,mvalue DOUBLE
        ,valid_value STRING
        ,claim_type STRING
        )
        )by tmsis_passthrough;
      
    sysecho "running 701 ot";
    %include "&progpath./&module./701_utl_ot_prep_l_sql.sas";
    %utl_ot_prep_sql;
    %timestamp_log;

    sysecho "running 701 stplan";
    %include "&progpath./&module./701_utl_stplan_prep_l_sql.sas";
    %utl_stplan_sql;
    %timestamp_log;

    sysecho "running 701 wvr";
    %include "&progpath./&module./701_utl_wvr_prep_l_sql.sas";
    %utl_wvr_sql;
    %timestamp_log;

    sysecho "running 702 el";
    %include "&progpath./&module./702_utl_el_tab_l_sql.sas";
    %utl_el_sql;
    %timestamp_log;

    sysecho "running 703 ip";
    %include "&progpath./&module./703_utl_ip_tab_n_sql.sas";
    %utl_ip_n_sql;
    %timestamp_log;

    sysecho "running 703 lt";
    %include "&progpath./&module./703_utl_lt_tab_n_sql.sas";
    %utl_lt_n_sql;
    %timestamp_log;

    sysecho "running 703 ot";
    %include "&progpath./&module./703_utl_ot_tab_n_sql.sas";
    %utl_ot_n_sql;
    %timestamp_log;

    sysecho "running 704 ip";
    %include "&progpath./&module./704_utl_ip_tab_w_sql.sas";
    %utl_ip_w_sql;
    %timestamp_log;

    sysecho "running 704 clms prov";
    %include "&progpath./&module./704_utl_clms_prov_tab_w_sql.sas";
    %timestamp_log;
	
    sysecho "running 705 ip";
    %include "&progpath./&module./705_utl_ip_tab_ab_ac_sql.sas";  
    %utl_ip_AB_AC_sql;
    %timestamp_log;
    %utl_link_IP_el_AB_sql;
    %timestamp_log;

    sysecho "running 705 lt";
    %include "&progpath./&module./705_utl_lt_tab_ab_ac_sql.sas";
    %utl_lt_AB_AC_sql;
    %timestamp_log;

    sysecho "running 705 ot";
    %include "&progpath./&module./705_utl_ot_tab_ab_ac_sql.sas";
    %utl_ot_AB_AC_sql;
    %timestamp_log;
    %utl_link_OT_el_AB_sql;
    %timestamp_log;

    sysecho "running 705 rx";
    %include "&progpath./&module./705_utl_rx_tab_ab_ac_sql.sas";
    %utl_link_RX_el_AB_sql;
    %timestamp_log;
  
    sysecho "running 706";
    %include "&progpath./&module./706_utl_all_clms_tab_ai_sql.sas";
    %utl_all_clms_ai_sql;
    %timestamp_log;

    sysecho "running 707";
    %include "&progpath./&module./707_utl_ot_tab_aj_sql.sas";
    %utl_ot_aj_sql;
    %timestamp_log;

    sysecho "running 707";
    %include "&progpath./&module./707_utl_all_clms_prov_tab_aj_sql.sas";
    %timestamp_log;

    sysecho "running 708";
    %include "&progpath./&module./708_utl_all_clms_tab_msng_sql.sas";
    %timestamp_log;
    
    sysecho "running 709";
    %include "&progpath./&module./709_utl_all_clms_tab_ah_sql.sas";
    %utl_all_clms_ah_sql;
    %timestamp_log;

    sysecho "running 710";
    %include "&progpath./&module./710_utl_all_clms_freq_sql.sas";
    %utl_all_clms_freq_sql;
    %timestamp_log;

    sysecho "running 711";
    %include "&progpath./&module./711_utl_all_clms_freq_sql.sas";
    %utl_all_clms_freq_oth;
    %timestamp_log;

    sysecho "running 712";
    %include "&progpath./&module./712_utl_all_clms_freq_stc_cd.sas";
    %utl_all_clms_freq_stc_cd;
    %timestamp_log;
    
    sysecho "running 713";
    %include "&progpath./&module./713_utl_all_pymnt_aj_sql.sas";
    %utl_all_pymnt_aj_w_sql;
    %timestamp_log;

    create table utl_700 as
        select * from connection to tmsis_passthrough
        (select * from &utl_output.);
    
    %tmsis_disconnect;
quit;
%status_check;

proc sort data=utl_700;
    by measure_id;
run;

proc sort data=dqout.measures_700 nodupkey out=measures;
    by measure_id valid_value;
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
        ,a.valid_value
        , claim_type
        from measures a left join utl_700 b
        on a.measure_id = b.measure_id and a.valid_value = b.valid_value
        ;
quit;
run;

data utl_700;
    format msr_id $20.;
    length msr_id $20;
    set all_measures;

    msr_id = upcase(measure_id);
    
    msr_id = tranwrd(upcase(measure_id),'_','.');

    if substr(msr_id, 1, 5) in ('ALL32', 'ALL33') then do; 
	    claim_type = scan(upcase(msr_id), -1, '.');
        msr_id = substr(msr_id, 1, index(msr_id, catx('.', claim_type))-2);
		end;

    * 1-value measures: move numer to mvalue;
    * freq measures: coalesce the frequency count;
    * 3-value measures: coalesce the denominator value;
    if msr_id in ('ALL1.1', 'ALL1.2', 'ALL1.3', 'ALL1.4', 'ALL1.5', 'ALL1.6',
        'ALL2.1',
        'ALL18.1', 'ALL18.2', 'ALL18.3', 'ALL18.4',
        'ALL27.1', 'ALL27.2') then do;
        mvalue = numer;
        numer = .;
        end;
    else if substr(msr_id, 1, 5) in ('ALL22', 'ALL23', 'ALL24', 'ALL25', 'ALL28', 'ALL29', 'ALL30', 'ALL31', 'ALL32', 'ALL33') then 
        mvalue = coalesce(mvalue, 0);
    else denom = coalesce(denom, 0);
    drop measure_id submodule;
    rename 
        msr_id = measure_id
        mvalue = statistic
        numer = numerator
        denom = denominator
        ;
run;

%reshape(utl,);

%timestamp_log;

/*this forces the timestamp to be printed*/
data _null_;
run;

