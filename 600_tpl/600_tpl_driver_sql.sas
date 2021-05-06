/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


proc sql;
%tmsis_connect;

	*Tables 2,3,4,5;
	sysecho "running program 601"; 
	%include "&progpath./&module./601_tpl_clm_tab_othr_sql.sas";
	%tpl_clm_sql; %timestamp_log;
	
	*Tables 6 and 7;
	sysecho "running program 602";
	%include "&progpath./&module./602_tpl_ot_tab_6_7_sql.sas";
	%tpl_ot_6_7_sql; %timestamp_log;

	*Table 1;
	sysecho "running program 603 mn";
	%include "&progpath./&module./603_tpl_prsn_mn_tab_sql.sas";
	%tpl_prsn_mn_sql; %timestamp_log;
	
    sysecho "running program 603 hi";
	%include "&progpath./&module./603_tpl_prsn_hi_tab_sql.sas";
	%tpl_prsn_hi_sql; %timestamp_log;                

    sysecho "running program 603 mn ever";
	%include "&progpath./&module./603_tpl_prsn_mn_ever_tab_sql.sas";
	%ever_tpl_elig_prsn_mn_sql; %timestamp_log;

	%macro assemble_tpl();
    
    %emptytable(tpl_ip);
    %emptytable(tpl_lt);
    %emptytable(tpl_ot);
    %emptytable(tpl_rx);
    %emptytable(ot_6_7);
    %emptytable(tpl_prsn_mn_tab);
    %emptytable(tpl_prsn_hi_tab);
    %emptytable(ever_tpl_elig_tab);

        %dropwrkviews(tpl_msrs);
		execute(
			create or replace view &wrktable..&taskprefix._tpl_msrs as
			select 
				a.submtg_state_cd
				,tpl1_5_numer
				,tpl1_5_denom
				,tpl1_5
				%do i = 2 %to 5;
					%do j = 1 %to 8;
						,tpl&i._&j._numer
						,tpl&i._&j._denom
						,tpl&i._&j.
					%end;
				%end;
				%do k = 1 %to 4;
					,TPL1_&k.
					,TPL6_&k.
					,TPL7_&k.
				%end;

			from &wrktable..&taskprefix._tpl_ip a
			left join &wrktable..&taskprefix._tpl_lt b on a.submtg_state_cd = b.submtg_state_cd
			left join &wrktable..&taskprefix._tpl_ot c on a.submtg_state_cd = c.submtg_state_cd
			left join &wrktable..&taskprefix._tpl_rx d on a.submtg_state_cd = d.submtg_state_cd
			left join &wrktable..&taskprefix._ot_6_7 e on a.submtg_state_cd = e.submtg_state_cd
			left join &wrktable..&taskprefix._tpl_prsn_mn_tab f on a.submtg_state_cd = f.submtg_state_cd
			left join &wrktable..&taskprefix._tpl_prsn_hi_tab g on a.submtg_state_cd = g.submtg_state_cd
			left join &wrktable..&taskprefix._ever_tpl_elig_tab h on a.submtg_state_cd = h.submtg_state_cd
		) by tmsis_passthrough;

	%mend;
	%assemble_tpl;

	create table tpl_msrs as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._tpl_msrs);

    %dropwrktables(tpl_ip tpl_lt tpl_ot tpl_rx ot_6_7 tpl_prsn_mn_tab tpl_prsn_hi_tab ever_tpl_elig_tab);
    %dropwrkviews(tpl_msrs);

	 %tmsis_disconnect;
quit;
%status_check;

%reshape(tpl,);

%timestamp_log;

/*this forces the timestamp to be printed*/
data _null_;
run;

