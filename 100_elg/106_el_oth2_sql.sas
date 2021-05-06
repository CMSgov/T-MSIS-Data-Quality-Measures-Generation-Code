/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 106_el_oth2_sql.sas
 Project: MACBIS Task 2

 Author:  Sabitha Gopalsamy
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: el_oth2 SAS work dataset
         
 Modifications: 9/11 : SGO added updated #EL9.1 and #EL7.1 measures for V1.2 updates
				9/25 : SGO corrected the issues in #EL9.1 and #EL7.1 from code review
				12/7/12 : SGO added mc_plan_enrlmt_efctv_dt criteria in #EL9.1 for V1.3 updates
				1/18/17 : SGO Added Emptytable macros
 
 ******************************************************************************************/

/*Other: Sections 5.7, 5.11 and 5.12 */
/****************************************************************************************************/
%macro _106;

%let viewlist = el103a el901a el701a;
%let tbllist = el103  el901  el701;

proc sql;
	%tmsis_connect;

	%dropwrkviews(&viewlist.);
	%dropwrktables(&tbllist.);

	*el1.3;
	execute(
		create or replace view &wrktable..&taskprefix._el103a as	
	    select ssn_num, 
	           count(distinct msis_ident_num) as subset
	    from &temptable..&taskprefix._tmsis_var_dmgrphc_elgblty
	    group by ssn_num
	    having count(distinct msis_ident_num) >1
	)by tmsis_passthrough;


	execute(
		create table &wrktable..&taskprefix._el103 as
	    select count(distinct ssn_num) as pct,
				%str(%')&state.%str(%') as submtg_state_cd,
				%str(%')el1.3%str(%') as measure
		from &wrktable..&taskprefix._el103a
	)by tmsis_passthrough;

	*el9.1;
	execute(
		create or replace view &wrktable..&taskprefix._el901a as
		select coalesce(plan_id,'.') as plan_id,
				coalesce(plan_type_el,'.') as plan_type_el,
				pct
		from
			(
				select  mc_plan_id as plan_id,
						enrld_mc_plan_type_cd as plan_type_el,
						count(distinct msis_ident_num) as pct
				from &temptable..&taskprefix._tmsis_mc_prtcptn_data
				where mc_plan_enrlmt_efctv_dt is not null
				group by mc_plan_id,enrld_mc_plan_type_cd
			) a
	)by tmsis_passthrough;   

	execute(
		create table &wrktable..&taskprefix._el901 as
	    select  plan_id,
				plan_type_el,
				pct,
				%str(%')&state.%str(%') as submtg_state_cd,
				%str(%')el9.1%str(%') as measure
		from &wrktable..&taskprefix._el901a
	)by tmsis_passthrough;

	*el7.1;
	execute(
		create or replace view &wrktable..&taskprefix._el701a as
		select coalesce(wvr_id,'.') as waiver_id,
				coalesce(wvr_type_cd,'.') as waiver_type,
				pct
		from
		(
			select wvr_id,
					wvr_type_cd,
					count(distinct msis_ident_num) as pct						
			from &temptable..&taskprefix._tmsis_wvr_prtcptn_data
			group by wvr_id,wvr_type_cd
		) a
	)by tmsis_passthrough;

	execute(
		create table &wrktable..&taskprefix._el701 as
	    select  waiver_id,
				waiver_type,
				pct,
				%str(%')&state.%str(%') as submtg_state_cd,
				%str(%')el7.1%str(%') as measure
		from &wrktable..&taskprefix._el701a
	)by tmsis_passthrough;
    
%emptytable(el103);
%emptytable(el901);
%emptytable(el701);
						

	create table el_oth2 as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._el103);

	create table el901 as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._el901);

	create table el701 as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._el701);

	%dropwrkviews(&viewlist.);
	%dropwrktables(&tbllist.);

%tmsis_disconnect;
quit;
%status_check;
	
%mend _106;
