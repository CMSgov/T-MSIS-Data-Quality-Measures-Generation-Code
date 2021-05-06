/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 109_el_oth5_sql.sas
 Project: MACBIS Task 2

 Author: Kerianne Hourihan 
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: el_13_1 SAS work dataset
         
 Modifications: 10/17/18: KHourihan created for new measure EL13.1
 ******************************************************************************************/


%macro _109;

proc sql;

%tmsis_connect;

%dropwrkviews(el_13_1_a el_13_1_b);
%dropwrktables(el_13_1);

execute(
	create or replace view &wrktable..&taskprefix._el_13_1_a as
	select distinct
		 msis_ident_num
		,mc_plan_id
	from &temptable..&taskprefix._tmsis_mc_prtcptn_data
	where mc_plan_id rlike '[A-Za-z1-9]'
) by tmsis_passthrough;

execute(
	create or replace view &wrktable..&taskprefix._el_13_1_b as
	select
		 a.msis_ident_num
		,a.mc_plan_id
		,b.afltd_pgm_id
	from &wrktable..&taskprefix._el_13_1_a a
	left join
	(select distinct
		 afltd_pgm_id
	from &temptable..&taskprefix._tmsis_prvdr_afltd_pgm
	where afltd_pgm_type_cd = '2'
	) b
	on a.mc_plan_id = b.afltd_pgm_id
) by tmsis_passthrough;

execute(
	create table &wrktable..&taskprefix._el_13_1 as
	select
	     %str(%')&state.%str(%') as submtg_state_cd
		,'EL13.1' as measure
		,numer
		,denom
		,case when denom > 0 then round((numer/denom), 2) else null end as pct
	from 
	(select
		 count(msis_ident_num) as denom
		,sum(case when afltd_pgm_id is null then 1 else 0 end) as numer
		from &wrktable..&taskprefix._el_13_1_b
	) b
) by tmsis_passthrough;

%emptytable(el_13_1);

create table EL_13_1 as
select submtg_state_cd, measure, numer as numerator, denom as denominator, pct from connection to tmsis_passthrough
(select * from &wrktable..&taskprefix._el_13_1);

%dropwrkviews(el_13_1_a el_13_1_b);
%dropwrktables(el_13_1);

%tmsis_disconnect;

quit;
%status_check;

%mend _109;

   


