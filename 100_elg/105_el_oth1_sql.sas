/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 105_el_oth1_sql.sas
 Project: MACBIS Task 2

 Author:  Sabitha Gopalsamy
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: el_oth1 SAS work dataset
         
 Modifications: 9/11 : SGO updated  #EL10.4 for V1.2 updates
				1/18/17 : SGO Added Emptytable macros
 
 ******************************************************************************************/

/*Other: Sections 5.1-5.6*/
/****************************************************************************************************/
%macro _105;

%let viewlist = el1003a el1003c el1004a el120t1  el314t1;
%let tbllist = el1003 el1004 el120t el314t;

proc sql;
	%tmsis_connect;

	%dropwrkviews(&viewlist.);
	%dropwrktables(&tbllist.);

*el10.3;

execute(
		create or replace view &wrktable..&taskprefix._el1003a as
		select count(distinct msis_ident_num) as denom
				,1 as merger
	    from &temptable..&taskprefix._tmsis_mc_prtcptn_data
		where enrld_mc_plan_type_cd not in ('00','99') or enrld_mc_plan_type_cd is null
	) by tmsis_passthrough;


execute(
		create or replace view &wrktable..&taskprefix._el1003c as
		select count(distinct msis_ident_num) as numer
			  ,1 as merger
		from 
		(
		select  msis_ident_num,
			count(distinct enrld_mc_plan_type_cd) as subset
	    from &temptable..&taskprefix._tmsis_mc_prtcptn_data    
		where (enrld_mc_plan_type_cd not in ('00','99') or enrld_mc_plan_type_cd is null)
		and msis_ident_num is not null
		group by msis_ident_num
		) a
		where subset > 1
	)by tmsis_passthrough;


execute(
		create table &wrktable..&taskprefix._el1003 as
	    select case when b.denom > 0 then 
					round(a.numer/b.denom,2)
					else null end as pct,	          
			   a.numer as numer,
			   b.denom as denom,
			   %str(%')&state.%str(%') as submtg_state_cd,
			   %str(%')el10.3%str(%') as measure
	    from &wrktable..&taskprefix._el1003c as a
		inner join &wrktable..&taskprefix._el1003a as b
		on a.merger = b.merger
	)by tmsis_passthrough;
        

*el10.4;

execute(
		create or replace view &wrktable..&taskprefix._el1004a as
    	select 
			 msis_ident_num
			,count(distinct mc_plan_id) as count_plans			
	    from &temptable..&taskprefix._tmsis_mc_prtcptn_data 
		where (%nmisslogic(mc_plan_id,12) = 1) and enrld_mc_plan_type_cd <> '00'
		group by msis_ident_num
	)by tmsis_passthrough;


execute(
	create table &wrktable..&taskprefix._el1004 as
	select
		 count(1) as denom
		,sum(count_plans) as numer
		,case when count(1) > 0 then round(sum(count_plans)/count(1),2)
		 	else null end as pct
		,%str(%')&state.%str(%') as submtg_state_cd
		,%str(%')el10.4%str(%') as measure
	from &wrktable..&taskprefix._el1004a
	) by tmsis_passthrough;


execute(
		create or replace view &wrktable..&taskprefix._el120t1 as
		select
			 ssn_num
			,count(distinct msis_ident_num) as count_ids
		from &temptable..&taskprefix._tmsis_var_dmgrphc_elgblty
		where ssn_num is not null
		group by ssn_num
	)by tmsis_passthrough;


execute(
		create table &wrktable..&taskprefix._el120t as	
	    select  %str(%')&state.%str(%') as submtg_state_cd,
				%str(%')el1.20%str(%') as measure,
				count(1) as denom,
				sum(case when count_ids > 1 then 1 else 0 end) as numer,
				round(sum(case when count_ids > 1 then 1 else 0 end)/count(1), 3) as pct
		from &wrktable..&taskprefix._el120t1
	)by tmsis_passthrough;

execute(
		create or replace view &wrktable..&taskprefix._el314t1 as	
	    select msis_ident_num,
			   count(1) as count_ids
		from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
		group by msis_ident_num
	)by tmsis_passthrough;

	
execute(
		create table &wrktable..&taskprefix._el314t as	
	    select  %str(%')&state.%str(%') as submtg_state_cd,
				%str(%')el3.14%str(%') as measure,
				count(1) as denom,
				sum(case when count_ids = 1 then 0 else 1 end) as numer,
				round(sum(case when count_ids = 1 then 0 else 1 end)/count(1), 3) as pct
		from &wrktable..&taskprefix._el314t1
	)by tmsis_passthrough;
    
 %emptytable(el1003);
 %emptytable(el1004);
 %emptytable(el120t);
 %emptytable(el314t);    

%let pull = %str(submtg_state_cd, measure, numer as numerator, denom as denominator, pct as pct);

  /*extract measure from redshift into sas*/
    create table el_oth1 as
	 select * from connection to tmsis_passthrough(
	 select &pull. from &wrktable..&taskprefix._el1003
	 union all
	 select &pull. from &wrktable..&taskprefix._el1004
	 union all
	 select &pull. from &wrktable..&taskprefix._el120t
	 union all
	 select &pull. from &wrktable..&taskprefix._el314t
	);	

	%dropwrkviews(&viewlist.);
	%dropwrktables(&tbllist.);

%tmsis_disconnect;
quit;
%status_check;

%mend _105;
