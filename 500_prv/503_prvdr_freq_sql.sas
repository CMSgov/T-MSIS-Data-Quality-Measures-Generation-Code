
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


/*frequency of provider classification types*/
%macro prvdr_freq_sql();


%let tblList = prvdr_txnmy prvdr_freq_t prvdr_freq_t2;

%dropwrktables(&tblList.);

execute(
   create table &wrktable..&taskprefix._prvdr_txnmy as
   select distinct submtg_state_cd,
   		  submtg_state_prvdr_id ,
		  prvdr_clsfctn_type_cd
   from &temptable..&taskprefix._tmsis_prvdr_txnmy_clsfctn
   ) by tmsis_passthrough;

execute(
	create table &wrktable..&taskprefix._prvdr_freq_t as
	select
		submtg_state_cd,
		submtg_state_prvdr_id,
		prvdr_clsfctn_type_cd,
		case when (prvdr_clsfctn_type_cd in ('1','2','3','4')) then 1 else 0 end as prvdr_clsfctn_any,
		case when ((prvdr_clsfctn_type_cd not in ('1','2','3','4')) or 
                   (prvdr_clsfctn_type_cd is null) 
                   ) then 1 else 0 end as prvdr_clsfctn_none
	from &wrktable..&taskprefix._prvdr_txnmy
) by tmsis_passthrough;

execute(
	create table &wrktable..&taskprefix._prvdr_freq_t2 as
	select
		submtg_state_cd,
		submtg_state_prvdr_id,
		max(prvdr_clsfctn_any) as prvdr_clsfctn_any,
		max(prvdr_clsfctn_none) as prvdr_clsfctn_none
	from &wrktable..&taskprefix._prvdr_freq_t
	group by submtg_state_cd,
		submtg_state_prvdr_id
) by tmsis_passthrough;

%dropwrktables(prvdr_freq_tot prvdr_freq);

execute(
	create table &wrktable..&taskprefix._prvdr_freq_tot as
	select
		submtg_state_cd,
		sum(prvdr_clsfctn_any) as prvdr_clsfctn_any,
		sum(prvdr_clsfctn_none) as prvdr_clsfctn_none,
		count(submtg_state_cd) as prvdr_clsfctn_tot
	from &wrktable..&taskprefix._prvdr_freq_t2
	group by submtg_state_cd
) by tmsis_passthrough;

execute(
	create table &wrktable..&taskprefix._prvdr_freq as
	select
		submtg_state_cd,
		prvdr_clsfctn_type_cd,
		count(submtg_state_cd) as prv4_1
	from &wrktable..&taskprefix._prvdr_txnmy
	where prvdr_clsfctn_type_cd in ('1','2','3','4')
	group by submtg_state_cd, prvdr_clsfctn_type_cd
) by tmsis_passthrough;

%dropwrktables(&tblList.);

%emptytable(prvdr_freq_tot);
%emptytable(prvdr_freq);


%mend prvdr_freq_sql;
