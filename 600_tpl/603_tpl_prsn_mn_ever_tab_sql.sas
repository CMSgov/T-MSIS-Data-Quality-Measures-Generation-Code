/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/



********************************************************;
* Join TPL and ENRLMT TIME SGMET FILE and create measure;
********************************************************;

%macro ever_tpl_elig_prsn_mn_sql();

%dropwrktables(ever_tpl_elig uniq_ever_tpl_elig)

execute(
	create table &wrktable..&taskprefix._ever_tpl_elig as
	select 
		 a.submtg_state_cd
        ,a.msis_ident_num
		,a.ever_tpl
		,coalesce(b.ever_eligible,0) as ever_eligible
        
	from &temptable..&taskprefix._ever_tpl a
	left join &temptable..&taskprefix._ever_elig b
	on a.msis_ident_num = b.msis_ident_num
	and a.submtg_state_cd = b.submtg_state_cd;
	) by tmsis_passthrough;

execute(
	create table &wrktable..&taskprefix._uniq_ever_tpl_elig as
	select 
		 submtg_state_cd
        ,msis_ident_num
		,max(ever_tpl) as ever_tpl
		,max(ever_eligible) as ever_eligible
        
	from &wrktable..&taskprefix._ever_tpl_elig
	group by submtg_state_cd, msis_ident_num ;
	) by tmsis_passthrough;

	%dropwrktables(ever_tpl_elig ever_tpl_elig_tab);

execute(
    create table &wrktable..&taskprefix._ever_tpl_elig_tab as
    select
         submtg_state_cd
		,sum(ever_tpl) as tpl1_5_denom
        ,sum(ever_eligible) as tpl1_5_numer
        ,round((sum(ever_eligible) / sum(ever_tpl)),4) as tpl1_5
    from &wrktable..&taskprefix._uniq_ever_tpl_elig
    group by submtg_state_cd
) by tmsis_passthrough;

    %dropwrktables(uniq_ever_tpl_elig);


%mend ever_tpl_elig_prsn_mn_sql;
