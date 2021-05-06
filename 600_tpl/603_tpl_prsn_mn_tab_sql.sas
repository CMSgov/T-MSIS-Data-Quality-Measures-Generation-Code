/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


%macro tpl_prsn_mn_sql();

%dropwrktables(tpl_prsn_mn tpl_prsn_mn_tab);

execute(
create table &wrktable..&taskprefix._tpl_prsn_mn as
    select
		 submtg_state_cd
        ,msis_ident_num
        ,max(case when (tpl_insrnc_cvrg_ind = '1' or tpl_othr_cvrg_ind = '1') 
			then 1 else 0 end) as tpl1_4
		,max(case when elgbl_prsn_mn_efctv_dt is not null then 1 else 0 end) as tpl1_1
    from  &temptable..&taskprefix._tmsis_tpl_mdcd_prsn_mn  
	group by submtg_state_cd, msis_ident_num
	) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._tpl_prsn_mn_tab as
    select
         submtg_state_cd
		,sum(tpl1_1) as tpl1_1
        ,sum(tpl1_4) as tpl1_4

    from &wrktable..&taskprefix._tpl_prsn_mn
    group by submtg_state_cd
) by tmsis_passthrough;

%dropwrktables(tpl_prsn_mn);

%mend tpl_prsn_mn_sql;
