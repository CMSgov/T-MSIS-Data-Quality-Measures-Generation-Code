/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


%macro tpl_prsn_hi_sql();

%let tpl_cvrg_typ = %str('01','02','03','04','05','06','07','08','09','10',
                         '11','12','13','14','15','16','17','18','19','20',
                         '21','22','23','98');

%let tpl_insrnc_typ = %str('01','02','03','04','05','06','07','08','09','10',
                           '11','12','13','14','15','16');


%dropwrktables(tpl_prsn_hi tpl_prsn_hi_tab);

execute(
create table &wrktable..&taskprefix._tpl_prsn_hi as
    select
         submtg_state_cd
        ,msis_ident_num
        ,max(case when (cvrg_type_cd in ( &tpl_cvrg_typ. )) then 1 else 0 end) as tpl1_2
        ,max(case when (insrnc_plan_type_cd in (&tpl_insrnc_typ.)) then 1 else 0 end) as tpl1_3
    from &temptable..&taskprefix._tmsis_tpl_mdcd_prsn_hi  
	group by submtg_state_cd, msis_ident_num
) by tmsis_passthrough;


execute(
    create table &wrktable..&taskprefix._tpl_prsn_hi_tab as
    select
         submtg_state_cd
        ,sum(tpl1_2) as tpl1_2
        ,sum(tpl1_3) as tpl1_3

    from &wrktable..&taskprefix._tpl_prsn_hi
    group by submtg_state_cd
) by tmsis_passthrough;

%dropwrktables(tpl_prsn_hi);

%mend tpl_prsn_hi_sql;



