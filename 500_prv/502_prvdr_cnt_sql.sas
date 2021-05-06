/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/



%macro prvdr_cnt_sql();


%dropwrkviews(sumprv);
execute(
   create or replace view &wrktable..&taskprefix._sumprv as
   select submtg_state_cd,
   		  submtg_state_prvdr_id  as prvdr_id,
		  max(case when (fac_grp_indvdl_cd = '01' ) then 1 else 0 end) as sumprv1_1,
		  max(case when (fac_grp_indvdl_cd = '03' ) then 1 else 0 end) as sumprv2_1,
		  max(case when (fac_grp_indvdl_cd = '02' ) then 1 else 0 end) as sumprv3_1   
   from &temptable..&taskprefix._tmsis_prvdr_attr_mn
   where submtg_state_prvdr_id is not null
   group by submtg_state_cd, submtg_state_prvdr_id
) by tmsis_passthrough; 

%dropwrktables(sumprv_tab);
execute(
    create table &wrktable..&taskprefix._sumprv_tab as
    select submtg_state_cd,
           sum (sumprv1_1) as sumprv_1,
		   sum (sumprv2_1) as sumprv_2,
		   sum (sumprv3_1) as sumprv_3                     
		   
    from &wrktable..&taskprefix._sumprv
    group by submtg_state_cd
    ) by tmsis_passthrough;    

%dropwrkviews(sumprv);

%emptytable(sumprv_tab);

%mend prvdr_cnt_sql;
