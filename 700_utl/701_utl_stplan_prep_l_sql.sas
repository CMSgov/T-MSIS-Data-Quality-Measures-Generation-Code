
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_stplan_sql();

execute(
    create or replace temporary view &taskprefix._utl_state_plan as
	select                                                 
        /*unique keys and other identifiers*/
         submtg_state_cd
        ,msis_ident_num    
        ,max(case when (state_plan_optn_type_cd ='01') then 1 else 0 end) as stplan_denom1
        ,max(case when (state_plan_optn_type_cd ='02') then 1 else 0 end) as stplan_denom2
		,max(case when (state_plan_optn_type_cd ='03') then 1 else 0 end) as stplan_denom3

    from &temptable..&taskprefix._tmsis_state_plan_prtcptn 
	group by submtg_state_cd, msis_ident_num
	) by tmsis_passthrough;

%mend;
