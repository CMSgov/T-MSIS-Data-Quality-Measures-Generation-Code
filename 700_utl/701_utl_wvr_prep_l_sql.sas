
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_wvr_sql();

execute(
	create or replace temporary view &taskprefix._utl_wvr as 
    select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,msis_ident_num
        ,max(case when (wvr_type_cd in ('06','07','08','09','10','11','12','13',
										'14','15','16','17','18','19','20','33')) then 1 else 0 end) as wvr_denom1
       
    from &temptable..&taskprefix._tmsis_wvr_prtcptn_data  
	group by submtg_state_cd, msis_ident_num
	) by tmsis_passthrough;

%mend;
