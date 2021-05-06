
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_el_sql;

    execute(
        create or replace temporary view &taskprefix._stplan_ot_l as
        select
         a.submtg_state_cd
        ,a.msis_ident_num
        ,case when b.submtg_state_cd is not null then 1 else 0 end as ot_clm_l_flag
        ,b.ot_hcpcs_1
        ,b.ot_hcpcs_2
        ,b.ot_hcpcs_5
        ,b.ot_val_hcpcs_txnmy
        ,1 as el_stplan_flag
        ,a.stplan_denom1
        ,a.stplan_denom2
        ,a.stplan_denom3
        
    from &taskprefix._utl_state_plan a
    left join &taskprefix._ot_prep_clm_l b
    on a.submtg_state_cd = b.submtg_state_cd and 
       a.msis_ident_num = b.msis_ident_num
	) by tmsis_passthrough;

    execute(
        create or replace temporary view &taskprefix._stplan_wvr_ot_l as
        select
		 a.*
		,case when b.submtg_state_cd is not null then 1 else 0 end as el_wvr_flag
		,b.wvr_denom1
		,&m_start. as tmsis_rptg_prd   
    from &taskprefix._stplan_ot_l a
    left join &taskprefix._utl_wvr b
    on a.submtg_state_cd = b.submtg_state_cd and 
       a.msis_ident_num = b.msis_ident_num
	) by tmsis_passthrough;

    execute(
        create or replace temporary view &taskprefix._utl_tab as
        select                                          
        /*unique keys and other identifiers*/
         submtg_state_cd
		,tmsis_rptg_prd
		,msis_ident_num
		,stplan_denom1
		,stplan_denom2
		,stplan_denom3
        /*standard measures calculated over all claim headers*/
        ,case when (stplan_denom1=1 and ot_clm_l_flag =1 )                       then 1 else 0 end as all2_2
        ,case when (stplan_denom2=1 and ot_clm_l_flag =1 )                       then 1 else 0 end as all2_3
        ,case when (stplan_denom2=1 and ot_clm_l_flag =1 and ot_hcpcs_1 =1)      then 1 else 0 end as all2_4
        ,case when (stplan_denom3=1 and ot_clm_l_flag =1)                        then 1 else 0 end as all2_5
        ,case when (stplan_denom3=1 and ot_clm_l_flag =1 and ot_hcpcs_2 =1)      then 1 else 0 end as all2_6
        ,case when (el_stplan_flag=1 and wvr_denom1 =1)                          then 1 else 0 end as denom_all2_7
        ,case when (el_stplan_flag=1 and wvr_denom1 =1 and ot_hcpcs_5 =1)        then 1 else 0 end as all2_7
        ,case when (el_stplan_flag=1 and (wvr_denom1 =1 or stplan_denom2=1))     then 1 else 0 end as denom_all2_8
        ,case when (el_stplan_flag=1 and (wvr_denom1 =1 or stplan_denom2=1)
                     and ot_val_hcpcs_txnmy =1) then 1 else 0 end as all2_8
	from &taskprefix._stplan_wvr_ot_l
	) by tmsis_passthrough;

execute(
	create or replace temporary view &taskprefix._utl_el  as
    select
         submtg_state_cd

        ,sum(all2_2) as all2_2_numer
        ,sum(stplan_denom1) as all2_2_denom
        ,case when sum(stplan_denom1) > 0 then round((sum(all2_2) / sum(stplan_denom1)),2)
			  else null end as all2_2

		,sum(all2_3) as all2_3_numer
        ,sum(stplan_denom2) as all2_3_denom
        ,case when sum(stplan_denom2) > 0 then round((sum(all2_3) / sum(stplan_denom2)),2)
			  else null end as all2_3

        ,sum(all2_4) as all2_4_numer
        ,sum(stplan_denom2) as all2_4_denom
        ,case when sum(stplan_denom2) > 0 then round((sum(all2_4) / sum(stplan_denom2)),2)
			  else null end as all2_4

        ,sum(all2_5) as all2_5_numer
        ,sum(stplan_denom3) as all2_5_denom
        ,case when sum(stplan_denom3) > 0 then round((sum(all2_5) / sum(stplan_denom3)),2)
			  else null end as all2_5

        ,sum(all2_6) as all2_6_numer
        ,sum(stplan_denom3) as all2_6_denom
        ,case when sum(stplan_denom3) > 0 then round((sum(all2_6) / sum(stplan_denom3)),2)
			  else null end as all2_6

        ,sum(all2_7) as all2_7_numer
        ,sum(denom_all2_7) as all2_7_denom
        ,case when sum(denom_all2_7) > 0 then round((sum(all2_7) / sum(denom_all2_7)),2)
			  else null end as all2_7

        ,sum(all2_8) as all2_8_numer
        ,sum(denom_all2_8) as all2_8_denom
        ,case when sum(denom_all2_8) > 0 then round((sum(all2_8) / sum(denom_all2_8)),2)
			  else null end as all2_8

    from  &taskprefix._utl_tab
    group by submtg_state_cd
    order by submtg_state_cd
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select
        submtg_state_cd
        , 'all2_2' 
        , '702' 
        , all2_2_numer
        , all2_2_denom
        , all2_2
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_3' 
        , '702' 
        ,all2_3_numer
        ,all2_3_denom
        ,all2_3
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;
        
    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_4' 
        , '702' 
        ,all2_4_numer
        ,all2_4_denom
        ,all2_4
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_5' 
        , '702' 
        ,all2_5_numer
        ,all2_5_denom
        ,all2_5
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_6' 
        , '702' 
        ,all2_6_numer
        ,all2_6_denom
        ,all2_6
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_7' 
        , '702' 
        ,all2_7_numer
        ,all2_7_denom
        ,all2_7
        , null
        , null
        from #temp.&taskprefix._utl_el
	) by tmsis_passthrough;

    execute (
        insert into &utl_output

        select 
        submtg_state_cd
        , 'all2_8' 
        , '702' 
        ,all2_8_numer
        ,all2_8_denom
        ,all2_8
        , null
        , null
        from #temp.&taskprefix._utl_el

	) by tmsis_passthrough;

    %insert_msr(msrid=all2_2);
    %insert_msr(msrid=all2_3);
    %insert_msr(msrid=all2_4);
    %insert_msr(msrid=all2_5);
    %insert_msr(msrid=all2_6);
    %insert_msr(msrid=all2_7);
    %insert_msr(msrid=all2_8);
    
%mend utl_el_sql;



