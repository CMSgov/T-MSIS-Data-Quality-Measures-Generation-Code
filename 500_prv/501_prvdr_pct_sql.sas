/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro prvdr_pct_sql();


%let tblList = prvdr_prep all_clms_prvdrs uniq_clms_prvdrs_file uniq_clms_prvdrs prv_clm
               clm_prv_tab clm_prv_ip clm_prv_tab_ip  clm_prv_lt clm_prv_tab_lt
               clm_prv_ot clm_prv_tab_ot clm_prv_rx clm_prv_tab_rx prv_addtyp_prep
               prv_addtyp_rollup prv_addtyp prv_idtyp_prep prv_idtyp prv_mdcd_prep prv_mdcd
			   prv_id_npi prvdr_npi_txnmy prvdr_npi_txnmy2
			   prv2_10_denom prv2_10_numer prv2_10_msr
               ;
               
%dropwrktables(&tblList.);

*prv1.1;
*obtain unique providers in provider file;
execute(
	create table &wrktable..&taskprefix._prvdr_prep as
	select distinct 
	           submtg_state_cd, 
	           submtg_state_prvdr_id as prvdr_id,
	           prvdr_lctn_id
	    from &temptable..&taskprefix._tmsis_prvdr_lctn_cntct
	    where prvdr_adr_type_cd in ('3','4')
	         and %nmsng(submtg_state_prvdr_id,30)
	         and %nmsng(prvdr_lctn_id,5)
			 
) by tmsis_passthrough;   


* obtain unique providers in the claim level files ;
execute(
    create table &wrktable..&taskprefix._all_clms_prvdrs as
    select 
			submtg_state_cd, 
           	blg_prvdr_num,
           	prvdr_lctn_id,
			'ip' as sourcefile
    from &temptable..&taskprefix._base_clh_ip
    where %nmsng(blg_prvdr_num,30)
         and %nmsng(prvdr_lctn_id,5)
	union
    select 
			submtg_state_cd, 
           	blg_prvdr_num,
           	prvdr_lctn_id,
			'lt' as sourcefile
    from &temptable..&taskprefix._base_clh_lt
    where %nmsng(blg_prvdr_num,30)
         and %nmsng(prvdr_lctn_id,5)
	union
    select 
			submtg_state_cd, 
           	blg_prvdr_num,
           	prvdr_lctn_id,
			'ot' as sourcefile
    from &temptable..&taskprefix._base_clh_ot
    where %nmsng(blg_prvdr_num,30)
         and %nmsng(prvdr_lctn_id,5)
	union
    select 
			submtg_state_cd, 
           	blg_prvdr_num,
           	prvdr_lctn_id,
			'rx' as sourcefile
    from &temptable..&taskprefix._base_clh_rx
    where %nmsng(blg_prvdr_num,30)
         and %nmsng(prvdr_lctn_id,5)
    ) by tmsis_passthrough;


	execute(
	create table &wrktable..&taskprefix._uniq_clms_prvdrs_file as
	select distinct
		 submtg_state_cd
		,blg_prvdr_num
		,prvdr_lctn_id
		,sourcefile
	from &wrktable..&taskprefix._all_clms_prvdrs
	) by tmsis_passthrough;

	execute(
	create table &wrktable..&taskprefix._uniq_clms_prvdrs as
	select distinct
		 submtg_state_cd
		,blg_prvdr_num
		,prvdr_lctn_id
		
	from &wrktable..&taskprefix._all_clms_prvdrs
	) by tmsis_passthrough;


	execute(
	create table &wrktable..&taskprefix._prv_clm as
	select
			 a.submtg_state_cd
			,a.prvdr_id
          	,sum(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as tot_match_loc_cnt
			,count(a.prvdr_lctn_id) as tot_loc_cnt
			,avg(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as pct_match_loc 
    from &wrktable..&taskprefix._prvdr_prep as a 
	left join &wrktable..&taskprefix._uniq_clms_prvdrs as b
    on  a.submtg_state_cd = b.submtg_state_cd and
        a.prvdr_id = b.blg_prvdr_num and
        a.prvdr_lctn_id = b.prvdr_lctn_id
	group by a.submtg_state_cd, a.prvdr_id
	) by tmsis_passthrough;


      *prv1.1 stat;

	execute(
    create table &wrktable..&taskprefix._clm_prv_tab as
    select  submtg_state_cd,                            
			sum(case when pct_match_loc = 1 then 1 else 0 end) as prv1_1_numer,
           count(prvdr_id)  as prv1_1_denom,
           round((sum(case when pct_match_loc = 1 then 1 else 0 end)
					/count(prvdr_id)), 2) as prv1_1,

           sum(tot_match_loc_cnt) as prv1_2_numer,
           sum(tot_loc_cnt)  as prv1_2_denom,
           round(sum(tot_match_loc_cnt)/sum(tot_loc_cnt), 2) as prv1_2
    from &wrktable..&taskprefix._prv_clm
	group by submtg_state_cd
    ) by tmsis_passthrough;

 *prv1.3 - prv1.4 - prv1.5 - prv1.6;
 *prv1.7 - prv1.8 - prv1.9 - prv1.10;
%macro combo(tab=,tab2=,ft=);

     * merge the provider and claims and flag the records where location match ;
	execute(
    create table &wrktable..&taskprefix._clm_prv_&ft. as
    select   a.submtg_state_cd
			,a.blg_prvdr_num as prvdr_id
          	,sum(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as tot_match_loc_cnt
			,count(a.prvdr_lctn_id) as tot_loc_cnt
			,avg(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as pct_match_loc 
    from &wrktable..&taskprefix._uniq_clms_prvdrs_file a 
	left join &wrktable..&taskprefix._prvdr_prep b
    on  a.submtg_state_cd = b.submtg_state_cd and
        a.blg_prvdr_num = b.prvdr_id and
        a.prvdr_lctn_id = b.prvdr_lctn_id
	where a.sourcefile = %str(%')&ft.%str(%')
	group by a.submtg_state_cd, a.blg_prvdr_num
    ) by tmsis_passthrough;

	execute(
    create table &wrktable..&taskprefix._clm_prv_tab_&ft. as
    select
		   submtg_state_cd,
           sum(tot_match_loc_cnt) as prv1_&tab._numer,
           sum(tot_loc_cnt)  as prv1_&tab._denom,
           round((sum(tot_match_loc_cnt)/sum(tot_loc_cnt)), 2) as prv1_&tab.,

           sum(case when pct_match_loc = 1 then 1 else 0 end) as prv1_&tab2._numer,
           count(prvdr_id)  as prv1_&tab2._denom,
           round((sum(case when pct_match_loc = 1 then 1 else 0 end)
					/count(prvdr_id)), 2) as prv1_&tab2.

    from &wrktable..&taskprefix._clm_prv_&ft.
	group by submtg_state_cd
    ) by tmsis_passthrough;

 
%mend combo;

%combo(tab=3,tab2=7, ft=ot);
%combo(tab=4,tab2=8, ft=ip);
%combo(tab=5,tab2=9, ft=lt);
%combo(tab=6,tab2=10, ft=rx);


execute(
    create table &wrktable..&taskprefix._prv_addtyp_prep as
    select submtg_state_cd,  
           submtg_state_prvdr_id as prvdr_id,
           case when (prvdr_adr_type_cd = '1') then 1 else 0 end as prv111_1,
           case when (prvdr_adr_type_cd = '3') then 1 else 0 end as prv113_1,
           case when (prvdr_adr_type_cd = '4') then 1 else 0 end as prv115_1
    from &temptable..&taskprefix._tmsis_prvdr_lctn_cntct
    where  %nmsng(submtg_state_prvdr_id,30)
    ) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._prv_addtyp_rollup as
    select submtg_state_cd,
           prvdr_id,
           max(prv111_1) as prv111_1,
           max(prv113_1) as prv113_1,
           max(prv115_1) as prv115_1
    from &wrktable..&taskprefix._prv_addtyp_prep
    group by submtg_state_cd, prvdr_id
    ) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._prv_addtyp as
    select submtg_state_cd,
		   sum (prv111_1) as prv1_11_numer,
           count(submtg_state_cd) as prv1_11_denom,
           sum (prv113_1) as prv1_13_numer,
           count(submtg_state_cd) as prv1_13_denom,
           sum (prv115_1) as prv1_15_numer,
           count(submtg_state_cd) as prv1_15_denom,
           round((sum(prv111_1) / count(submtg_state_cd)), 2) as prv1_11,
           round((sum(prv113_1) / count(submtg_state_cd)), 2) as prv1_13,
           round((sum(prv115_1) / count(submtg_state_cd)), 2) as prv1_15
    from &wrktable..&taskprefix._prv_addtyp_rollup
	group by submtg_state_cd
    ) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._prv_idtyp_prep as
    select submtg_state_cd,
           submtg_state_prvdr_id as prvdr_id
           %do i = 1  %to 8;
                 ,max(case when cast(coalesce(prvdr_id_type_cd,'9') as int) = &i. then 1 else 0 end) as prv2_&i._1
           %end;
    from &temptable..&taskprefix._tmsis_prvdr_id
    where submtg_state_prvdr_id is not null                	     
	group by submtg_state_cd, submtg_state_prvdr_id
    ) by tmsis_passthrough;

    
execute(
    create table &wrktable..&taskprefix._prv_idtyp as
	select
			submtg_state_cd
           %do i = 1 %to 8;
                ,sum(prv2_&i._1) as prv2_&i._numer
                ,count(submtg_state_cd) as prv2_&i._denom
                ,round((sum(prv2_&i._1) / count(submtg_state_cd)),2) as prv2_&i.
           %end;
    from &wrktable..&taskprefix._prv_idtyp_prep
	group by submtg_state_cd
    ) by tmsis_passthrough;
    

/***prv 2.9**/

%*dropwrkviews(prv_id_npi prvdr_npi_txnmy prvdr_npi_txnmy2);

execute(
    create table &wrktable..&taskprefix._prv_id_npi as
    select submtg_state_cd,
           submtg_state_prvdr_id as prvdr_id
          ,max(case when prvdr_id_type_cd = '2' then 1 else 0 end) as prv2_9_denom0
          
    from &temptable..&taskprefix._tmsis_prvdr_id
    where submtg_state_prvdr_id is not null 	     
	group by submtg_state_cd, submtg_state_prvdr_id
    ) by tmsis_passthrough;


execute(
   create table &wrktable..&taskprefix._prvdr_npi_txnmy as
   select a.submtg_state_cd,
          a.prvdr_id,
          a.prv2_9_denom0,          
          case when a.prv2_9_denom0=1 and 
                   (b.prvdr_clsfctn_type_eq1 =0 or b.prvdr_clsfctn_type_eq1 is null) then 1 else 0 end as prv2_9_numer0

   from &wrktable..&taskprefix._prv_id_npi a
   left join 

    (select submtg_state_cd,
   		    submtg_state_prvdr_id as prvdr_id,
		    max(case when prvdr_clsfctn_type_cd=1 then 1 else 0 end) as prvdr_clsfctn_type_eq1
      from &temptable..&taskprefix._tmsis_prvdr_txnmy_clsfctn 
      group by submtg_state_cd, submtg_state_prvdr_id ) b

   on a.submtg_state_cd =b.submtg_state_cd and
      a.prvdr_id=b.prvdr_id
   ) by tmsis_passthrough;
   
execute(
    create table &wrktable..&taskprefix._prvdr_npi_txnmy2 as
	select
			submtg_state_cd
           ,prv2_9_numer
           ,prv2_9_denom
           ,case when prv2_9_denom >0 then round(prv2_9_numer / prv2_9_denom,2) 
                 else null end as prv2_9          
    from 
		(select
			submtg_state_cd
           ,sum(prv2_9_numer0) as prv2_9_numer
           ,sum(prv2_9_denom0) as prv2_9_denom
		from &wrktable..&taskprefix._prvdr_npi_txnmy
		group by submtg_state_cd
		) a
    ) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._prv_mdcd_prep as
    select  submtg_state_cd 
           ,submtg_state_prvdr_id as prvdr_id
           ,max(case when cast(coalesce(prvdr_mdcd_enrlmt_stus_cd,'99') as int) between 20 and 24
				then 1 else 0 end) as prv3_1_1
           ,max(case when cast(coalesce(prvdr_mdcd_enrlmt_stus_cd,'99') as int) between 60 and 83
                then 1 else 0 end) as prv3_2_1
           ,max(case when (state_plan_enrlmt_cd = '1') then 1 else 0 end) as prv3_3_1
           ,max(case when (state_plan_enrlmt_cd = '2') then 1 else 0 end) as prv3_4_1
           ,max(case when (state_plan_enrlmt_cd = '3') then 1 else 0 end) as prv3_5_1
           ,max(case when (state_plan_enrlmt_cd = '4') then 1 else 0 end) as prv3_6_1
    from &temptable..&taskprefix._tmsis_prvdr_mdcd_enrlmt
    where submtg_state_prvdr_id is not null 
	group by submtg_state_cd, submtg_state_prvdr_id
	) by tmsis_passthrough;      
    
execute(
    create table &wrktable..&taskprefix._prv_mdcd as
    select submtg_state_cd
           %do i = 1  %to 6;
                ,count(submtg_state_cd) as prv3_&i._denom
                ,sum(prv3_&i._1) as prv3_&i._numer
                ,round((sum(prv3_&i._1) / count(submtg_state_cd)), 2) as prv3_&i.
           %end;
    from &wrktable..&taskprefix._prv_mdcd_prep
    group by submtg_state_cd
    ) by tmsis_passthrough;
    
    * based on prv2_9;
    %*dropwrkviews(prv2_10_denom prv2_10_numer prv2_10_msr);
    * denominator;
    execute (
        create table &wrktable..&taskprefix._prv2_10_denom as
        select a.submtg_state_cd,
        a.submtg_state_prvdr_id
        ,max(case when a.fac_grp_indvdl_cd = '03' and a.ever_provider = 1 then 1 else 0 end) as prv2_10_denom0
        from (select * from &temptable..&taskprefix._ever_tmsis_prvdr_attr_mn where submtg_state_prvdr_id is not null) a 
		group by a.submtg_state_cd, a.submtg_state_prvdr_id
        ) by tmsis_passthrough;
    * numerator;
    execute (
        create table &wrktable..&taskprefix._prv2_10_numer as
        select a.submtg_state_cd,
        a.submtg_state_prvdr_id,
        a.prv2_10_denom0,
        case when a.prv2_10_denom0=1 and b.numer_count >= 2 then 1 else 0 end as prv2_10_numer0
        from &wrktable..&taskprefix._prv2_10_denom a
        left join
        (select submtg_state_cd, submtg_state_prvdr_id,
        count(distinct case when prvdr_id_type_cd = '2' and ever_provider_id = 1 and %nmsng(prvdr_id, 12) then prvdr_id else null end) as numer_count
        from &temptable..&taskprefix._ever_tmsis_prvdr_id
        where submtg_state_prvdr_id is not null                	     
        group by submtg_state_cd, submtg_state_prvdr_id) b
        on a.submtg_state_cd=b.submtg_state_cd and a.submtg_state_prvdr_id=b.submtg_state_prvdr_id
        ) by tmsis_passthrough;
    * measure;
    execute(
        create table &wrktable..&taskprefix._prv2_10_msr as
        select
        submtg_state_cd
        ,prv2_10_numer
        ,prv2_10_denom
        ,case when prv2_10_denom >0 then round(prv2_10_numer / prv2_10_denom,2) 
        else null end as prv2_10          
        from 
        (select
        submtg_state_cd
        ,sum(prv2_10_numer0) as prv2_10_numer
        ,sum(prv2_10_denom0) as prv2_10_denom
        from &wrktable..&taskprefix._prv2_10_numer
        group by submtg_state_cd
        ) a
        ) by tmsis_passthrough;


%emptytable(clm_prv_tab);
%emptytable(clm_prv_tab_ip);
%emptytable(clm_prv_tab_lt);
%emptytable(clm_prv_tab_ot);
%emptytable(clm_prv_tab_rx);
%emptytable(prv_addtyp);
%emptytable(prv_idtyp);
%emptytable(prv_mdcd);
%emptytable(prvdr_npi_txnmy2);
    
%emptytable(prv2_10_msr);

%dropwrktables(prvdr_pct);
execute(
	create table &wrktable..&taskprefix._prvdr_pct as
	select
		 a.submtg_state_cd
		%do i = 1 %to 11;
			,prv1_&i._numer
			,prv1_&i._denom
			,prv1_&i
		%end;
		    ,prv1_13_numer
			,prv1_13_denom
			,prv1_13
			,prv1_15_numer
			,prv1_15_denom
			,prv1_15
		%do i = 1 %to 10;
			,prv2_&i._numer
			,prv2_&i._denom
			,prv2_&i
		%end;
		%do i = 1 %to 6;
			,prv3_&i._numer
			,prv3_&i._denom
			,prv3_&i
		%end;
from &wrktable..&taskprefix._clm_prv_tab a
left join &wrktable..&taskprefix._clm_prv_tab_ip b on a.submtg_state_cd = b.submtg_state_cd
left join &wrktable..&taskprefix._clm_prv_tab_lt c on a.submtg_state_cd = c.submtg_state_cd
left join &wrktable..&taskprefix._clm_prv_tab_ot d on a.submtg_state_cd = d.submtg_state_cd
left join &wrktable..&taskprefix._clm_prv_tab_rx e on a.submtg_state_cd = e.submtg_state_cd
left join &wrktable..&taskprefix._prv_addtyp f on a.submtg_state_cd = f.submtg_state_cd 
left join &wrktable..&taskprefix._prv_idtyp g on a.submtg_state_cd = g.submtg_state_cd
left join &wrktable..&taskprefix._prv_mdcd h on a.submtg_state_cd = h.submtg_state_cd
left join &wrktable..&taskprefix._prvdr_npi_txnmy2 i on a.submtg_state_cd = i.submtg_state_cd 
left join &wrktable..&taskprefix._prv2_10_msr j on a.submtg_state_cd = j.submtg_state_cd

) by tmsis_passthrough;

%dropwrktables(&tblList.);
%*dropwrkviews(prv_id_npi prvdr_npi_txnmy prvdr_npi_txnmy2);
%*dropwrkviews(prv2_10_denom prv2_10_numer prv2_10_msr);

%mend prvdr_pct_sql;
