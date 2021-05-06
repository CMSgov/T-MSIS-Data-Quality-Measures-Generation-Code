/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


%macro tpl_ot_6_7_sql();

%macro tpl_ot_6_7_clm(clmcat, tblnum);

%dropwrktables(ot_prep_clm_&clmcat. ot_rollup_clm_&clmcat. ot_clm_&clmcat.);

execute(
create table &wrktable..&taskprefix._ot_prep_clm_&clmcat. as
    select
         submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
		,orgnl_line_num
		,adjstmt_line_num
		,line_adjstmt_ind

        ,case when  stc_cd = '121' and
                    srvc_endg_dt between tmsis_rptg_prd and last_day(tmsis_rptg_prd)
                    then 1 else 0 end as tpl&tblnum._1

        ,case when  stc_cd = '121' and
                    srvc_endg_dt > last_day(tmsis_rptg_prd) 
					then 1 else 0 end as tpl&tblnum._2

        ,case when  stc_cd = '121' and
                    srvc_endg_dt < date_sub(tmsis_rptg_prd,30) 
					then 1 else 0 end as tpl&tblnum._3

        ,case when  stc_cd = '121' and
                    srvc_endg_dt < tmsis_rptg_prd and
                    srvc_endg_dt >= date_sub(tmsis_rptg_prd,30)
					then 1 else 0 end as tpl&tblnum._4

    from &temptable..&taskprefix._base_cll_ot
    where claim_cat_&clmcat. = 1
	and childless_header_flag = 0
	) by tmsis_passthrough;

	execute(
    create table &wrktable..&taskprefix._ot_rollup_clm_&clmcat. as
    select
         submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
	    ,orgnl_line_num
        ,adjstmt_line_num
		,line_adjstmt_ind
        %do i = 1 %to 4;
        ,max(tpl&tblnum._&i.) as tpl&tblnum._&i.
        %end;
    from &wrktable..&taskprefix._ot_prep_clm_&clmcat.
    group by submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
				orgnl_line_num, adjstmt_line_num, line_adjstmt_ind
	) by tmsis_passthrough;

	execute(
    create table &wrktable..&taskprefix._ot_clm_&clmcat. as
    select
         submtg_state_cd
        %do i = 1 %to 4;
        ,sum(tpl&tblnum._&i.) as tpl&tblnum._&i.
        %end;
    from &wrktable..&taskprefix._ot_rollup_clm_&clmcat.
    group by submtg_state_cd
	) by tmsis_passthrough;

    %dropwrktables(ot_prep_clm_&clmcat. ot_rollup_clm_&clmcat.);

%mend tpl_ot_6_7_clm;

%tpl_ot_6_7_clm(d, 6);
%tpl_ot_6_7_clm(j, 7);

%dropwrktables(ot_6_7);

execute(
create table &wrktable..&taskprefix._ot_6_7 as
select
	 a.submtg_state_cd
	%do i = 1 %to 4;
	,tpl6_&i.
	,tpl7_&i.
	%end;
from &wrktable..&taskprefix._ot_clm_d a
left join &wrktable..&taskprefix._ot_clm_j j
on a.submtg_state_cd = j.submtg_state_cd
) by tmsis_passthrough;

%dropwrktables(ot_clm_d ot_clm_j);

%mend tpl_ot_6_7_sql;
