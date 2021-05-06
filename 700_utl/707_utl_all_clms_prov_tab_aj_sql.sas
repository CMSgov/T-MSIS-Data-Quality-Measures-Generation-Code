
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro get_prov_ever_enrld_sql;

/** get unique providers in tmsis_prvdr_mdcd_enrlmt ever enrolled file */

 
execute(
	create or replace temporary view &taskprefix._prvdr_ever_enrld as
	select distinct 
	       submtg_state_cd, 
	       submtg_state_prvdr_id as prvdr_id,
		   prvdr_mdcd_efctv_dt,
		   prvdr_mdcd_end_dt
	    from &temptable..&taskprefix._ever_tmsis_prvdr_mdcd_enrlmt
        where ever_enrolled_provider =1 and
		      lpad(prvdr_mdcd_enrlmt_stus_cd,2,'0') in ('01', '02', '03', '04', '05', '06')

) by tmsis_passthrough;   

	
%mend;

%get_prov_ever_enrld_sql;

/**Merge claims header to Privider **/
%macro clm_blg_prov_evr_enrl_sql(ftype, n);

/**for each claim file, get the unique list of billing providers and claim date*/
 
execute(
    create or replace temporary view &taskprefix._&ftype._evr_enrl_blg_prvdr_aj as
    select distinct
           submtg_state_cd
           ,blg_prvdr_num as clm_prvdr
		   %if "&ftype."="ip" %then %do;
		   ,admsn_dt as clm_dt
		   %end;
		   %if "&ftype."="lt" or "&ftype."="ot" %then %do;
		   ,srvc_bgnng_dt as clm_dt
		   %end;
		   %if "&ftype."="rx" %then %do;
		   ,rx_fill_dt as clm_dt
		   %end;
    from &temptable..&taskprefix._base_clh_&ftype.
    where claim_cat_aj = 1 and 
	      blg_prvdr_num is not null

	) by tmsis_passthrough;


execute(
	create or replace temporary view &taskprefix._&ftype._evr_enrl_blg_prvdr as
	select 
		  a.submtg_state_cd
         ,a.clm_prvdr
		 /**at least one claim id/date that didn't link to provider file **/
		 ,max(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov
        
	from &taskprefix._&ftype._evr_enrl_blg_prvdr_aj a
	left join &taskprefix._prvdr_ever_enrld b

	on  a.submtg_state_cd = b.submtg_state_cd and
        a.clm_prvdr = b.prvdr_id and
		(((prvdr_mdcd_efctv_dt  <= clm_dt and prvdr_mdcd_efctv_dt is not null)
		  and (prvdr_mdcd_end_dt  >= clm_dt or prvdr_mdcd_end_dt  is null)))

   group by a.submtg_state_cd, a.clm_prvdr
    ) by tmsis_passthrough;
  
execute(
    create or replace temporary view &taskprefix._utl_&ftype._evr_enrl_blg_prov as
    select submtg_state_cd,
           sum(flag_not_in_prov) as all21_&n._numer,
		   count(submtg_state_cd) as all21_&n._denom, 
           round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as all21_&n.
	from &taskprefix._&ftype._evr_enrl_blg_prvdr
    group by submtg_state_cd
    ) by tmsis_passthrough;
	

%mend;

%clm_blg_prov_evr_enrl_sql(ip,1);
%clm_blg_prov_evr_enrl_sql(lt,2);
%clm_blg_prov_evr_enrl_sql(ot,3);
%clm_blg_prov_evr_enrl_sql(rx,4);

/**Merge claim lines with Provider **/
%macro clm_srvc_prov_evr_enrl_sql(ftype, n);

/**for each claim file, get the unique list of servicing providers and claim date*/ 

execute(
    create or replace temporary view &taskprefix._&ftype._evr_enrl_srvc_prvdr_aj as
    select distinct
           submtg_state_cd
		   %if "&ftype."="ip" or "&ftype."="lt" or "&ftype."="ot" %then %do;
              ,srvcng_prvdr_num as clm_prvdr
		   %end;
		   %if "&ftype."="rx" %then %do;
              ,dspnsng_pd_prvdr_num as clm_prvdr
		   %end;

		   %if "&ftype."="ip" or "&ftype."="lt" or "&ftype."="ot" %then %do;
		      ,srvc_bgnng_dt as clm_dt
		   %end;
		   %if "&ftype."="rx" %then %do;
		      ,rx_fill_dt as clm_dt
		   %end;
    from 
      %if "&ftype."="rx" %then %do;
         &temptable..&taskprefix._base_clh_&ftype.
	  %end;
	  %else %do;
         &temptable..&taskprefix._base_cll_&ftype.
	  %end;
    where claim_cat_aj = 1 and 
	       %if "&ftype."="rx" %then %do;
              dspnsng_pd_prvdr_num is not null
		   %end;
		   %if "&ftype."="ip" or "&ftype."="lt" or "&ftype."="ot" %then %do;
	          srvcng_prvdr_num is not null
		   %end;

	) by tmsis_passthrough;

execute(
	create or replace temporary view &taskprefix._&ftype._evr_enrl_srvc_prvdr as
	select 
		  a.submtg_state_cd
         ,a.clm_prvdr
		 /**at least one claim id/date that didn't link to provider file **/
		 ,max(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov
        
	from &taskprefix._&ftype._evr_enrl_srvc_prvdr_aj a
	left join &taskprefix._prvdr_ever_enrld b

	on  a.submtg_state_cd = b.submtg_state_cd and
        a.clm_prvdr = b.prvdr_id and
		(((prvdr_mdcd_efctv_dt  <= clm_dt and prvdr_mdcd_efctv_dt is not null)
		  and (prvdr_mdcd_end_dt  >= clm_dt or prvdr_mdcd_end_dt  is null)))

   group by a.submtg_state_cd, a.clm_prvdr
    ) by tmsis_passthrough;
  
execute(
    create or replace temporary view &taskprefix._utl_&ftype._evr_enrl_srvc_prov as
    select submtg_state_cd,
           sum(flag_not_in_prov) as all21_&n._numer,
		   count(submtg_state_cd) as all21_&n._denom, 
           round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as all21_&n.
	from &taskprefix._&ftype._evr_enrl_srvc_prvdr
    group by submtg_state_cd
    ) by tmsis_passthrough;

%mend;


%clm_srvc_prov_evr_enrl_sql(ip,5);
%clm_srvc_prov_evr_enrl_sql(lt,6);
%clm_srvc_prov_evr_enrl_sql(ot,7);
%clm_srvc_prov_evr_enrl_sql(rx,8);

execute(

    insert into &utl_output
    select 
    submtg_state_cd
    , 'all21_1'
    , '707'
    ,all21_1_numer
    ,all21_1_denom
    ,all21_1
    , null
    , null    
    from #temp.&taskprefix._utl_ip_evr_enrl_blg_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_2'
    , '707'
    ,all21_2_numer
    ,all21_2_denom
    ,all21_2
    , null
    , null    
    from #temp.&taskprefix._utl_lt_evr_enrl_blg_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_3'
    , '707'
    ,all21_3_numer
    ,all21_3_denom
    ,all21_3
    , null
    , null    
    from #temp.&taskprefix._utl_ot_evr_enrl_blg_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_4'
    , '707'
    ,all21_4_numer
    ,all21_4_denom
    ,all21_4
    , null
    , null    
    from #temp.&taskprefix._utl_rx_evr_enrl_blg_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_5'
    , '707'
    ,all21_5_numer
    ,all21_5_denom
    ,all21_5
    , null
    , null    
    from #temp.&taskprefix._utl_ip_evr_enrl_srvc_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output
    
    select 
    submtg_state_cd
    , 'all21_6'
    , '707'
    ,all21_6_numer
    ,all21_6_denom 
    ,all21_6
    , null
    , null    
    from #temp.&taskprefix._utl_lt_evr_enrl_srvc_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_7'
    , '707'
    ,all21_7_numer
    ,all21_7_denom 
    ,all21_7
    , null
    , null    
    from #temp.&taskprefix._utl_ot_evr_enrl_srvc_prov

    ) by tmsis_passthrough; 

   execute ( 
    insert into &utl_output

    select 
    submtg_state_cd
    , 'all21_8'
    , '707'
    ,all21_8_numer
    ,all21_8_denom
    ,all21_8
    , null
    , null    
    from #temp.&taskprefix._utl_rx_evr_enrl_srvc_prov

    ) by tmsis_passthrough;

    %insert_msr(msrid=all21_1);
    %insert_msr(msrid=all21_2);
    %insert_msr(msrid=all21_3);
    %insert_msr(msrid=all21_4);
    %insert_msr(msrid=all21_5);
    %insert_msr(msrid=all21_6);
    %insert_msr(msrid=all21_7);
    %insert_msr(msrid=all21_8);
