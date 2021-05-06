
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro get_prov_id_sql;

/** get unique providers in prvdr_attr_mn or prvdr_id file */
/**Look for providers in EVER_xx provider files **/ 
 
execute(
	create or replace temporary view &taskprefix._prvdr_prep as
	select distinct 
	       submtg_state_cd, 
	       submtg_state_prvdr_id as prvdr_id
	    from &temptable..&taskprefix._ever_tmsis_prvdr_attr_mn 
        where submtg_state_prvdr_id is not null
	union
	select distinct
           submtg_state_cd,
           prvdr_id
		from &temptable..&taskprefix._ever_tmsis_prvdr_id
        where prvdr_id is not null

) by tmsis_passthrough;   


%mend;

%get_prov_id_sql;

%macro merge_clm_prov_sql(ftype, n);

/**for each claim file, get the unique list of servicing or billing providers */ 

execute(
    create or replace temporary view &taskprefix._&ftype._prvdr_w as
    select distinct
           submtg_state_cd
           ,blg_prvdr_num as clm_prvdr
    from &temptable..&taskprefix._base_clh_&ftype.
    where claim_cat_w = 1 

	union 

    select distinct
           submtg_state_cd
		   %if "&ftype."="rx" %then %do;
           ,dspnsng_pd_prvdr_num as clm_prvdr 
		   %end;
		   %else %do;
           ,srvcng_prvdr_num as clm_prvdr  
           %end; 
    from &temptable..&taskprefix._base_cll_&ftype.
    where claim_cat_w = 1 
	) by tmsis_passthrough;

execute(
	create or replace temporary view &taskprefix._&ftype._prvdr as
	select 
		  a.submtg_state_cd
         ,a.clm_prvdr
		 ,sum(case when b.prvdr_id is null then 1 else 0 end) as flag_not_in_prov
	from &taskprefix._&ftype._prvdr_w a
	left join &taskprefix._prvdr_prep b
	on  a.submtg_state_cd = b.submtg_state_cd and
        a.clm_prvdr = b.prvdr_id 
	where a.clm_prvdr is not null
    group by a.submtg_state_cd, a.clm_prvdr
    ) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_&ftype._prov as
    select submtg_state_cd,
           sum(flag_not_in_prov) as all4_&n._numer,
		   count(submtg_state_cd) as all4_&n._denom, 
           round((sum(flag_not_in_prov)/count(submtg_state_cd)),2) as all4_&n.
	from &taskprefix._&ftype._prvdr
    group by submtg_state_cd
    ) by tmsis_passthrough;

%mend;

%merge_clm_prov_sql(ip,1);
%merge_clm_prov_sql(lt,2);
%merge_clm_prov_sql(ot,3);
%merge_clm_prov_sql(rx,4);

%macro get_dups(ftype,n,n2);

/**header duplicates **/
execute(
    create or replace temporary view &taskprefix._dup_clh_recs_&ftype. as
    select  
		tmsis_run_id
        ,tmsis_rptg_prd
        ,submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,case when (count(submtg_state_cd) > 1) then 1 else 0 end as flag_dup
		     
    from &temptable..&taskprefix._dup_clh_&ftype.
	where claim_cat_w = 1 
    group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, 
             adjstmt_clm_num, adjdctn_dt, adjstmt_ind    
	) by tmsis_passthrough;


/**line duplicates */
	
execute(
    create or replace temporary view &taskprefix._dup_cll_recs_&ftype. as
    select  
		tmsis_run_id
		,tmsis_rptg_prd
        ,submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,orgnl_line_num
        ,adjstmt_line_num
		,line_adjstmt_ind
        ,case when (count(submtg_state_cd) > 1) then 1 else 0 end as flag_dup
		     
    from &temptable..&taskprefix._dup_cll_&ftype.
	/*Note - removed claim_cat_w because it just means all claims. restructuring to get that variable
	         onto this file is more effort than it is worth. No filter needed since it is all claims. */
    group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num, adjstmt_line_num, line_adjstmt_ind
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_hdr_dups_&ftype. as
    select  
		 submtg_state_cd
        ,sum(flag_dup) as all5_&n._numer
		,count(submtg_state_cd) as all5_&n._denom
	    ,round((sum(flag_dup)/count(submtg_state_cd)),3) as all5_&n.
    from &taskprefix._dup_clh_recs_&ftype.
    group by submtg_state_cd
    
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_line_dups_&ftype. as
    select  
		 submtg_state_cd
        ,sum(flag_dup) as all5_&n2._numer
		,count(submtg_state_cd) as all5_&n2._denom
	    ,round((sum(flag_dup)/count(submtg_state_cd)),3) as all5_&n2.
    from &taskprefix._dup_cll_recs_&ftype.
	/*KH Note: childless headers already excluded from dups*/
    group by submtg_state_cd
    
	) by tmsis_passthrough;

%mend;
%get_dups(ip,1,5);
%get_dups(lt,2,6);
%get_dups(ot,3,7);
%get_dups(rx,4,8);

execute(
    
    insert into &utl_output
    
    select submtg_state_cd
        , 'all4_1'
        , '704'
    ,all4_1_numer
    ,all4_1_denom 
    ,all4_1
        , null
        , null
    from #temp.&taskprefix._utl_ip_prov

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select submtg_state_cd
        , 'all4_2'
        , '704'
    ,all4_2_numer
    ,all4_2_denom 
    ,all4_2
        , null
        , null
    from #temp.&taskprefix._utl_lt_prov

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select submtg_state_cd
        , 'all4_3'
        , '704'
    ,all4_3_numer
    ,all4_3_denom 
    ,all4_3
        , null
        , null
    from #temp.&taskprefix._utl_ot_prov

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select submtg_state_cd
        , 'all4_4'
        , '704'
    ,all4_4_numer
    ,all4_4_denom 
    ,all4_4
        , null
        , null
    from #temp.&taskprefix._utl_rx_prov

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_1'
        , '704'
    ,all5_1_numer
    ,all5_1_denom
    ,all5_1
        , null
        , null
    from #temp.&taskprefix._utl_hdr_dups_ip
    
     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_5'
        , '704'
    ,all5_5_numer
    ,all5_5_denom
    ,all5_5
        , null
        , null
    from #temp.&taskprefix._utl_line_dups_ip

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select  
    submtg_state_cd
        , 'all5_2'
        , '704'
    ,all5_2_numer
    ,all5_2_denom
    ,all5_2
        , null
        , null
    from #temp.&taskprefix._utl_hdr_dups_lt

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_6'
        , '704'
    ,all5_6_numer
    ,all5_6_denom
    ,all5_6
        , null
        , null
    from #temp.&taskprefix._utl_line_dups_lt

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select  
    submtg_state_cd
        , 'all5_3'
        , '704'
    ,all5_3_numer
    ,all5_3_denom
    ,all5_3
        , null
        , null
    from #temp.&taskprefix._utl_hdr_dups_ot

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_7'
        , '704'
    ,all5_7_numer
    ,all5_7_denom
    ,all5_7
        , null
        , null
    from #temp.&taskprefix._utl_line_dups_ot

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_4'
        , '704'
    ,all5_4_numer
    ,all5_4_denom
    ,all5_4
        , null
        , null
    from #temp.&taskprefix._utl_hdr_dups_rx

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select  
    submtg_state_cd
        , 'all5_8'
        , '704'
    ,all5_8_numer
    ,all5_8_denom
    ,all5_8
        , null
        , null
    from #temp.&taskprefix._utl_line_dups_rx

    ) by tmsis_passthrough;


    %insert_msr(msrid=all4_1);
    %insert_msr(msrid=all5_1);
    %insert_msr(msrid=all5_5);
    %insert_msr(msrid=all4_2);
    %insert_msr(msrid=all5_2);
    %insert_msr(msrid=all5_6);
    %insert_msr(msrid=all4_3);
    %insert_msr(msrid=all5_3);
    %insert_msr(msrid=all5_7);
    %insert_msr(msrid=all4_4);
    %insert_msr(msrid=all5_4);
    %insert_msr(msrid=all5_8);
