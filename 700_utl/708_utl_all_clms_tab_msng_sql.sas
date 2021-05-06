/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_msng(ftype,n,n2);

/**header duplicates **/

execute(
    create or replace temporary view &taskprefix._msng_clh_recs_&ftype. as
    select  
		tmsis_run_id
        ,tmsis_rptg_prd
        ,submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
		,count_claims_key
        ,case when adjdctn_dt_orig is null then 1 else 0 end as msng_hdr_adjdctn_dt
	
      	     
    from &temptable..&taskprefix._dup_clh_&ftype.
	   
	) by tmsis_passthrough;


execute(
    insert into &utl_output
    select  
    submtg_state_cd
    , %tslit(all16_&n)
    , '708'
    ,sum(msng_hdr_adjdctn_dt) as ALL16_&n._numer
    ,count(submtg_state_cd) as ALL16_&n._denom
    ,round((sum( msng_hdr_adjdctn_dt)/count(submtg_state_cd)),3) as ALL16_&n.
    , null
    , null        
    from #temp.&taskprefix._msng_clh_recs_&ftype.
    group by submtg_state_cd    
	) by tmsis_passthrough;

%insert_msr(msrid=all16_&n);

/**line duplicates */
	
execute(
    create or replace temporary view &taskprefix._msng_cll_recs_&ftype. as
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
		,count_cll_key
        ,case when line_adjdctn_dt_orig is null then 1 else 0 end as msng_lne_adjdctn_dt
		     
    from &temptable..&taskprefix._dup_cll_&ftype.
	/*KH note: childless headers already excluded from dup_cll file*/
	
	) by tmsis_passthrough;


execute(
    insert into &utl_output
    select  
    submtg_state_cd
    , %tslit(all16_&n2)
    , '708'
    ,sum(msng_lne_adjdctn_dt) 
    ,count(submtg_state_cd) 
    ,round((sum(msng_lne_adjdctn_dt)/count(submtg_state_cd)),3)
    , null
    , null    
    from #temp.&taskprefix._msng_cll_recs_&ftype.
    group by submtg_state_cd
    
	) by tmsis_passthrough;
%insert_msr(msrid=all16_&n2);

%mend;
%utl_msng(ip,1,2);
%utl_msng(lt,3,4);
%utl_msng(ot,5,6);
%utl_msng(rx,7,8);


%macro utl_orphn_lnes_chls_hdr(ftype,n,n2);

execute (
    create or replace temporary view &taskprefix._msng_h_recs_&ftype. as
	select 
	a.submtg_state_cd
	,sum(case when b.cll_flag is null then 1 else 0 end) as all_numer
    ,count(a.submtg_state_cd) as all_denom

	from 
    (select distinct
                 submtg_state_cd
                 ,orgnl_clm_num 
                 ,adjstmt_clm_num
                 ,adjdctn_dt
                 ,adjstmt_ind
          from &taskprefix._msng_clh_recs_&ftype. 
             /*where count_claims_key = 1*/) a
	left join 
     	 (select distinct 
		         submtg_state_cd
                 ,orgnl_clm_num
				 ,adjstmt_clm_num
				 ,adjdctn_dt
				 ,line_adjstmt_ind
			     ,1 as cll_flag
         from &taskprefix._msng_cll_recs_&ftype.
          /*where count_cll_key = 1*/) b
	 on 	    a.submtg_state_cd=b.submtg_state_cd and
			    a.orgnl_clm_num = b.orgnl_clm_num and
    			a.adjstmt_clm_num = b.adjstmt_clm_num and
    			a.adjdctn_dt = b.adjdctn_dt and
    			a.adjstmt_ind = b.line_adjstmt_ind
    group by a.submtg_state_cd
    
	) by tmsis_passthrough;


/**17.1-17.8**/
/****Childless Claim Headers **/
execute(
    insert into &utl_output
    select  
    submtg_state_cd
    , %tslit(all17_&n)
    , '708'
    ,all_numer
    ,all_denom
    ,round((all_numer/all_denom),3)
    , null
    , null    
	   
    from #temp.&taskprefix._msng_h_recs_&ftype.

	) by tmsis_passthrough;

%insert_msr(msrid=all17_&n);

/****Orphan Claim Lines **/

execute (
    create or replace temporary view &taskprefix._msng_l_recs_&ftype. as
	select 
	a.submtg_state_cd
	,sum(case when b.clh_flag is null then 1 else 0 end) as all_numer
    ,count(a.submtg_state_cd) as all_denom

	from (select distinct 
		         submtg_state_cd
        		,orgnl_clm_num
       			,adjstmt_clm_num
        		,adjdctn_dt
				,orgnl_line_num
        		,adjstmt_line_num
				,line_adjstmt_ind
          from &taskprefix._msng_cll_recs_&ftype. 
          /*where count_cll_key = 1*/) a
	left join 
     	 (select distinct 
		         submtg_state_cd
        		 ,orgnl_clm_num
				 ,adjstmt_clm_num
				 ,adjdctn_dt
				 ,adjstmt_ind
			     ,1 as clh_flag
         from &taskprefix._msng_clh_recs_&ftype.
         /*where count_claims_key = 1*/) b

	on 	                a.submtg_state_cd=b.submtg_state_cd 
			        and a.orgnl_clm_num = b.orgnl_clm_num
    				and a.adjstmt_clm_num = b.adjstmt_clm_num
    				and a.adjdctn_dt = b.adjdctn_dt
    				and a.line_adjstmt_ind = b.adjstmt_ind
    group by a.submtg_state_cd
    
	) by tmsis_passthrough;



execute(
    insert into &utl_output
    select  
    submtg_state_cd
    , %tslit(all17_&n2)
    , '708'
    ,all_numer
    ,all_denom
    ,round((all_numer/all_denom),3)
    , null
    , null    
	   
    from #temp.&taskprefix._msng_l_recs_&ftype.
	) by tmsis_passthrough;

%insert_msr(msrid=all17_&n2);

%mend;
%utl_orphn_lnes_chls_hdr(ip,1,5);
%utl_orphn_lnes_chls_hdr(lt,2,6);
%utl_orphn_lnes_chls_hdr(ot,3,7);
%utl_orphn_lnes_chls_hdr(rx,4,8);



%macro utl_mfp(ftype,n);


/**header file **/

execute(
    insert into &utl_output
    select  
    submtg_state_cd
    , %tslit(all18_&n)
    , '708'
    ,count(submtg_state_cd) as ALL18_&n.
    , null
    , null    
    , null
    , null    		     
    from &temptable..&taskprefix._base_clh_&ftype.
	where clm_type_cd in ('U','V','W','X','Y') and 
          (pgm_type_cd != '08' or pgm_type_cd  is null)
	   
	group by submtg_state_cd
	) by tmsis_passthrough;
%insert_msr(msrid=all18_&n);

%mend;
%utl_mfp(ip,1);
%utl_mfp(lt,2);
%utl_mfp(ot,3);
%utl_mfp(rx,4);
