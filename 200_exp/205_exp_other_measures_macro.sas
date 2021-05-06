/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
Program: 205_exp_other_measures_macros.sas     
Project: MACBIS Task 2
urpose: Defines and calls macros for module 200 (expenditures) for all measures that arent
        defined in programs 201-204. These are the special measures that dont fit into
        a more formulaic pattern like the other measures in 201-204.
        
        Designed to be %included in module level driver         
        Uses v1.1 of specs, converted to do measures creation in AREMAC instead of SAS EBI
 
Author:             Richard Chapman
Date Created:       3/1/2017
Current Programmer:
 
Input: must be called from module level driver, which creates full set of temporary AREMAC tables
       using the standard pull code. Also requires macro variables defined from macros called in 
       module level driver.
 
Output: Each macro call creates a single measure, and extracts that measure into a SAS work dataset
        from AREMAC. These tables are named EXP##.#. Most are a single observation, except for
        frequency measures or other measures that have one observation per oberved value.
 
Modifications:
08/14/17 RSC Updates for v1.2:
             -Commented out call for EXP26.1, EXP26.2, and EXP26.3 (v1.2 called for removal)
             -updated EXP14.4, EXP25.1, EXP25.2, EXP23.1, EXP23.2, EXP11.83
******************************************************************************************/

/******************************************************************************************
  EXP 11-83  

  v1.2: updated
        -dropped mdcd_pd_amt non-missing denominator criteria 
        -changed hcpcs_srvc_cd logic to hcpcs_txnmy_cd_beginning logic (numerator)
        -added hcpcs_txnmy_cd_beginning non-missing logic to denominator 
 ******************************************************************************************/
 
%macro exp11_83;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp11_83' as measure_id
		,'205' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
       from (
	  select 
	     sum(case when %not_missing_1(hcpcs_txnmy_cd,5) and (claim_cat_a = 1)                                                         then mdcd_pd_amt else 0 end) as denom
	    ,sum(case when %not_missing_1(hcpcs_txnmy_cd,5) and (claim_cat_a = 1) and (substring(hcpcs_txnmy_cd,1,2) in ('02','04','08')) then mdcd_pd_amt else 0 end) as numer
	  from &temptable..&taskprefix._base_cll_ot
	  where childless_header_flag = 0
	) a
	
  )by tmsis_passthrough;
  

%mend exp11_83;

/******************************************************************************************
  exp 28.2
 ******************************************************************************************/
%macro exp28_2;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp28_2' as measure_id
		,'205' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
       from (
	  select 
	     sum(case when (tot_mdcd_pd_amt>0 and tot_mdcd_pd_amt <200000) and (claim_cat_g = 1)  then 1           else 0 end) as denom
	    ,sum(case when (tot_mdcd_pd_amt>0 and tot_mdcd_pd_amt <200000) and (claim_cat_g = 1)  then tot_mdcd_pd_amt else 0 end) as numer
	  from &temptable..&taskprefix._base_clh_ot
	) a
	
  )by tmsis_passthrough;

%mend exp28_2;

/******************************************************************************************
  exp 14-4

  v1.2 update: removed criteria that mcdcd_pd_amt be not null
 ******************************************************************************************/
%macro exp14_4;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp14_4' as measure_id
		,'205' as submodule
        ,null as numer
        ,null as denom
	    ,sum(case when (claim_cat_g = 1)  then mdcd_pd_amt else 0 end) as mvalue
	  from &temptable..&taskprefix._base_cll_ot	  	 
	  where childless_header_flag = 0
	
  )by tmsis_passthrough;


%mend exp14_4;
/******************************************************************************************
  exp 14-1
 ******************************************************************************************/
 
%macro exp14_1;

  execute(
    insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp14_1' as measure_id
		,'205' as submodule
        ,null as numer
        ,null as denom
	    ,sum(case when (claim_cat_g = 1) and (mdcd_pd_amt > 100000) then 1 else 0 end) as mvalue
	  from &temptable..&taskprefix._base_cll_ot
	  where childless_header_flag = 0
	
  )by tmsis_passthrough;
 

%mend exp14_1;
/******************************************************************************************
  exp 25-1

  v1.2: removed criteria that mcdcd_pd_amt be non-null
 ******************************************************************************************/
 
%macro exp25_1;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp25_1' as measure_id
		,'205' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer else NULL end as mvalue
    from (
	  select
	     sum(case when (claim_cat_k = 1) and (line_adjstmt_ind in ('1','4')) then 1                else 0 end) as denom
	    ,sum(case when (claim_cat_k = 1) and (line_adjstmt_ind in ('1','4')) then abs(mdcd_pd_amt) else 0 end) as numer
	  from &temptable..&taskprefix._base_cll_ot
	  where childless_header_flag = 0
	) a
	 
  )by tmsis_passthrough;

%mend exp25_1;
/******************************************************************************************
  exp 25-2

  v1.2: removed criteria that mdcd_pd_amt be not null
 ******************************************************************************************/
 
%macro exp25_2;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp25_2' as measure_id
		,'205' as submodule
        ,null as numer
        ,null as denom
	    ,sum(case when (claim_cat_k = 1) then mdcd_pd_amt else 0 end) as mvalue
	  from &temptable..&taskprefix._base_cll_ot
	  where childless_header_flag = 0

  )by tmsis_passthrough;

%mend exp25_2;
/******************************************************************************************
  exp 23-1

  v1.2: updated to remove criteria that mdcd_pd_amt be non-null
 ******************************************************************************************/
 
%macro exp23_1;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp23_1' as measure_id
		,'205' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
         from (
		 select
	     sum(case when (claim_cat_e = 1) and (line_adjstmt_ind in ('1','4')) then 1                else 0 end) as denom
	    ,sum(case when (claim_cat_e = 1) and (line_adjstmt_ind in ('1','4')) then abs(mdcd_pd_amt) else 0 end) as numer
	  from &temptable..&taskprefix._base_cll_ot
	  where childless_header_flag = 0
	) a
	
  )by tmsis_passthrough;


%mend exp23_1;
/******************************************************************************************
  exp 23-2

  v1.2: updated to remove criteria that mdcd_pd_amt be non-null
 ******************************************************************************************/
 
%macro exp23_2;

  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp23_2' as measure_id
		,'205' as submodule
        ,null as numer
        ,null as denom
       ,sum(case when (claim_cat_e = 1) then mdcd_pd_amt else 0 end) as mvalue
	  from &temptable..&taskprefix._base_cll_ot	
	  where childless_header_flag = 0
  )by tmsis_passthrough;

%mend exp23_2;

/******************************************************************************************
  exp 12-1
 ******************************************************************************************/
 
%macro exp12_1;

/*6/27/17 rsc
  changed measure to return 0 when there are no claim_cat_b lines, rather than missing,
  to match sas v1.1 results*/
  
execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,'exp12_1' as measure_id
		,'205' as submodule
        ,null as numer
        ,null as denom
	    ,sum(case when mdcd_pd_amt>100000 and claim_cat_b = 1 then 1 else 0 end) as mvalue
	  from &temptable..&taskprefix._base_cll_ot	
	  where childless_header_flag = 0 
  ) by tmsis_passthrough;
  
%mend exp12_1;

%macro claims_with_time_span(measure_id=, claim_cat=, claim_type=);
  * based off of FFS any_span macro                   ;
  *   measure_id = measure name, i.e. EXP1.1          ;
  *   claim_cat  = claim category, defined in specs   ;
  *   claim_type = ip, lt, rx, ot                     ;

    execute (
      	insert into &wrktable..&taskprefix._exp_200
	    select 
			 %str(%')&state.%str(%') as submtg_state_cd
	    	,%str(%')&measure_id.%str(%') as measure_id
			,'205' as submodule
	        ,coalesce(numer,0) as numer
	        ,coalesce(denom,0) as denom
		    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
	    from (
	        select 
		         sum(denom_inner) as denom
		        ,sum(case when numer_inner=1 and denom_inner=1 then 1 else 0 end) as numer
	        from
	        (
				select 
					 msis_ident_num
					,submtg_state_cd
					,max(claim_cat_&claim_cat.) as denom_inner
	        	from &temptable..&taskprefix._base_clh_&claim_type.
	        	group by msis_ident_num, submtg_state_cd
	        ) d
			left join
        	(
				select 
					 msis_ident_num
					,submtg_state_cd
					,max(ever_eligible) as numer_inner 
				from &temptable..&taskprefix._ever_elig
        		group by msis_ident_num, submtg_state_cd
			) n
	        on d.msis_ident_num=n.msis_ident_num and d.submtg_state_cd=n.submtg_state_cd
        ) m
    ) by tmsis_passthrough;

    %mend;

/******************************************************************************************
  macro to run all above macros (will be called from module driver)
 ******************************************************************************************/

%macro run_205_all_other_exp;
  %exp11_83;
  %exp28_2;
  %exp14_4;
  %exp14_1;
  %exp25_1;
  %exp25_2;
  %exp23_1;
  %exp23_2;
  %exp12_1;
  %claims_with_time_span(measure_id=exp45_4, claim_cat=at, claim_type=ip);
  %claims_with_time_span(measure_id=exp45_5, claim_cat=at, claim_type=lt);
  %claims_with_time_span(measure_id=exp45_6, claim_cat=at, claim_type=ot);

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values('exp11_83')
  values('exp28_2')
  values('exp14_4')
  values('exp14_1')
  values('exp25_1')
  values('exp25_2')
  values('exp23_1')
  values('exp23_2')
  values('exp12_1')
  values('exp45_4')
  values('exp45_5')
  values('exp45_6');


%mend run_205_all_other_exp;
