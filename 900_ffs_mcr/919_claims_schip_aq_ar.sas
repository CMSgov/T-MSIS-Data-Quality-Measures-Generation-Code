/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 919_claims_schip_aq_ar.sas  
 Project: MACBIS Task 2

 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.

 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.
 
******************************************************************************************/

%macro tab_clh_aq_ar(filetype,msrnum, claim_cat);

	execute(
     create or replace temporary view &taskprefix._aq_ar_mdcd_pd as
	    select
             orgnl_clm_num
            ,adjstmt_clm_num
            ,adjdctn_dt
	    	,adjstmt_ind
            ,max(case when ((mdcr_pd_amt=0 or mdcr_pd_amt is NULL) and 
                            (tot_mdcr_coinsrnc_amt=0 or tot_mdcr_coinsrnc_amt is NULL) and 
                            (tot_mdcr_ddctbl_amt=0 or tot_mdcr_ddctbl_amt is NULL)) 
                      then 1 else 0 end) as clh_numer
			
        from &temptable..&taskprefix._base_clh_&filetype.
	    where claim_cat_&claim_cat = 1
		group by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,adjstmt_ind 
	) by tmsis_passthrough;


	execute(
     insert into &wrktable..&taskprefix._clms_900b
	    select
	        %str(%')&state.%str(%') as submtg_state_cd,
			%str(%')&msrnum.%str(%') as measure_id,
			'919' as submodule,
			coalesce(numer,0) as numer,
			coalesce(denom,0) as denom, 
	    	case when coalesce(denom,0) <> 0 then numer/denom else null end as mvalue,
			null as valid_value
		from
			(select
	         count(1) as denom
			,sum(clh_numer) as numer
       	from #temp.&taskprefix._aq_ar_mdcd_pd) a
	) by tmsis_passthrough;

	 *add measure to list of measures in SAS dataset;
 	 insert into dqout.measures_900 
 	 values("&msrnum.",null);
	
%mend tab_clh_aq_ar;


%macro tab_cll_to_clh_aq_ar(claim_type,msrnum, claim_cat);

  /*step 1: Sum amounts across line level records 
   */
  execute(
    create or replace temporary view &taskprefix._cll as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
	  ,sum(case when (mdcr_pd_amt is not null) then mdcr_pd_amt else 0 end) as sum_mdcr_pd_amt      	  

    from &temptable..&taskprefix._base_cll_&claim_type.
    where claim_cat_&claim_cat. =1
	and childless_header_flag = 0
	group by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;

  /*step 2: Get amount from the header level
  */
  execute(
    create or replace temporary view &taskprefix._clh as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,max(case when (tot_mdcr_coinsrnc_amt=0 or tot_mdcr_coinsrnc_amt is NULL) and 
                     (tot_mdcr_ddctbl_amt=0 or tot_mdcr_ddctbl_amt is NULL) then 1 else 0 end) as h_cond

    from &temptable..&taskprefix._base_clh_&claim_type.
    where claim_cat_&claim_cat. =1
	group by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;
  

  /*step 3: take average at the header level among claims that meet all denominator criteria
   */

  execute(
    insert into &wrktable..&taskprefix._clms_900b
	select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&msrnum.%str(%') as measure_id, 
		'919' as submodule,
		coalesce(numer,0) as numer,
		coalesce(denom,0) as denom, 
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	select
	     count(1) as denom  
	    ,sum(case when (sum_mdcr_pd_amt=0 and h_cond=1)  then 1 else 0 end) as numer
	
          from #temp.&taskprefix._clh a left join
               #temp.&taskprefix._cll b
           on a.orgnl_clm_num  =b.orgnl_clm_num and
              a.adjstmt_clm_num=b.adjstmt_clm_num and
              a.adjdctn_dt     =b.adjdctn_dt and
              a.adjstmt_ind    =b.adjstmt_ind
        ) c
     ) by tmsis_passthrough;

	 *add measure to list of measures in SAS dataset;
 	 insert into dqout.measures_900 
 	 values("&msrnum.",null);
	
%mend tab_cll_to_clh_aq_ar;

%macro run_919_claims_schip_aq_ar;

    %tab_clh_aq_ar(ip,FFS54_1,AQ);
    %tab_clh_aq_ar(ip,MCR64_1,AR);
    %tab_clh_aq_ar(lt,FFS54_2,AQ);
    %tab_clh_aq_ar(lt,MCR64_2,AR);
    %tab_cll_to_clh_aq_ar(ot,FFS54_3,AQ);
    %tab_cll_to_clh_aq_ar(ot,MCR64_3,AR);
    %tab_cll_to_clh_aq_ar(rx,FFS54_4,AQ);
    %tab_cll_to_clh_aq_ar(rx,MCR64_4,AR);

/*extract measure from AREMAC into sas*/
create table batch_900b as
select * from connection to tmsis_passthrough
(select * from &wrktable..&taskprefix._clms_900b);

%dropwrktables(900b);

%mend run_919_claims_schip_aq_ar;


