/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 915_mcr_provider_taxonomy.sas  
 Project: MACBIS Task 12 v1.7
 Purpose: %of records that have an invalid BILLING PROV TAXONOMY
          for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          Uses v1.7 of specs, converted to do measures creation in AREMAC instead of SAS EBI
 
 Author:  Jacqueline Agufa
 Date Created: 5/2/2019
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.
 

 Modifications:
 5/2/19  
 ******************************************************************************************/
 
 /******************************************************************************************
 
  ******************************************************************************************/
  
%macro run_taxonomy(
  measure_id=,
  claim_cat=,
  claim_type=
  );
  
  /*step 1: */
  
  execute(
    create or replace temporary view &taskprefix._denom as

	select orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
      ,blg_prvdr_txnmy_cd
      ,1 as has_denom      	  

    from &temptable..&taskprefix._base_clh_&claim_type.
	where %not_missing_1(blg_prvdr_txnmy_cd,12) and claim_cat_&claim_cat. =1

  )by tmsis_passthrough;

  /*step 2: */


  execute(
    insert into &wrktable..&taskprefix._clms_900b
	  select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id, 
		'915' as submodule,
		coalesce(numer,0) as numer,
		coalesce(denom,0) as denom,
	    case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
		null as valid_value
    from (
	    select 
	            sum(case when has_denom=1                 then 1 else 0 end) as denom
	           ,sum(case when has_denom=1 and has_numer=1 then 1 else 0 end) as numer
	    from 
          (     	  
        	select *
            ,case when blg_prvdr_txnmy_cd not in ( &Taxonomy_List ) then 1 else 0 end as has_numer      	  
            from 
            #temp.&taskprefix._denom 
          ) a            
	  ) b	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend run_taxonomy;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/
 

%macro run_915_claims_provider_taxonomy;

   select quote(Taxonomy, "'")  into :Taxonomy_List 
       separated by ", "
   from prvtxnmy.Sheet1
     ;

  *ffs50.1, ffs51.1, mcr60.1, mcr61.1;
  %run_taxonomy(measure_id=ffs50_1,  claim_cat=c, claim_type=ip);
  %run_taxonomy(measure_id=ffs51_1,  claim_cat=i, claim_type=ip);
  %run_taxonomy(measure_id=mcr60_1,  claim_cat=o, claim_type=ip);
  %run_taxonomy(measure_id=mcr61_1,  claim_cat=u, claim_type=ip);
  
  *ffs50.2, ffs51.2, mcr60.2, mcr61.2;
  %run_taxonomy(measure_id=ffs50_2,  claim_cat=c, claim_type=lt);
  %run_taxonomy(measure_id=ffs51_2,  claim_cat=i, claim_type=lt);
  %run_taxonomy(measure_id=mcr60_2,  claim_cat=o, claim_type=lt);
  %run_taxonomy(measure_id=mcr61_2,  claim_cat=u, claim_type=lt);
  
  *ffs50.3, ffs51.3, mcr60.3, mcr61.3;
  %run_taxonomy(measure_id=ffs50_3,  claim_cat=c, claim_type=ot);
  %run_taxonomy(measure_id=ffs51_3,  claim_cat=i, claim_type=ot);
  %run_taxonomy(measure_id=mcr60_3,  claim_cat=o, claim_type=ot);
  %run_taxonomy(measure_id=mcr61_3,  claim_cat=u, claim_type=ot);
  
  *ffs50.4, ffs51.4, mcr60.4, mcr61.4;
  %run_taxonomy(measure_id=ffs50_4,  claim_cat=c, claim_type=rx);  
  %run_taxonomy(measure_id=ffs51_4,  claim_cat=i, claim_type=rx);
  %run_taxonomy(measure_id=mcr60_4,  claim_cat=o, claim_type=rx);  
  %run_taxonomy(measure_id=mcr61_4,  claim_cat=u, claim_type=rx);


%mend run_915_claims_provider_taxonomy;

