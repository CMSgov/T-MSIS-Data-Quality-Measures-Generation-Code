/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/******************************************************************************************
 Program: 906_ffs_clms_ad.sas  
 Project: MACBIS Task 2

 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.

 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.
******************************************************************************************/

%macro tab_clm_ad(filetype,msrnum);

	execute(
     create or replace temporary view &taskprefix._ad_mdcd_pd as
	    select

	         submtg_state_cd
	     	,tmsis_rptg_prd 
            ,orgnl_clm_num
            ,adjstmt_clm_num
            ,adjdctn_dt
	    	,adjstmt_ind
			/*,tot_mdcd_pd_amt
            ,tot_mdcr_coinsrnc_amt 
            ,tot_mdcr_ddctbl_amt*/
            ,max(case when (coalesce(tot_mdcd_pd_amt,0) != coalesce(tot_mdcr_coinsrnc_amt,0) + 
                            coalesce(tot_mdcr_ddctbl_amt,0)) then 1 else 0 end) as FFS&msrnum._1_numer
			
        from &temptable..&taskprefix._base_clh_&filetype.
	    where claim_cat_AD = 1
		group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,adjstmt_ind 
	) by tmsis_passthrough;

	execute(
     insert into &wrktable..&taskprefix._clms_900b
	    select
	         %str(%')&state.%str(%') as submtg_state_cd,
			 %str(%')FFS&msrnum._1%str(%') as measure_id,
			 '906' as submodule,
    		  coalesce(numer,0) as numer,
    		  coalesce(denom,0) as denom,
	    	  case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue,
			  null as valid_value
		from
		(select
	         count(submtg_state_cd) as denom
			,sum(FFS&msrnum._1_numer) as numer
       	from #temp.&taskprefix._ad_mdcd_pd
		group by submtg_state_cd
		) a
	) by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("FFS&msrnum._1",null);

	
%mend tab_clm_ad;

%macro run_906_ffs_clms_ad;

    %tab_clm_ad(ip,43);
    %tab_clm_ad(lt,44);
    %tab_clm_ad(ot,45);
    %tab_clm_ad(rx,46);

%mend run_906_ffs_clms_ad;


