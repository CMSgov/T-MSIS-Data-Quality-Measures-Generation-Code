
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/



/** 2019/01/17: new measures - 13.1 and 13.4 **/

%macro utl_link_rx_el_ab_sql;


/**read header */
	
execute(
create or replace temporary view &taskprefix._rx_hdr_clm_ab as
    select

         submtg_state_cd
	 	,msis_ident_num  /**keep msis id to link to el files*/
	    ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind 
		,rx_fill_dt
	    ,max(case when (pgm_type_cd not in ('02') or pgm_type_cd is null) then 1 else 0 end) as all13_2_numer_orig 
		
    from &temptable..&taskprefix._base_clh_rx
	where claim_cat_ab = 1
	group by submtg_state_cd, msis_ident_num, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind, rx_fill_dt
	) by tmsis_passthrough;
	
      
	/*merge to ever eligible **/
  	execute(
	create or replace temporary view &taskprefix._rx_hdr_ab_ever_elig as
	select a.*
    from &taskprefix._rx_hdr_clm_ab a 
	inner join
          (select submtg_state_cd, msis_ident_num
		   from &temptable..&taskprefix._ever_elig
		   where ever_eligible=1 
           group by submtg_state_cd, msis_ident_num) b
	on    a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num 
	) by tmsis_passthrough;

	/**merge to el determinant file and apply restrictions */
    
    execute(
	create or replace temporary view &taskprefix._rx_hdr_ab_ever_elig2 as
	select a.*
	       ,case when b.rstrctd_bnfts_cd in ('6') then 1 else 0 end as all13_2_denom_orig
    from &taskprefix._rx_hdr_ab_ever_elig a
    inner join &temptable..&taskprefix._ever_elig_dtrmnt b
        on a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num and 
		  (a.rx_fill_dt >=b.elgblty_dtrmnt_efctv_dt and a.rx_fill_dt is not null ) and 
		  (a.rx_fill_dt <=b.elgblty_dtrmnt_end_dt or b.elgblty_dtrmnt_end_dt is null)
    	) by tmsis_passthrough;

	/*now rolling upto one record per msis id*/

execute(
    create or replace temporary view &taskprefix._rx_el_lnk_ab as
    select
         submtg_state_cd
        ,msis_ident_num
		,max(all13_2_denom_orig) as all13_2_denom_1
		,max(case when all13_2_denom_orig=1 then all13_2_numer_orig else 0 end) as all13_2_numer_l
    from &taskprefix._rx_hdr_ab_ever_elig2
    group by submtg_state_cd,msis_ident_num
    ) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_rx_el_lnk_pct_ab as
    select
		 submtg_state_cd
		,all13_2_numer
		,all13_2_denom
        ,case when all13_2_denom >0 then round((all13_2_numer / all13_2_denom),2) 
              else null end as all13_2
        
    from 
		(select
		 	 submtg_state_cd
        	,sum(all13_2_numer_l) as all13_2_numer
        	,sum(all13_2_denom_1) as all13_2_denom
		from &taskprefix._rx_el_lnk_ab
    	group by submtg_state_cd) a
	) by tmsis_passthrough;	

execute(
    
    insert into &utl_output

    select
    submtg_state_cd
        , 'all13_2'
        , '705'
    ,all13_2_numer
    ,all13_2_denom
    ,all13_2       
        , null
        , null
    from #temp.&taskprefix._utl_rx_el_lnk_pct_ab
    ) by tmsis_passthrough;	

     %insert_msr(msrid=all13_2);

%mend ;



