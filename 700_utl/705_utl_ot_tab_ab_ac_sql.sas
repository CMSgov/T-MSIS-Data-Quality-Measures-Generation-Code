
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/


%macro utl_ot_ab_ac_sql();

%macro utl_ot_ab_ac_clm(clmcat, tblnum);

execute(
create or replace temporary view &taskprefix._ot_prep_clm_&clmcat. as
   select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,tmsis_rptg_prd
		,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
	    ,orgnl_line_num
        ,adjstmt_line_num
		,line_adjstmt_ind
		,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm

    from &temptable..&taskprefix._base_cll_ot
	where claim_cat_&clmcat. = 1
	and childless_header_flag = 0
	group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind    
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ot_clm_&clmcat. as
    select
         submtg_state_cd
        ,sum(xover_clm) as all&tblnum._1_numer
        ,count(submtg_state_cd) as all&tblnum._1_denom
        ,round((sum(xover_clm) / count(submtg_state_cd)),2) as all&tblnum._1

    from &taskprefix._ot_prep_clm_&clmcat.
    group by submtg_state_cd
	) by tmsis_passthrough;

%mend utl_ot_ab_ac_clm;

%utl_ot_ab_ac_clm(ab, 8);
%utl_ot_ab_ac_clm(ac, 11);

%mend utl_ot_ab_ac_sql;


/** 2019/01/17: new measures - 13.1 and 13.4 **/

%macro utl_link_ot_el_ab_sql;

/**read header */
execute(
create or replace temporary view &taskprefix._ot_hdr_clm_ab as
    select
    /*+ BROADCAST(&temptable..&taskprefix._prgncy_codes) */

        /*unique keys and other identifiers*/
         a.submtg_state_cd
	 	,msis_ident_num  /**keep msis id to link to el files*/
		,tmsis_rptg_prd 
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
		,srvc_bgnng_dt
		,pgm_type_cd
		,wvr_id
		,case when (pgm_type_cd not in ('02') or pgm_type_cd is null) then 1 else 0 end as all13_1_orig 
		,case when array_contains(code_cm,dgns_1_cd)
				or array_contains(code_cm,dgns_2_cd)
             then 1 else 0 end as prgncy_dx

    from &temptable..&taskprefix._base_clh_ot a
	left join &temptable..&taskprefix._prgncy_codes b
	on a.submtg_state_cd = b.submtg_state_cd
	where claim_cat_ab = 1
	) by tmsis_passthrough;
	
/**************************************************/

execute(
create or replace temporary view &taskprefix._ot_line_to_hdr_rollup_ab as
    select
    /*+ BROADCAST(&temptable..&taskprefix._prgncy_codes) */

        /*unique keys and other identifiers*/
         a.submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind 
		,max(case when rev_cd in ('450', '451', '452', '453', '454', '455', '456', '457', '458', '459',
                                  '0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459',
                                  '981','720','721','722','723','724','729',
								  '0981','0720','0721','0722','0723','0724','0729' )  or  
                        srvc_plc_cd in ('23')  then 1 else 0 end) as rev_cd_excl/**ER claims - NOT be included in Numerator */
		,max(case when array_contains(code_prc,prcdr_cd) or array_contains(code_prc,prcdr_cd) then 1 else 0 end) as prgncy_pcs
    from &temptable..&taskprefix._base_cll_ot a
	left join &temptable..&taskprefix._prgncy_codes b
	on a.submtg_state_cd = b.submtg_state_cd
	where claim_cat_ab = 1
	and childless_header_flag = 0
	group by a.submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind 
	) by tmsis_passthrough;



/** merge header and line */

/** we are merging header and line instead of rolling up the line because we want to count the msis id in header.
	if there is any msis id in line file not in header, we don't want to include it. 
	there can be a msis id in line file not in header because the msis id is not a linking variable between header and line files.

**/

/*
execute(
	    create or replace temporary view &taskprefix._ot_hdr_clm_bene_ab as
	    select
	         a.submtg_state_cd
		    ,a.msis_ident_num
	        ,a.tmsis_rptg_prd
	        ,a.orgnl_clm_num
	        ,a.adjstmt_clm_num
	        ,a.adjdctn_dt
			,a.adjstmt_ind
	        ,a.srvc_bgnng_dt
		    ,max(a.all13_1_orig) as all13_1_numer_orig
			,max(case when b.rev_cd_excl=1 or a.prgncy_dx=1 or b.prgncy_pcs=1 then 1 else 0 end ) as all13_6_numer_excl	    
	    from &taskprefix._ot_hdr_clm_ab a 
        left join &taskprefix._ot_line_to_hdr_rollup_ab b
			 on a.submtg_state_cd=b.submtg_state_cd and
			    a.tmsis_rptg_prd=b.tmsis_rptg_prd and
				a.orgnl_clm_num=b.orgnl_clm_num and
				a.adjstmt_clm_num=b.adjstmt_clm_num and
				a.adjdctn_dt=b.adjdctn_dt and
				a.adjstmt_ind=b.adjstmt_ind
	    group by a.submtg_state_cd, a.msis_ident_num, a.tmsis_rptg_prd, a.orgnl_clm_num, a.adjstmt_clm_num, a.adjdctn_dt, a.adjstmt_ind, a.srvc_bgnng_dt
	) by tmsis_passthrough;*/
	
 
 	

	/*merge to ever eligible **/
	/*
  	execute(
	create or replace temporary view &taskprefix._ot_hdr_ab_ever_elig as
	select a.*
	      ,case when all13_6_numer_excl =1 then 0 else 1 end as all13_6_numer_orig 
    from &taskprefix._ot_hdr_clm_bene_ab a
	inner join
          (select submtg_state_cd, msis_ident_num
		   from &temptable..&taskprefix._ever_elig
		   where ever_eligible=1 
           group by submtg_state_cd, msis_ident_num) b
	on    a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num 
	) by tmsis_passthrough;*/



	/**merge to el determinant file and apply restrictions */
    /*
    execute(
	create or replace temporary view &taskprefix._ot_hdr_ab_ever_elig2 as
	select a.*
	       ,case when b.rstrctd_bnfts_cd in ('6') then 1 else 0 end as all13_1_denom_orig
		   ,case when b.rstrctd_bnfts_cd in ('2') then 1 else 0 end as all13_6_denom_orig
    from &taskprefix._ot_hdr_ab_ever_elig a
    inner join &temptable..&taskprefix._ever_elig_dtrmnt b
	   on a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num and 
		  (a.srvc_bgnng_dt>=b.elgblty_dtrmnt_efctv_dt and a.srvc_bgnng_dt is not null ) and 
		  (a.srvc_bgnng_dt<=b.elgblty_dtrmnt_end_dt or b.elgblty_dtrmnt_end_dt is null)
    	) by tmsis_passthrough;*/




	/*now rolling upto one record per msis id*/
/*
execute(
    create or replace temporary view &taskprefix._ot_el_lnk_ab as
    select
         submtg_state_cd
        ,msis_ident_num
		,max(all13_1_denom_orig) as all13_1_denom_1
		,max(all13_6_denom_orig) as all13_6_denom_1
		,max(case when all13_1_denom_orig=1 then all13_1_numer_orig else 0 end) as all13_1_numer_1
        ,max(case when all13_6_denom_orig=1 then all13_6_numer_orig else 0 end) as all13_6_numer_1
	
    from &taskprefix._ot_hdr_ab_ever_elig2
    group by submtg_state_cd,msis_ident_num
    ) by tmsis_passthrough;*/

/*
execute(
    create or replace temporary view &taskprefix._utl_ot_el_lnk_pct_ab as
    select
		submtg_state_cd
		,all13_1_numer
		,all13_1_denom
		,all13_6_numer
		,all13_6_denom
        ,case when all13_1_denom >0 then round((all13_1_numer / all13_1_denom),2) 
              else null end as all13_1
        ,case when all13_6_denom >0 then round((all13_6_numer /all13_6_denom),2)
              else null end as all13_6
    from 
		(select
			 submtg_state_cd
        	,sum(all13_1_numer_1) as all13_1_numer
        	,sum(all13_1_denom_1) as all13_1_denom
        	,sum(all13_6_numer_1) as all13_6_numer
        	,sum(all13_6_denom_1) as all13_6_denom
		from &taskprefix._ot_el_lnk_ab
	    group by submtg_state_cd) a
	) by tmsis_passthrough;*/

EXECUTE (
  CREATE OR replace temporary VIEW &taskprefix._all_13_1 AS
  SELECT /* agg to st */
  submtg_state_cd
  ,sum(all13_1_numer_1) as all13_1_numer
  ,sum(all13_1_denom_1) as all13_1_denom
  ,sum(all13_6_numer_1) as all13_6_numer
  ,sum(all13_6_denom_1) as all13_6_denom
  FROM ( /* agg to st, mid */
    SELECT  
      submtg_state_cd
      ,msis_ident_num
      ,max(all13_1_denom_orig) as all13_1_denom_1
      ,max(all13_6_denom_orig) as all13_6_denom_1
      ,max(case when all13_1_denom_orig=1 then all13_1_numer_orig else 0 end) as all13_1_numer_1
      ,max(case when all13_6_denom_orig=1 then all13_6_numer_orig else 0 end) as all13_6_numer_1
      FROM ( /* select from q1, q2, q3 */
        SELECT
          q1.submtg_state_cd
          , q1.msis_ident_num
	  ,case when q3.rstrctd_bnfts_cd in ('6') then 1 else 0 end as all13_1_denom_orig
	  ,case when q3.rstrctd_bnfts_cd in ('2') then 1 else 0 end as all13_6_denom_orig
	  ,case when all13_6_numer_excl =1 then 0 else 1 end as all13_6_numer_orig
	  , all13_1_numer_orig
          FROM ( /* ot_hdr_clm_bene_ab */
            
            SELECT
	      a.submtg_state_cd
	      ,a.msis_ident_num
	      ,a.tmsis_rptg_prd
	      ,a.orgnl_clm_num
	      ,a.adjstmt_clm_num
	      ,a.adjdctn_dt
	      ,a.adjstmt_ind
	      ,a.srvc_bgnng_dt
	      ,max(a.all13_1_orig) as all13_1_numer_orig
	      ,max(case when b.rev_cd_excl=1 or a.prgncy_dx=1 or b.prgncy_pcs=1 then 1 else 0 end ) as all13_6_numer_excl
              FROM
                  &taskprefix._ot_hdr_clm_ab a 
                  LEFT JOIN
                  &taskprefix._ot_line_to_hdr_rollup_ab b
		      ON
                  a.submtg_state_cd=b.submtg_state_cd AND 
		  a.tmsis_rptg_prd=b.tmsis_rptg_prd AND 
		  a.orgnl_clm_num=b.orgnl_clm_num AND 
		  a.adjstmt_clm_num=b.adjstmt_clm_num AND 
		  a.adjdctn_dt=b.adjdctn_dt AND 
		  a.adjstmt_ind=b.adjstmt_ind
                  
	     GROUP BY a.submtg_state_cd, a.msis_ident_num, a.tmsis_rptg_prd, a.orgnl_clm_num, a.adjstmt_clm_num,
                      a.adjdctn_dt, a.adjstmt_ind, a.srvc_bgnng_dt ) q1
                 
                 INNER JOIN (
                   /* q1 ij q2 = ot_hdr_ab_ever_elig */
                   SELECT
                     submtg_state_cd
                     , msis_ident_num
                     FROM &temptable..&taskprefix._ever_elig
                    WHERE ever_eligible = 1
                    GROUP BY submtg_state_cd, msis_ident_num) q2 /* LJ + IJ = ot_hdr_ab_ever_elig */
                     ON
                 q1.submtg_state_cd = q2.submtg_state_cd AND
                 q1.msis_ident_num = q2.msis_ident_num

                 INNER JOIN
                 /* q2 ij q3 = ot_hdr_ab_ever_elig2 */
                 
                 &temptable..&taskprefix._ever_elig_dtrmnt q3 /* lj, ij, ij = ot_hdr_ab_ever_elig2 */
                     ON
                 q1.submtg_state_cd = q3.submtg_state_cd AND 
                 q1.msis_ident_num = q3.msis_ident_num AND 
                 (q1.srvc_bgnng_dt>=q3.elgblty_dtrmnt_efctv_dt and q1.srvc_bgnng_dt is not null ) AND 
                 (q1.srvc_bgnng_dt<=q3.elgblty_dtrmnt_end_dt or q3.elgblty_dtrmnt_end_dt is NULL)
                                                                          
      ) q4

GROUP BY q4.submtg_state_cd, q4.msis_ident_num) q5
GROUP BY submtg_state_cd
) BY tmsis_passthrough;

/**New Measures 2.9, 2.10 */

execute(
create or replace temporary view &taskprefix._ot_prep_clm2_ab as
   select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,tmsis_rptg_prd
		,msis_ident_num
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
	    ,orgnl_line_num
        ,adjstmt_line_num
		,line_adjstmt_ind
		,max(case when hcpcs_srvc_cd = '4' then 1 else 0 end) as hcpcs_srvc_cd_eq_4 
	
    from &temptable..&taskprefix._base_cll_ot
	where claim_cat_ab = 1
	and childless_header_flag = 0
	group by submtg_state_cd, tmsis_rptg_prd, msis_ident_num, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, 
             orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind    
	) by tmsis_passthrough;

	execute(
	create or replace temporary view &taskprefix._utl_1915_wvr as 
    select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,msis_ident_num
        ,wvr_id
    from &temptable..&taskprefix._tmsis_wvr_prtcptn_data  
	where wvr_type_cd in ('06','07','08','09','10','11','12','13',
						  '14','15','16','17','18','19','20','33')
	
	) by tmsis_passthrough;


	
	/**merge wvr to OT hdr */
    
    execute(
	create or replace temporary view &taskprefix._wvr_1915_ot_hdr_ab as
	select  a.submtg_state_cd
           ,a.msis_ident_num
		   ,case when b.msis_ident_num is null then 1 else 0 end as no_clm_rec
	       ,case when a.wvr_id=b.wvr_id then 1 else 0 end as  ALL2_9_same_wvr_id
           ,case when b.pgm_type_cd in ('07') then 1 else 0 end as  ALL2_10_pgmtyp_07

	from &taskprefix._utl_1915_wvr a
        left join &taskprefix._ot_hdr_clm_ab  b
    
	   on a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num 
    	) by tmsis_passthrough;


	
	/*now rolling upto one record per msis id*/

execute(
    create or replace temporary view &taskprefix._wvr_1915_ot_hdr_ab2 as
    select  submtg_state_cd
       		 ,msis_ident_num
			 ,case when evr_no_clm_rec =1 or  ALL2_9_evr_same_wvr_id=0 then 1 else 0 end as all2_9_numer_1
			 ,case when evr_no_clm_rec =1 or  ALL2_10_evr_pgmtyp_07=0 then 1 else 0 end as all2_10_numer_1
	from 
    	(select
        	 submtg_state_cd
       		 ,msis_ident_num
			 ,max(no_clm_rec) as  evr_no_clm_rec
			 ,max(ALL2_9_same_wvr_id) as  ALL2_9_evr_same_wvr_id
			 ,max( ALL2_10_pgmtyp_07) as  ALL2_10_evr_pgmtyp_07

			
    	from &taskprefix._wvr_1915_ot_hdr_ab
    	group by submtg_state_cd,msis_ident_num) a

    ) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_ot_hdr_wvr_lnk_pct_ab as
    select
		submtg_state_cd
		,all2_9_numer
		,all2_9_denom
		,all2_10_numer
		,all2_10_denom
        ,case when all2_9_denom >0 then round((all2_9_numer / all2_9_denom),2) 
              else null end as all2_9
        ,case when all2_10_denom >0 then round((all2_10_numer /all2_10_denom),2)
              else null end as all2_10
    from 
		(select
			 submtg_state_cd
        	,sum(all2_9_numer_1)  as all2_9_numer
        	,sum(all2_10_numer_1) as all2_10_numer
        	,sum(1)               as all2_9_denom
			,sum(1)               as all2_10_denom
		from &taskprefix._wvr_1915_ot_hdr_ab2
	    group by submtg_state_cd) a
	) by tmsis_passthrough;

/***2.11**/

	execute(
	create or replace temporary view &taskprefix._wvr_1915_ot_line_ab as
	select  a.submtg_state_cd
           ,a.msis_ident_num
		   ,case when b.msis_ident_num is null then 1 else 0 end as no_clm_rec
	       ,case when b.hcpcs_srvc_cd_eq_4=1   then 1 else 0 end as  ALL2_11_hcpcs_srvc_cd_eq_4

	from &taskprefix._utl_1915_wvr a
        left join &taskprefix._ot_prep_clm2_ab b
    
	   on a.submtg_state_cd=b.submtg_state_cd and
	      a.msis_ident_num=b.msis_ident_num 
    	) by tmsis_passthrough;


	
	/*now rolling upto one record per msis id*/

execute(
    create or replace temporary view &taskprefix._wvr_1915_ot_line_ab2 as
	select 	submtg_state_cd
        	,msis_ident_num
			,case when evr_no_clm_rec=1 or ALL2_11_ever_hcpcs_srvccd4=0 then 1 else 0 end as all2_11_numer_1
	from
    	(select
         	submtg_state_cd
        	,msis_ident_num
			,max(no_clm_rec) as  evr_no_clm_rec
			,max(ALL2_11_hcpcs_srvc_cd_eq_4) as  ALL2_11_ever_hcpcs_srvccd4			
    	from &taskprefix._wvr_1915_ot_line_ab
    	group by submtg_state_cd,msis_ident_num) a
    ) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._utl_ot_line_wvr_lnk_pct_ab as
    select
		submtg_state_cd
		,all2_11_numer
		,all2_11_denom
        ,case when all2_11_denom >0 then round((all2_11_numer /all2_11_denom),2)
              else null end as all2_11
    from 
		(select
			 submtg_state_cd
        	,sum(all2_11_numer_1) as all2_11_numer
        	,sum(1)               as all2_11_denom
		from &taskprefix._wvr_1915_ot_line_ab2
	    group by submtg_state_cd) a
	) by tmsis_passthrough;

execute(
    insert into &utl_output
    
    select
         submtg_state_cd
        , 'all8_1'
        , '705'
        ,all8_1_numer
        ,all8_1_denom
        ,all8_1
        , null
        , null
        from #temp.&taskprefix._ot_clm_ab

         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all11_1'
        , '705'
        ,all11_1_numer
        ,all11_1_denom
        ,all11_1
        , null
        , null
        from #temp.&taskprefix._ot_clm_ac

         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all13_1'
        , '705'
        ,all13_1_numer
        ,all13_1_denom
        ,case when all13_1_denom > 0 then round((all13_1_numer / all13_1_denom),2) 
              else null end
        , null
        , null
        from #temp.&taskprefix._all_13_1

         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all13_6'
        , '705'
        ,all13_6_numer
        ,all13_6_denom
        ,case when all13_6_denom > 0 then round((all13_6_numer / all13_6_denom),2) 
              else null end
        , null
        , null
        from #temp.&taskprefix._all_13_1

         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all2_9'
        , '705'
        ,all2_9_numer
        ,all2_9_denom
        ,all2_9
        , null
        , null
        from #temp.&taskprefix._utl_ot_hdr_wvr_lnk_pct_ab

         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all2_10'
        , '705'
        ,all2_10_numer
        ,all2_10_denom
        ,all2_10
        , null
        , null
        from #temp.&taskprefix._utl_ot_hdr_wvr_lnk_pct_ab
        
         ) by tmsis_passthrough; 

execute ( 
        insert into &utl_output 

        select
        submtg_state_cd
        , 'all2_11'
        , '705'        
        ,all2_11_numer
        ,all2_11_denom
        ,all2_11
        , null
        , null
        from #temp.&taskprefix._utl_ot_line_wvr_lnk_pct_ab

        ) by tmsis_passthrough;

    %insert_msr(msrid=all8_1);
    %insert_msr(msrid=all11_1);
    %insert_msr(msrid=all13_1);
    %insert_msr(msrid=all13_6);
    %insert_msr(msrid=all2_9);
    %insert_msr(msrid=all2_10);
    %insert_msr(msrid=all2_11);

%mend ;


