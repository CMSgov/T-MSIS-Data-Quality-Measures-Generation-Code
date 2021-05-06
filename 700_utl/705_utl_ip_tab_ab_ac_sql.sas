
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_ip_ab_ac_sql();

%macro utl_ip_ab_ac_clm(clmcat, tblnum);

execute(
create or replace temporary view &taskprefix._ip_prep_clm_&clmcat. as
    select

        /*unique keys and other identifiers*/
         submtg_state_cd
		,tmsis_rptg_prd 
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,max(case when (xovr_ind = '1') then 1 else 0 end) as xover_clm

    from &temptable..&taskprefix._base_clh_ip
	where claim_cat_&clmcat. = 1
	group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind    
	) by tmsis_passthrough;

execute(
    create or replace temporary view &taskprefix._ip_clm_&clmcat. as
    select
         submtg_state_cd
        ,sum(xover_clm) as all&tblnum._1_numer
        ,count(submtg_state_cd) as all&tblnum._1_denom
        ,round((sum(xover_clm) / count(submtg_state_cd)),2) as all&tblnum._1

    from &taskprefix._ip_prep_clm_&clmcat.
    group by submtg_state_cd
	) by tmsis_passthrough;

%mend utl_ip_ab_ac_clm;

%utl_ip_ab_ac_clm(ab, 6);
%utl_ip_ab_ac_clm(ac, 9);

%mend utl_ip_ab_ac_sql;

/** 2019/01/17: new measures - 13.1 and 13.4 **/

%macro utl_link_ip_el_ab_sql;

/**read header */
execute(
create or replace temporary view &taskprefix._ip_hdr_clm_ab as
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
		,admsn_dt
		,case 
	          %do i=1 %to 12;
	              when array_contains(code_cm,dgns_&i._cd) then 1
	          %end; else 0 end as prgncy_dx
		,case 
	          %do i=1 %to 6;
	              when array_contains(code_pcs,prcdr_&i._cd) then 1
	          %end; else 0 end as prgncy_pcs
    from &temptable..&taskprefix._base_clh_ip a
	left join &temptable..&taskprefix._prgncy_codes b
	on a.submtg_state_cd = b.submtg_state_cd
	where claim_cat_ab = 1
	) by tmsis_passthrough;

/**read line */

execute(
create or replace temporary view &taskprefix._ip_line_to_hdr_rollup_ab as
    select
         submtg_state_cd
        ,tmsis_rptg_prd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind 
	    ,max(case when rev_cd in ('450', '451', '452', '453', '454', '455', '456', '457', '458', '459',
                                  '0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459',
								  '981','720','721','722','723','724','729',
								  '0981','0720','0721','0722','0723','0724','0729'
                                   ) 
                  then 1 else 0 end) as rev_cd_excl

    from &temptable..&taskprefix._base_cll_ip
	where claim_cat_ab = 1
	group by submtg_state_cd, tmsis_rptg_prd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind   

	) by tmsis_passthrough;
	
/** merge header and line */

/** we are merging header and line instead of rolling up the line because we want to count the msis id in header.
	if there is any msis id in line file not in header, we don't want to include it. 
	there can be a msis id in line file not in header because the msis id is not a linking variable between header and line files.

**/
/*
execute(
	    create or replace temporary view &taskprefix._ip_hdr_clm_bene_ab as
	    select
	         a.submtg_state_cd
		    ,a.msis_ident_num
	        ,a.tmsis_rptg_prd
	        ,a.orgnl_clm_num
	        ,a.adjstmt_clm_num
	        ,a.adjdctn_dt
			,a.adjstmt_ind
	        ,a.admsn_dt
		    ,max(case when b.rev_cd_excl=1 or a.prgncy_dx=1 or a.prgncy_pcs=1 then 1 else 0 end ) as all13_5_numer_excl
	    from &taskprefix._ip_hdr_clm_ab a 
        left join &taskprefix._ip_line_to_hdr_rollup_ab b
			 on a.submtg_state_cd=b.submtg_state_cd and
			    a.tmsis_rptg_prd=b.tmsis_rptg_prd and
				a.orgnl_clm_num=b.orgnl_clm_num and
				a.adjstmt_clm_num=b.adjstmt_clm_num and
				a.adjdctn_dt=b.adjdctn_dt and
				a.adjstmt_ind=b.adjstmt_ind
	    group by a.submtg_state_cd, a.msis_ident_num, a.tmsis_rptg_prd, a.orgnl_clm_num, a.adjstmt_clm_num, a.adjdctn_dt, a.adjstmt_ind, a.admsn_dt
	) by tmsis_passthrough;*/

  
	/*merge to ever eligible **/
	/*
  	execute(
	create or replace temporary view &taskprefix._ip_hdr_ab_ever_elig as
	select a.*
	      ,case when ALL13_5_numer_excl =1 then 0 else 1 end as ALL13_5_numer_orig 

    from &taskprefix._ip_hdr_clm_bene_ab a
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
	create or replace temporary view &taskprefix._ip_hdr_ab_ever_elig2 as
	select a.*
	       ,b.elgblty_dtrmnt_efctv_dt
		   ,b.elgblty_dtrmnt_end_dt
	       ,case when b.rstrctd_bnfts_cd in ('2') then 1 else 0 end as all13_5_denom_orig

    from &taskprefix._ip_hdr_ab_ever_elig a
    inner join &temptable..&taskprefix._ever_elig_dtrmnt b
        on a.submtg_state_cd=b.submtg_state_cd and
	       a.msis_ident_num=b.msis_ident_num and 
		  (a.admsn_dt>=b.elgblty_dtrmnt_efctv_dt and a.admsn_dt is not null ) and 
		  (a.admsn_dt<=b.elgblty_dtrmnt_end_dt or b.elgblty_dtrmnt_end_dt is null)
    	) by tmsis_passthrough;*/

	/*now rolling upto one record per msis id*/
/*
execute(
    create or replace temporary view &taskprefix._ip_el_lnk_ab as
    select
         submtg_state_cd
        ,msis_ident_num
	    ,max(all13_5_denom_orig) as all13_5_denom_1
		,max(case when all13_5_denom_orig=1 then all13_5_numer_orig else 0 end) as all13_5_numer_1 
	
    from &taskprefix._ip_hdr_ab_ever_elig2
    group by submtg_state_cd,msis_ident_num
    ) by tmsis_passthrough;

execute(
    create table &wrktable..&taskprefix._utl_ip_el_lnk_pct_ab as
    create or replace temporary view &taskprefix._utl_ip_el_lnk_pct_ab as
    select
		 submtg_state_cd
		,all13_5_numer
		,all13_5_denom
        ,case when all13_5_denom >0 then round((all13_5_numer /all13_5_denom),2) 
              else null end as all13_5
    from 
		(select
		  	 submtg_state_cd
        	,sum(all13_5_numer_1) as all13_5_numer
        	,sum(all13_5_denom_1) as all13_5_denom
		 from &taskprefix._ip_el_lnk_ab
    	  group by submtg_state_cd
		  ) a
	) by tmsis_passthrough;	*/

EXECUTE (
  CREATE OR replace temporary VIEW &taskprefix._all_13_5 AS
  
  SELECT submtg_state_cd, SUM(all13_5_numer_1) AS all13_5_numer, SUM(all13_5_denom_1) AS all13_5_denom
  
  FROM (

    SELECT submtg_state_cd, msis_ident_num
           ,max(all13_5_denom_orig) as all13_5_denom_1
           ,max(case when all13_5_denom_orig=1 then all13_5_numer_orig else 0 end) as all13_5_numer_1 

      FROM (
        
        SELECT
          q1.submtg_state_cd
          , q1.msis_ident_num
          , CASE WHEN q3.rstrctd_bnfts_cd IN ('2') THEN 1 ELSE 0 END AS all13_5_denom_orig
          , CASE WHEN all13_5_numer_excl = 1 THEN 0 ELSE 1 END AS all13_5_numer_orig
                                                              
          FROM (
            /* ip_hdr_clm_bene_ab */
            SELECT 
              a.submtg_state_cd
              ,a.msis_ident_num
              ,a.tmsis_rptg_prd
              ,a.orgnl_clm_num
              ,a.adjstmt_clm_num
              ,a.adjdctn_dt
              ,a.adjstmt_ind
              ,a.admsn_dt
              ,max(CASE WHEN b.rev_cd_excl=1 OR a.prgncy_dx=1 OR a.prgncy_pcs=1 THEN 1 ELSE 0 END) as all13_5_numer_excl 
              FROM
                  &taskprefix._ip_hdr_clm_ab a 
                  LEFT JOIN
                  &taskprefix._ip_line_to_hdr_rollup_ab b
                      ON
                  a.submtg_state_cd=b.submtg_state_cd AND 
                  a.tmsis_rptg_prd=b.tmsis_rptg_prd AND 
                  a.orgnl_clm_num=b.orgnl_clm_num AND 
                  a.adjstmt_clm_num=b.adjstmt_clm_num AND 
                  a.adjdctn_dt=b.adjdctn_dt AND 
                  a.adjstmt_ind=b.adjstmt_ind
                  
             GROUP BY a.submtg_state_cd, a.msis_ident_num, a.tmsis_rptg_prd, a.orgnl_clm_num, a.adjstmt_clm_num,
                      a.adjdctn_dt, a.adjstmt_ind, a.admsn_dt) q1 /* ip_hdr_clm_bene_ab */

                 INNER JOIN (
                   /* q1 ij q2 = ip_hdr_ab_ever_elig */
                   SELECT
                     submtg_state_cd
                     , msis_ident_num
                     FROM &temptable..&taskprefix._ever_elig
                    WHERE ever_eligible = 1
                    GROUP BY submtg_state_cd, msis_ident_num) q2 /* LJ + IJ = ip_hdr_ab_ever_elig */
                     ON
                 q1.submtg_state_cd = q2.submtg_state_cd AND
                 q1.msis_ident_num = q2.msis_ident_num

                 INNER JOIN
                 /* q2 ij q3 = ip_hdr_ab_ever_elig2 */
                 
                 &temptable..&taskprefix._ever_elig_dtrmnt q3 /* lj, ij, ij = ip_hdr_ab_ever_elig2 */
                     ON
                 q1.submtg_state_cd = q3.submtg_state_cd AND 
                 q1.msis_ident_num = q3.msis_ident_num AND 
                 (q1.admsn_dt>=q3.elgblty_dtrmnt_efctv_dt and q1.admsn_dt is not null ) AND 
                 (q1.admsn_dt<=q3.elgblty_dtrmnt_end_dt or q3.elgblty_dtrmnt_end_dt is NULL)

      ) q4

GROUP BY q4.submtg_state_cd, q4.msis_ident_num) q5
GROUP BY submtg_state_cd

) BY tmsis_passthrough;

execute (
    insert into &utl_output

    select
    submtg_state_cd
            , 'all6_1'
            , '705'
    ,all6_1_numer
    ,all6_1_denom
    ,all6_1
            , null
            , null
    from #temp.&taskprefix._ip_clm_ab

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
            , 'all9_1'
            , '705'
    ,all9_1_numer
    ,all9_1_denom
    ,all9_1
            , null
            , null
    from #temp.&taskprefix._ip_clm_ac

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
            , 'all13_5'
            , '705'
    ,all13_5_numer
    ,all13_5_denom
    ,case when all13_5_denom > 0 then round((all13_5_numer /all13_5_denom),2) else null end 
            , null
            , null
    from #temp.&taskprefix._all_13_5 /*#temp.&taskprefix._utl_ip_el_lnk_pct_ab*/ /*&wrktable..&taskprefix._utl_ip_el_lnk_pct_ab */

    
    ) by tmsis_passthrough;	

    %insert_msr(msrid=all6_1);
    %insert_msr(msrid=all9_1);
    %insert_msr(msrid=all13_5);

%mend ;


