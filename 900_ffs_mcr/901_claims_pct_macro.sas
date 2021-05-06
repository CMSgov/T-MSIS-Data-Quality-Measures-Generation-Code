/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
 /******************************************************************************************
 Program: 901_claims_pct_macro.sas 
 Project: MACBIS Task 2
 Purpose: Defines and calls claims percentage macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          
 
 Author:  Richard Chapman
 Date Created: 3/1/2017
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure, and inserts that measure into an AREMAC table.
         At the end of the program, the AREMAC table is extracted into SAS
 
 Modifications:
 ******************************************************************************************/ 
 
 /******************************************************************************************
   Macro #1: Claims Percentage
   
   Can be run on either measures that are strictly line level or strictly header level
   (But not ones that require identifying claims with at least one line that meets a
    certain criteria -- use the next macro for those measures)
  ******************************************************************************************/
  
 
 %macro claims_pct(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  denom=,      /*Logical constraints to apply to the denominator, as defined in specs. Often set to %wtr(1=1), which will not apply any contstraints to denom*/
  numer=,      /*Logical constraints to apply to the denominator, as defined in specs.*/
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );

  sysecho "Calculating measure &measure_id.";
  execute(
    insert into &wrktable..&taskprefix._clms_900a
    select 
     %str(%')&state.%str(%') as submtg_state_cd
    ,%str(%')&measure_id.%str(%') as measure_id
    ,coalesce(numer,0) as numer
    ,coalesce(denom,0) as denom
    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
        from (
          select 
             sum(case when (&numer.) then 1 else 0 end) as numer
          ,sum(1) as denom
    from &temptable..&taskprefix._base_&level._&claim_type.
    where (&denom.) 
    and claim_cat_&claim_cat. = 1
      %if %lowcase(&level.) = cll %then %do;
        and childless_header_flag = 0
      %end;
  ) a 
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
  
%mend claims_pct;

/******************************************************************************************
   Macro #2: Claims Percentage for measures that roll up from line to header level
   
   Run this macro for percentages that require checking whether checking whether a 
   claim has at least one line that meets certain criteria, in order to include that
   claim in a claim-level percentage
 ******************************************************************************************/

%macro claims_pct_cll_to_clh(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  denom_line=, /*Logical constraints to apply to the denominator, as defined in specs. Often set to %wtr(1=1), which will not apply any contstraints to denom*/
  numer_line=, /*Logical constraints to apply to the denominator, as defined in specs.*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );

  sysecho "Calculating measure &measure_id.";
  
  /*step 1: roll up from line level to header level.
            cannot just use the pre-created header level file
            because the constraints require us to check to
            see if the claim has at least one line that
            meets certain contraints

            ok to take the max of the claim_cat binaries and the
            variable that we are going to average because these
            values do not vary within claim. (Could similarly take
            min or avg if we wanted to and would get same result).  */
   
  execute(
    create or replace temporary view &taskprefix._clh as
    
    select tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
           ,max(claim_cat_&claim_cat.)                      as claim_cat_&claim_cat.
           ,max(case when (&denom_line.)                    then 1 else 0 end) as has_line_denom
           ,max(case when (&denom_line.) and (&numer_line.) then 1 else 0 end) as has_line_numer    

    from &temptable..&taskprefix._base_cll_&claim_type.

    group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;

  /*step 2: take percentage at the header level among claims
            that meet all denominator criteria*/

  execute(
    insert into &wrktable..&taskprefix._clms_900a
    select 
     %str(%')&state.%str(%') as submtg_state_cd
    ,%str(%')&measure_id.%str(%') as measure_id
    ,coalesce(numer,0) as numer
    ,coalesce(denom,0) as denom 
    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
      from (
        select 
              sum(has_line_numer) as numer
         ,sum(1) as denom
      from #temp.&taskprefix._clh
    where claim_cat_&claim_cat. = 1 
      and has_line_denom=1
    ) a   
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);

%mend claims_pct_cll_to_clh;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/
 

%macro run_901_all_claims_pct;

 
 /******************************************************************************************
   Step 0 - Create Table to hold output
  ******************************************************************************************/

  %dropwrktables(clms_900a);
  execute(
	  create table &wrktable..&taskprefix._clms_900a (

	     submtg_state_cd STRING
	    ,measure_id STRING
	    ,numer DOUBLE
	    ,denom DOUBLE
	    ,mvalue DOUBLE
	)
  
  )by tmsis_passthrough;

 /******************************************************************************************
   Then use macro calls to calculate measures and insert results into the table
  ******************************************************************************************/

* ffs 1.1, ffs 3.1, mcr 1.1, mcr 3.1;
%claims_pct(measure_id=ffs1_1,  claim_cat=A, denom=%str(1=1), numer=%str(admsn_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_1,  claim_cat=F, denom=%str(1=1), numer=%str(admsn_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_1,  claim_cat=P, denom=%str(1=1), numer=%str(admsn_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_1,  claim_cat=R, denom=%str(1=1), numer=%str(admsn_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=ip);


* ffs-1.3, ffs-3.3, mcr-1.3, mcr-3.3;
%claims_pct_cll_to_clh(measure_id=ffs1_3,  claim_cat=A, denom_line=%str(1=1), numer_line=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=ffs3_3,  claim_cat=F, denom_line=%str(1=1), numer_line=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr1_3,  claim_cat=P, denom_line=%str(1=1), numer_line=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr3_3,  claim_cat=R, denom_line=%str(1=1), numer_line=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), claim_type=ip);


* ffs 5.10, ffs 7.10, mcr 5.10, mcr 7.10;
%claims_pct(measure_id=ffs5_10,  claim_cat=A, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_10,  claim_cat=F, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_10,  claim_cat=P, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_10,  claim_cat=R, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=lt);

* ffs 9.8, ffs 11.8, mcr 10.8, mcr 14.8;
%claims_pct(measure_id=ffs9_8,  claim_cat=A, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_8, claim_cat=F, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_8, claim_cat=P, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_8, claim_cat=R, denom=%str(1=1), numer=%str(srvc_endg_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLL, claim_type=ot);

* ffs-14.7, ffs-16.7, mcr-17.7, mcr-19.7;
%claims_pct(measure_id=ffs14_7, claim_cat=A, denom=%str(1=1), numer=%str(rx_fill_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=rx);
%claims_pct(measure_id=ffs16_7, claim_cat=F, denom=%str(1=1), numer=%str(rx_fill_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr17_7, claim_cat=P, denom=%str(1=1), numer=%str(rx_fill_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr19_7, claim_cat=R, denom=%str(1=1), numer=%str(rx_fill_dt >= date_sub(add_months(tmsis_rptg_prd,-11),1)), level=CLH, claim_type=rx);

* ffs-1.2, ffs-3.2, mcr-1.2, mcr-3.2;
%claims_pct(measure_id=ffs1_2,  claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_2,  claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_2,  claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_2,  claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=ip);

* ffs-5.9, ffs-7.9, mcr-5.9, mcr-7.9;
%claims_pct(measure_id=ffs5_9,  claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_9,  claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_9,  claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_9,  claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('20','40','41','42')), level=CLH, claim_type=lt);

* ffs-1.4, ffs-3.4, mcr-1.4, mcr-3.4;
%claims_pct(measure_id=ffs1_4,  claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_4,  claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_4,  claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_4,  claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=ip);

* ffs-5.11, ffs-7.11, mcr-5.11, mcr-7.11;
%claims_pct(measure_id=ffs5_11, claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_11, claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_11, claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_21, claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('01','06','08','50','81','86')), level=CLH, claim_type=lt);

* ffs-1.17, ffs-3.5, mcr-1.5, mcr-3.5;
%claims_pct(measure_id=ffs1_17, claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('02','03','04','05','43','51','61','62','63','64','65','66','70','82','83','84','85','88','89','90','91','92','93','94','95')), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_5,  claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('02','03','04','05','43','51','61','62','63','64','65','66','70','82','83','84','85','88','89','90','91','92','93','94','95')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_5,  claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('02','03','04','05','43','51','61','62','63','64','65','66','70','82','83','84','85','88','89','90','91','92','93','94','95')), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_5,  claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD in ('02','03','04','05','43','51','61','62','63','64','65','66','70','82','83','84','85','88','89','90','91','92','93','94','95')), level=CLH, claim_type=ip);

* ffs-1.18, ffs-3.6, mcr-1.6, mcr-3.6;
%claims_pct(measure_id=ffs1_18, claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_6,  claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_6,  claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_6,  claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=ip);

* ffs-5.25, ffs-7.15, mcr-5.16, mcr-7.15;
%claims_pct(measure_id=ffs5_25, claim_cat=A, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_15, claim_cat=F, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_16, claim_cat=P, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_15, claim_cat=R, denom=%str(1=1), numer=%str(PTNT_STUS_CD = '30'), level=CLH, claim_type=lt);

* ffs-1.31, ffs-3.19, mcr-1.19, mcr-3.19;
%claims_pct(measure_id=ffs1_31,  claim_cat=A, denom=%str(1=1), numer=%str(SUBSTRING(DRG_CD_IND,1,2) = 'HG'), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_19,  claim_cat=F, denom=%str(1=1), numer=%str(SUBSTRING(DRG_CD_IND,1,2) = 'HG'), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_19,  claim_cat=P, denom=%str(1=1), numer=%str(SUBSTRING(DRG_CD_IND,1,2) = 'HG'), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_19,  claim_cat=R, denom=%str(1=1), numer=%str(SUBSTRING(DRG_CD_IND,1,2) = 'HG'), level=CLH, claim_type=ip);

* ffs-1.20, ffs-3.8, mcr-1.8, mcr-3.8;
%claims_pct(measure_id=ffs1_20, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(drg_cd,4)), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_8,  claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(drg_cd,4)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_8,  claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(drg_cd,4)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_8,  claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(drg_cd,4)), level=CLH, claim_type=ip);

* ffs-1.21, ffs-3.9, mcr-1.9, mcr-3.9;
%claims_pct(measure_id=ffs1_21, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_9,  claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_9,  claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_9,  claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=ip);

* ffs-5.27, ffs-7.17, mcr-5.18, mcr-7.17;
%claims_pct(measure_id=ffs5_27, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_17, claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_18, claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_17, claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLH, claim_type=lt);

* ffs-1.22, ffs-3.10, mcr-1.10, mcr-3.10;
%claims_pct(measure_id=ffs1_22, claim_cat=A, denom=%str(1=1), 
            numer=%quote( %not_missing_1(DGNS_1_CD,7) and not %not_missing_1(DGNS_2_CD,7)  and not %not_missing_1(DGNS_3_CD,7)  and not %not_missing_1(DGNS_4_CD,7)
                 and not %not_missing_1(DGNS_5_CD,7) and not %not_missing_1(DGNS_6_CD,7)  and not %not_missing_1(DGNS_7_CD,7)  and not %not_missing_1(DGNS_8_CD,7)
             and not %not_missing_1(DGNS_9_CD,7) and not %not_missing_1(DGNS_10_CD,7) and not %not_missing_1(DGNS_11_CD,7) and not %not_missing_1(DGNS_12_CD,7) )            
            ,level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_10, claim_cat=F, denom=%str(1=1), 
            numer=%quote( %not_missing_1(DGNS_1_CD,7) and not %not_missing_1(DGNS_2_CD,7)  and not %not_missing_1(DGNS_3_CD,7)  and not %not_missing_1(DGNS_4_CD,7)
                 and not %not_missing_1(DGNS_5_CD,7) and not %not_missing_1(DGNS_6_CD,7)  and not %not_missing_1(DGNS_7_CD,7)  and not %not_missing_1(DGNS_8_CD,7)
             and not %not_missing_1(DGNS_9_CD,7) and not %not_missing_1(DGNS_10_CD,7) and not %not_missing_1(DGNS_11_CD,7) and not %not_missing_1(DGNS_12_CD,7) )            
            ,level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_10, claim_cat=P, denom=%str(1=1), 
            numer=%quote( %not_missing_1(DGNS_1_CD,7) and not %not_missing_1(DGNS_2_CD,7)  and not %not_missing_1(DGNS_3_CD,7)  and not %not_missing_1(DGNS_4_CD,7)
                 and not %not_missing_1(DGNS_5_CD,7) and not %not_missing_1(DGNS_6_CD,7)  and not %not_missing_1(DGNS_7_CD,7)  and not %not_missing_1(DGNS_8_CD,7)
             and not %not_missing_1(DGNS_9_CD,7) and not %not_missing_1(DGNS_10_CD,7) and not %not_missing_1(DGNS_11_CD,7) and not %not_missing_1(DGNS_12_CD,7) )            
            ,level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_10, claim_cat=R, denom=%str(1=1), 
            numer=%quote( %not_missing_1(DGNS_1_CD,7) and not %not_missing_1(DGNS_2_CD,7)  and not %not_missing_1(DGNS_3_CD,7)  and not %not_missing_1(DGNS_4_CD,7)
                 and not %not_missing_1(DGNS_5_CD,7) and not %not_missing_1(DGNS_6_CD,7)  and not %not_missing_1(DGNS_7_CD,7)  and not %not_missing_1(DGNS_8_CD,7)
             and not %not_missing_1(DGNS_9_CD,7) and not %not_missing_1(DGNS_10_CD,7) and not %not_missing_1(DGNS_11_CD,7) and not %not_missing_1(DGNS_12_CD,7) )            
            ,level=CLH, claim_type=ip);                                    

* ffs-1.23, ffs-3.11, mcr-1.11, mcr-3.11;
%claims_pct(measure_id=ffs1_23, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(PRCDR_1_CD,8)), level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs3_11, claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(PRCDR_1_CD,8)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr1_11, claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(PRCDR_1_CD,8)), level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr3_11, claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(PRCDR_1_CD,8)), level=CLH, claim_type=ip);

* ffs-11.1, ffs-9.1, mcr-10.1, mcr-14.1;
%claims_pct(measure_id=ffs9_1,  claim_cat=A, denom=%str(STC_CD IN ('002', '061')), numer=%str(SUBSTRING(REV_CD,1,2)='01' or  SUBSTRING(REV_CD,1,3) in ('020','021')), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_1, claim_cat=F, denom=%str(STC_CD IN ('002', '061')), numer=%str(SUBSTRING(REV_CD,1,2)='01' or  SUBSTRING(REV_CD,1,3) in ('020','021')), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_1, claim_cat=P, denom=%str(STC_CD IN ('002', '061')), numer=%str(SUBSTRING(REV_CD,1,2)='01' or  SUBSTRING(REV_CD,1,3) in ('020','021')), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_1, claim_cat=R, denom=%str(STC_CD IN ('002', '061')), numer=%str(SUBSTRING(REV_CD,1,2)='01' or  SUBSTRING(REV_CD,1,3) in ('020','021')), level=CLL, claim_type=ot);

* ffs-11.2, ffs-9.2, mcr-10.2, mcr-14.2; 
%claims_pct(measure_id=ffs9_2,  claim_cat=A, denom=%str(1=1), numer=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_2, claim_cat=F, denom=%str(1=1), numer=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_2, claim_cat=P, denom=%str(1=1), numer=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_2, claim_cat=R, denom=%str(1=1), numer=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), level=CLL, claim_type=ot);

* ffs-11.3, ffs-9.3, mcr-10.3, mcr-14.3;    
%claims_pct(measure_id=ffs9_3,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '01'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_3, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '01'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_3, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '01'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_3, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '01'), level=CLL, claim_type=ot);

* ffs-11.5, ffs-9.5, mcr-10.5, mcr-14.5;
%claims_pct(measure_id=ffs9_5,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '05'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_5, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '05'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_5, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '05'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_5, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '05'), level=CLL, claim_type=ot);

* ffs-11.6, ffs-9.6, mcr-10.6, mcr-14.6;                 
%claims_pct(measure_id=ffs9_6,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '04'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_6, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '04'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_6, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '04'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_6, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '04'), level=CLL, claim_type=ot);

* ffs-11.7, ffs-9.7, mcr-10.7, mcr-14.7;    
%claims_pct(measure_id=ffs9_7,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '03'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_7, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '03'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_7, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '03'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_7, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '03'), level=CLL, claim_type=ot);
                                                                 
* ffs-11.9, ffs-9.9, mcr-10.9, mcr-14.9;
%claims_pct(measure_id=ffs9_9,   claim_cat=A, denom=%str(1=1), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_9,  claim_cat=F, denom=%str(1=1), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_9,  claim_cat=P, denom=%str(1=1), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_9,  claim_cat=R, denom=%str(1=1), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);                       

* ffs-11.10, ffs-9.10, mcr-10.10, mcr-14.10;
%claims_pct(measure_id=ffs9_10,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '06'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_10, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '06'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_10, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '06'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_10, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND = '06'), level=CLL, claim_type=ot);                                          
                                                                    
* ffs-11.16, ffs-9.16, mcr-10.16, mcr-14.16;
%claims_pct(measure_id=ffs9_16,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND in ('02','07')), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_16, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND in ('02','07')), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_16, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND in ('02','07')), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_16, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND in ('02','07')), level=CLL, claim_type=ot);
   
* ffs-11.17, ffs-9.17, mcr-10.17, mcr-14.17;                        
%claims_pct(measure_id=ffs9_17,  claim_cat=A, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND rlike '([1-7][0-9])|(8[0-7])'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_17, claim_cat=F, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND rlike '([1-7][0-9])|(8[0-7])'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_17, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND rlike '([1-7][0-9])|(8[0-7])'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_17, claim_cat=R, denom=%str(STC_CD IN ('012','025','026')), numer=%str(PRCDR_CD_IND rlike '([1-7][0-9])|(8[0-7])'), level=CLL, claim_type=ot);

* ffs-11.18, ffs-9.18, mcr-10.18, mcr-14.18;                     
%claims_pct(measure_id=ffs9_18,  claim_cat=A, denom=%str(1=1), numer=%quote(not %not_missing_1(srvc_plc_cd,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_18, claim_cat=F, denom=%str(1=1), numer=%quote(not %not_missing_1(srvc_plc_cd,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_18, claim_cat=P, denom=%str(1=1), numer=%quote(not %not_missing_1(srvc_plc_cd,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_18, claim_cat=R, denom=%str(1=1), numer=%quote(not %not_missing_1(srvc_plc_cd,2)), level=CLL, claim_type=ot);

* ffs-11.19, ffs-9.98, mcr-10.19, mcr-14.19;                                                      
%claims_pct(measure_id=ffs9_98,  claim_cat=A, denom=%str(1=1), numer=%str(srvc_plc_cd ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_19, claim_cat=F, denom=%str(1=1), numer=%str(srvc_plc_cd ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_19, claim_cat=P, denom=%str(1=1), numer=%str(srvc_plc_cd ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_19, claim_cat=R, denom=%str(1=1), numer=%str(srvc_plc_cd ='11'), level=CLL, claim_type=ot);                                                                

* ffs-11.21, ffs-9.100, mcr-10.21, mcr-14.21;                    
%claims_pct(measure_id=ffs9_100, claim_cat=A, denom=%str(STC_CD IN ('012','002','061','028','041')), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_21, claim_cat=F, denom=%str(STC_CD IN ('012','002','061','028','041')), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_21, claim_cat=P, denom=%str(STC_CD IN ('012','002','061','028','041')), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_21, claim_cat=R, denom=%str(STC_CD IN ('012','002','061','028','041')), numer=%quote(%not_missing_1(DGNS_1_CD,7)), level=CLL, claim_type=ot);

* ffs-11.23, ffs-9.102, mcr-10.23, mcr-14.23; 
%claims_pct(measure_id=ffs9_102, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(prcdr_cd,8)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs11_23, claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(prcdr_cd,8)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_23, claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(prcdr_cd,8)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr14_23, claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(prcdr_cd,8)), level=CLL, claim_type=ot);                                            
                                            
* ffs-14.6, ffs-16.6, mcr-17.6, mcr-19.6;                                               
%claims_pct(measure_id=ffs14_6, claim_cat=A, denom=%str(1=1), numer=%str(RX_FILL_DT = PRSCRBD_DT), level=CLH, claim_type=rx);
%claims_pct(measure_id=ffs16_6, claim_cat=F, denom=%str(1=1), numer=%str(RX_FILL_DT = PRSCRBD_DT), level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr17_6, claim_cat=P, denom=%str(1=1), numer=%str(RX_FILL_DT = PRSCRBD_DT), level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr19_6, claim_cat=R, denom=%str(1=1), numer=%str(RX_FILL_DT = PRSCRBD_DT), level=CLH, claim_type=rx);                                            

* ffs-18.1, ffs-22.1, mcr-21.1, mcr-24.1;
%claims_pct(measure_id=ffs18_1, claim_cat=A, denom=%str(STC_CD='012'), numer=%quote(%not_missing_1(SRVCNG_PRVDR_SPCLTY_CD,2)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=ffs22_1, claim_cat=F, denom=%str(STC_CD='012'), numer=%quote(%not_missing_1(SRVCNG_PRVDR_SPCLTY_CD,2)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=mcr21_1, claim_cat=P, denom=%str(STC_CD='012'), numer=%quote(%not_missing_1(SRVCNG_PRVDR_SPCLTY_CD,2)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=mcr24_1, claim_cat=R, denom=%str(STC_CD='012'), numer=%quote(%not_missing_1(SRVCNG_PRVDR_SPCLTY_CD,2)), level=CLL, claim_type=ot);                                            

* ffs-18.2, ffs-19.1, ffs-22.2, ffs-23.1, mcr-21.2, mcr-22.1, mcr-24.2, mcr-25.1;
%claims_pct(measure_id=ffs18_2, claim_cat=A, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs19_1, claim_cat=B, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs22_2, claim_cat=F, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs23_1, claim_cat=G, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr21_2, claim_cat=P, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr22_1, claim_cat=T, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr24_2, claim_cat=R, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr25_1, claim_cat=V, denom=%str(STC_CD IN ('012', '029', '015', '002', '028', '041', '061')), numer=%str(SRVCNG_PRVDR_NUM = BLG_PRVDR_NUM), level=CLL, claim_type=ot);
                                                               
* ffs-18.3, ffs-19.2, ffs-22.3, ffs-23.2, mcr-21.4, mcr-22.2, mcr-24.4, mcr-25.2;
%claims_pct(measure_id=ffs18_3, claim_cat=A, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs19_2, claim_cat=B, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs22_3, claim_cat=F, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs23_2, claim_cat=G, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr21_4, claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr22_2, claim_cat=T, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr24_4, claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr25_2, claim_cat=V, denom=%str(1=1), numer=%quote(%not_missing_1(SRVCNG_PRVDR_NUM,30)), level=CLL, claim_type=ot);                                           

* ffs-5.12, ffs-7.12, mcr-5.12, mcr-7.12;
%claims_pct(measure_id=ffs5_12, claim_cat=A, denom=%str(1=1), numer=%quote(not %is_missing_2(LVE_DAYS_CNT,5)), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_12, claim_cat=F, denom=%str(1=1), numer=%quote(not %is_missing_2(LVE_DAYS_CNT,5)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_12, claim_cat=P, denom=%str(1=1), numer=%quote(not %is_missing_2(LVE_DAYS_CNT,5)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_12, claim_cat=R, denom=%str(1=1), numer=%quote(not %is_missing_2(LVE_DAYS_CNT,5)), level=CLH, claim_type=lt);                                           

* ffs-5.13, ffs-7.13, mcr-5.13, mcr-7.13;      
%claims_pct(measure_id=ffs5_13, claim_cat=A, denom=%str(1=1), numer=%str(&ltc_days.>=28 and &ltc_days.<=31), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_13, claim_cat=F, denom=%str(1=1), numer=%str(&ltc_days.>=28 and &ltc_days.<=31), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_13, claim_cat=P, denom=%str(1=1), numer=%str(&ltc_days.>=28 and &ltc_days.<=31), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_13, claim_cat=R, denom=%str(1=1), numer=%str(&ltc_days.>=28 and &ltc_days.<=31), level=CLH, claim_type=lt);

* ffs-5.23, ffs-7.14, mcr-5.14, mcr-7.14;
%claims_pct(measure_id=ffs5_23, claim_cat=A, denom=%str(1=1), numer=%str(&ltc_days.>31 or (&ltc_days.<28 and (&ltc_days.<6 or &ltc_days.>8))), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_14, claim_cat=F, denom=%str(1=1), numer=%str(&ltc_days.>31 or (&ltc_days.<28 and (&ltc_days.<6 or &ltc_days.>8))), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_14, claim_cat=P, denom=%str(1=1), numer=%str(&ltc_days.>31 or (&ltc_days.<28 and (&ltc_days.<6 or &ltc_days.>8))), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_14, claim_cat=R, denom=%str(1=1), numer=%str(&ltc_days.>31 or (&ltc_days.<28 and (&ltc_days.<6 or &ltc_days.>8))), level=CLH, claim_type=lt);                                           
                                           
* ffs-5.24, mcr-5.15;                                           
%claims_pct(measure_id=ffs5_24, claim_cat=A, denom=%str(1=1), numer=%quote(not %is_missing_3(LTC_RCP_LBLTY_AMT,13,2)), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_15, claim_cat=P, denom=%str(1=1), numer=%quote(not %is_missing_3(LTC_RCP_LBLTY_AMT,13,2)), level=CLH, claim_type=lt);                                           
                                           
* ffs-5.26, ffs-7.16, mcr-5.17, mcr-7.16;  
%claims_pct(measure_id=ffs5_26, claim_cat=A, denom=%str(1=1), numer=%str(&ltc_days.>=6 and &ltc_days.<=8), level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs7_16, claim_cat=F, denom=%str(1=1), numer=%str(&ltc_days.>=6 and &ltc_days.<=8), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr5_17, claim_cat=P, denom=%str(1=1), numer=%str(&ltc_days.>=6 and &ltc_days.<=8), level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr7_16, claim_cat=R, denom=%str(1=1), numer=%str(&ltc_days.>=6 and &ltc_days.<=8), level=CLH, claim_type=lt);                                           
                                           
* ffs-10.1, ffs-12.1, mcr-11.1, mcr-15.1;
%claims_pct(measure_id=ffs10_1, claim_cat=B, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD_IND,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs12_1, claim_cat=G, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD_IND,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr11_1, claim_cat=T, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD_IND,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr15_1, claim_cat=V, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD_IND,2)), level=CLL, claim_type=ot);                                           

* ffs-10.2, mcr-11.2;                                                                        
%claims_pct(measure_id=ffs10_2, claim_cat=B, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD,8) or %not_missing_1(REV_CD,4)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr11_2, claim_cat=T, denom=%str(MDCD_PD_AMT > 0), numer=%quote(%not_missing_1(PRCDR_CD,8) or %not_missing_1(REV_CD,4)), level=CLL, claim_type=ot);
                
* ffs-10.3, ffs-12.2, mcr-11.3, mcr-15.2;
%claims_pct(measure_id=ffs10_3, claim_cat=B, denom=%str(STC_CD IN ('012','002','061') and MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs12_2, claim_cat=G, denom=%str(STC_CD IN ('012','002','061') and MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr11_3, claim_cat=T, denom=%str(STC_CD IN ('012','002','061') and MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr15_2, claim_cat=V, denom=%str(STC_CD IN ('012','002','061') and MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD = '23'), level=CLL, claim_type=ot);
                                      
* ffs-10.4, ffs-12.3, mcr-11.4, mcr-15.3;                                    
%claims_pct(measure_id=ffs10_4, claim_cat=B, denom=%str(MDCD_PD_AMT > 0), numer=%quote(not %not_missing_1(SRVC_PLC_CD,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs12_3, claim_cat=G, denom=%str(MDCD_PD_AMT > 0), numer=%quote(not %not_missing_1(SRVC_PLC_CD,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr11_4, claim_cat=T, denom=%str(MDCD_PD_AMT > 0), numer=%quote(not %not_missing_1(SRVC_PLC_CD,2)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr15_3, claim_cat=V, denom=%str(MDCD_PD_AMT > 0), numer=%quote(not %not_missing_1(SRVC_PLC_CD,2)), level=CLL, claim_type=ot);                                      
                                      
* ffs-10.84, ffs-12.4, mcr-11.5, mcr-15.4;                                      
%claims_pct(measure_id=ffs10_84, claim_cat=B, denom=%str(MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs12_4,  claim_cat=G, denom=%str(MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr11_5,  claim_cat=T, denom=%str(MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD ='11'), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr15_4,  claim_cat=V, denom=%str(MDCD_PD_AMT > 0), numer=%str(SRVC_PLC_CD ='11'), level=CLL, claim_type=ot);                                      
                                      
* ffs-10.5, ffs-13.1, ffs-9.19, mcr-12.1, mcr-16.1;
%claims_pct(measure_id=ffs9_19,  claim_cat=A, denom=%quote(%not_missing_1(REV_CD,4)), numer=%quote(%not_missing_1(HCPCS_RATE,14)), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs10_5,  claim_cat=B, denom=%quote(%not_missing_1(REV_CD,4)), numer=%quote(%not_missing_1(HCPCS_RATE,14)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=ffs13_1,  claim_cat=H, denom=%quote(%not_missing_1(REV_CD,4)), numer=%quote(%not_missing_1(HCPCS_RATE,14)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=mcr12_1,  claim_cat=Q, denom=%quote(%not_missing_1(REV_CD,4)), numer=%quote(%not_missing_1(HCPCS_RATE,14)), level=CLL, claim_type=ot); 
%claims_pct(measure_id=mcr16_1,  claim_cat=S, denom=%quote(%not_missing_1(REV_CD,4)), numer=%quote(%not_missing_1(HCPCS_RATE,14)), level=CLL, claim_type=ot);                                      
                                      
****************************** STC CODE LISTS ************************************;

%claims_pct_cll_to_clh(measure_id=ffs1_5,  claim_cat=A, denom_line=%str(1=1), numer_line=%str(STC_CD = '001'), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=ffs2_1,  claim_cat=B, denom_line=%str(1=1), numer_line=%str(STC_CD = '001'), claim_type=ip);

**********************************************************************************************************************************************;

*sumffs.1, sumffs.12, summcr.1, summcr.12;
%claims_pct(measure_id=sumffs_1,  claim_cat=M, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=ip);
%claims_pct(measure_id=sumffs_12, claim_cat=H, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=ip);
%claims_pct(measure_id=summcr_1,  claim_cat=Q, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=ip);
%claims_pct(measure_id=summcr_12, claim_cat=S, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=ip);

* sumffs.4, sumffs.15, summcr.4, summcr.15;
%claims_pct(measure_id=sumffs_4,  claim_cat=M, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=lt);
%claims_pct(measure_id=sumffs_15, claim_cat=H, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=lt);
%claims_pct(measure_id=summcr_4,  claim_cat=Q, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=lt);
%claims_pct(measure_id=summcr_15, claim_cat=S, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLH, claim_type=lt);

* sumffs.7, sumffs.18, summcr.7, summcr.18;
%claims_pct(measure_id=sumffs_7,  claim_cat=M, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLL, claim_type=ot);
%claims_pct(measure_id=sumffs_18, claim_cat=H, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLL, claim_type=ot);
%claims_pct(measure_id=summcr_7,  claim_cat=Q, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLL, claim_type=ot);
%claims_pct(measure_id=summcr_18, claim_cat=S, denom=%str(1=1), numer=%str(XOVR_IND = '1'), level=CLL, claim_type=ot);

* sumffs.2, sumffs.13, summcr.2, summcr.13;
%claims_pct(measure_id=sumffs_2,   claim_cat=C, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=ip);
%claims_pct(measure_id=sumffs_13,  claim_cat=I, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=ip);
%claims_pct(measure_id=summcr_2,   claim_cat=O, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=ip);
%claims_pct(measure_id=summcr_13,  claim_cat=U, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=ip);

* sumffs.5, sumffs.16, summcr.5, summcr.16;
%claims_pct(measure_id=sumffs_5,   claim_cat=C, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=lt);
%claims_pct(measure_id=sumffs_16,  claim_cat=I, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=lt);
%claims_pct(measure_id=summcr_5,   claim_cat=O, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=lt);
%claims_pct(measure_id=summcr_16,  claim_cat=U, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=lt);

* sumffs.8, sumffs.19, summcr.8, summcr.19;
%claims_pct(measure_id=sumffs_8,   claim_cat=C, denom=%str(1=1), numer=%str(LINE_ADJSTMT_IND = '0'), level=CLL, claim_type=ot);
%claims_pct(measure_id=sumffs_19,  claim_cat=I, denom=%str(1=1), numer=%str(LINE_ADJSTMT_IND = '0'), level=CLL, claim_type=ot);
%claims_pct(measure_id=summcr_8,   claim_cat=O, denom=%str(1=1), numer=%str(LINE_ADJSTMT_IND = '0'), level=CLL, claim_type=ot);
%claims_pct(measure_id=summcr_19,  claim_cat=U, denom=%str(1=1), numer=%str(LINE_ADJSTMT_IND = '0'), level=CLL, claim_type=ot);

* sumffs.10, sumffs.21, summcr.10, summcr.21;
%claims_pct(measure_id=sumffs_10, claim_cat=C, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=rx);
%claims_pct(measure_id=sumffs_21, claim_cat=I, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=rx);
%claims_pct(measure_id=summcr_10, claim_cat=O, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=rx);
%claims_pct(measure_id=summcr_21, claim_cat=U, denom=%str(1=1), numer=%str(ADJSTMT_IND = '0'), level=CLH, claim_type=rx);

* ffs26.1, ffs26.9, mcr32.1, mcr32.11;
%claims_pct(measure_id=ffs26_1,  claim_cat=C, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs26_9,  claim_cat=I, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr32_1,  claim_cat=O, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr32_11, claim_cat=U, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=ip);

* ffs26.2, ffs26.10, mcr32.2, mcr32.12;
%claims_pct(measure_id=ffs26_2,  claim_cat=C, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ip);
%claims_pct(measure_id=ffs26_10, claim_cat=I, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ip);
%claims_pct(measure_id=mcr32_2,  claim_cat=O, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ip);
%claims_pct(measure_id=mcr32_12, claim_cat=U, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ip);

* ffs26.3, ffs26.11, mcr32.3, mcr32.13;
%claims_pct(measure_id=ffs26_3,  claim_cat=C, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=lt);
%claims_pct(measure_id=ffs26_11, claim_cat=I, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr32_3,  claim_cat=O, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr32_13, claim_cat=U, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=lt);

* ffs26.4, ffs26.12, mcr32.4, mcr32.14;
%claims_pct(measure_id=ffs26_4,  claim_cat=C, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=lt);
%claims_pct(measure_id=ffs26_12, claim_cat=I, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=lt);
%claims_pct(measure_id=mcr32_4,  claim_cat=O, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=lt);
%claims_pct(measure_id=mcr32_14, claim_cat=U, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=lt);

* ffs26.5, ffs26.13, mcr32.5, mcr32.6, mcr32.15, mcr32.16;
%claims_pct(measure_id=ffs26_5,  claim_cat=C, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);
%claims_pct(measure_id=ffs26_13, claim_cat=I, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);
%claims_pct(measure_id=mcr32_5,  claim_cat=Y, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);
%claims_pct(measure_id=mcr32_6,  claim_cat=O, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);
%claims_pct(measure_id=mcr32_15, claim_cat=Z, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);
%claims_pct(measure_id=mcr32_16, claim_cat=U, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')       or ADJSTMT_IND is NULL),      level=CLH, claim_type=ot);

* ffs26.6, ffs26.14, mcr32.7, mcr32.8, mcr32.17, mcr32.18;
%claims_pct(measure_id=ffs26_6,  claim_cat=C, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);
%claims_pct(measure_id=ffs26_14, claim_cat=I, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr32_7,  claim_cat=Y, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr32_8,  claim_cat=O, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr32_17, claim_cat=Z, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr32_18, claim_cat=U, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')  or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=ot);

* ffs26.7, ffs26.15, mcr32.9, mcr32.19;
%claims_pct(measure_id=ffs26_7,  claim_cat=C, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=rx);
%claims_pct(measure_id=ffs26_15, claim_cat=I, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr32_9,  claim_cat=O, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr32_19, claim_cat=U, denom=%str(1=1), numer=%quote(ADJSTMT_IND not in ('0','1','4','5','6')        or ADJSTMT_IND is NULL),      level=CLH, claim_type=rx);

* ffs26.8, ffs26.16, mcr32.10, mcr32.20;
%claims_pct(measure_id=ffs26_8,   claim_cat=C, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=rx);
%claims_pct(measure_id=ffs26_16,  claim_cat=I, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=rx);
%claims_pct(measure_id=mcr32_10,  claim_cat=O, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=rx);
%claims_pct(measure_id=mcr32_20,  claim_cat=U, denom=%str(1=1), numer=%quote(LINE_ADJSTMT_IND not in ('0','1','4','5','6')   or LINE_ADJSTMT_IND is NULL), level=CLL, claim_type=rx);

* ffs47.1, ffs48.1, mcr56.1, mcr57.1;          
%claims_pct(measure_id=ffs47_1,  claim_cat=C, denom=%str(PTNT_STUS_CD<>'30' or PTNT_STUS_CD is NULL),  numer=%quote(DSCHRG_DT is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=ffs48_1,  claim_cat=I, denom=%str(PTNT_STUS_CD<>'30' or PTNT_STUS_CD is NULL),  numer=%quote(DSCHRG_DT is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr56_1,  claim_cat=O, denom=%str(PTNT_STUS_CD<>'30' or PTNT_STUS_CD is NULL),  numer=%quote(DSCHRG_DT is NULL),      level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr57_1,  claim_cat=U, denom=%str(PTNT_STUS_CD<>'30' or PTNT_STUS_CD is NULL),  numer=%quote(DSCHRG_DT is NULL),      level=CLH, claim_type=ip);          
          

* ffs-49.5, mcr-59.5;          
%claims_pct(measure_id=ffs49_5,  claim_cat=AE, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr59_5,  claim_cat=AF, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=ip);

* ffs-49.6, mcr-59.6;
%claims_pct(measure_id=ffs49_6,  claim_cat=AE, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=lt);
%claims_pct(measure_id=mcr59_6,  claim_cat=AF, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=lt);


* ffs-49.7, mcr-59.7;
%claims_pct(measure_id=ffs49_7,  claim_cat=AE, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=ot);
%claims_pct(measure_id=mcr59_7,  claim_cat=AF, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=ot);


* ffs-49.8, mcr-59.8;
%claims_pct(measure_id=ffs49_8,  claim_cat=AE, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=rx);
%claims_pct(measure_id=mcr59_8,  claim_cat=AF, denom=%str(TOT_MDCD_PD_AMT is not NULL and TOT_ALOWD_AMT is not NULL and TOT_ALOWD_AMT <> 0),  numer=%quote(TOT_MDCD_PD_AMT > TOT_ALOWD_AMT),      level=CLH, claim_type=rx);


*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------;
                                           
                                                                                                                                                                                                                                                           
* ffs-14.4, ffs-16.4, mcr-17.4, mcr-19.4;
%claims_pct_cll_to_clh(measure_id=ffs14_4,  claim_cat=A, denom_line=%str(1=1), numer_line=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=ffs16_4,  claim_cat=F, denom_line=%str(1=1), numer_line=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr17_4,  claim_cat=P, denom_line=%str(1=1), numer_line=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr19_4,  claim_cat=R, denom_line=%str(1=1), numer_line=%str(OTHR_TOC_RX_CLM_ACTL_QTY = 1), claim_type=rx);


* ffs-1.24, ffs-3.12, mcr-1.12, mcr-3.12;
%claims_pct_cll_to_clh(measure_id=ffs1_24, claim_cat=A, denom_line=%str(1=1), numer_line=%str(SUBSTRING(REV_CD,1,2)='01'      or  SUBSTRING(REV_CD,1,3) in ('020','021')),  claim_type=ip);
%claims_pct_cll_to_clh(measure_id=ffs3_12, claim_cat=F, denom_line=%str(1=1), numer_line=%str(SUBSTRING(REV_CD,1,2)='01'      or  SUBSTRING(REV_CD,1,3) in ('020','021')),  claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr1_12, claim_cat=P, denom_line=%str(1=1), numer_line=%str(SUBSTRING(REV_CD,1,2)='01'      or  SUBSTRING(REV_CD,1,3) in ('020','021')),  claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr3_12, claim_cat=R, denom_line=%str(1=1), numer_line=%str(SUBSTRING(REV_CD,1,2)='01'      or  SUBSTRING(REV_CD,1,3) in ('020','021')),  claim_type=ip);

* ffs-1.25, ffs-3.13, mcr-1.13, mcr-3.13;       
%claims_pct_cll_to_clh(measure_id=ffs1_25, claim_cat=A, denom_line=%str(1=1), numer_line=%str(REV_CD between '0220' and '0998'), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=ffs3_13, claim_cat=F, denom_line=%str(1=1), numer_line=%str(REV_CD between '0220' and '0998'), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr1_13, claim_cat=P, denom_line=%str(1=1), numer_line=%str(REV_CD between '0220' and '0998'), claim_type=ip);
%claims_pct_cll_to_clh(measure_id=mcr3_13, claim_cat=R, denom_line=%str(1=1), numer_line=%str(REV_CD between '0220' and '0998'), claim_type=ip);

* ffs14.1, ffs16.1, mcr17.1, mcr19.1;
%claims_pct_cll_to_clh(measure_id=ffs14_1,  claim_cat=A, denom_line=%str(1=1), numer_line=%quote(%is_missing_2(SUPLY_DAYS_CNT, 5)),               claim_type=rx);
%claims_pct_cll_to_clh(measure_id=ffs16_1,  claim_cat=F, denom_line=%str(1=1), numer_line=%quote(%is_missing_2(SUPLY_DAYS_CNT, 5)),               claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr17_1,  claim_cat=P, denom_line=%str(1=1), numer_line=%quote(%is_missing_2(SUPLY_DAYS_CNT, 5)),               claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr19_1,  claim_cat=R, denom_line=%str(1=1), numer_line=%quote(%is_missing_2(SUPLY_DAYS_CNT, 5)),               claim_type=rx);

* ffs14.2, ffs16.2, mcr17.2, mcr19.2;
%claims_pct_cll_to_clh(measure_id=ffs14_2,  claim_cat=A, denom_line=%str(1=1), numer_line=%quote(%is_missing_3(OTHR_TOC_RX_CLM_ACTL_QTY, 9, 3)),     claim_type=rx);
%claims_pct_cll_to_clh(measure_id=ffs16_2,  claim_cat=F, denom_line=%str(1=1), numer_line=%quote(%is_missing_3(OTHR_TOC_RX_CLM_ACTL_QTY, 9, 3)),     claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr17_2,  claim_cat=P, denom_line=%str(1=1), numer_line=%quote(%is_missing_3(OTHR_TOC_RX_CLM_ACTL_QTY, 9, 3)),     claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr19_2,  claim_cat=R, denom_line=%str(1=1), numer_line=%quote(%is_missing_3(OTHR_TOC_RX_CLM_ACTL_QTY, 9, 3)),     claim_type=rx);

* ffs14.3, ffs16.3, mcr17.3, mcr19.3;
%claims_pct_cll_to_clh(measure_id=ffs14_3,  claim_cat=A, denom_line=%str(1=1), numer_line=%str(NDC_CD rlike '^\\d{11}$'), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=ffs16_3,  claim_cat=F, denom_line=%str(1=1), numer_line=%str(NDC_CD rlike '^\\d{11}$'), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr17_3,  claim_cat=P, denom_line=%str(1=1), numer_line=%str(NDC_CD rlike '^\\d{11}$'), claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr19_3,  claim_cat=R, denom_line=%str(1=1), numer_line=%str(NDC_CD rlike '^\\d{11}$'), claim_type=rx);

* ffs14.5, ffs16.5, mcr17.5, mcr19.5;
%claims_pct_cll_to_clh(measure_id=ffs14_5,  claim_cat=A, denom_line=%str(1=1), numer_line=%str(SUPLY_DAYS_CNT > 30),                  claim_type=rx);
%claims_pct_cll_to_clh(measure_id=ffs16_5,  claim_cat=F, denom_line=%str(1=1), numer_line=%str(SUPLY_DAYS_CNT > 30),                  claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr17_5,  claim_cat=P, denom_line=%str(1=1), numer_line=%str(SUPLY_DAYS_CNT > 30),                  claim_type=rx);
%claims_pct_cll_to_clh(measure_id=mcr19_5,  claim_cat=R, denom_line=%str(1=1), numer_line=%str(SUPLY_DAYS_CNT > 30),                  claim_type=rx)

* ffs-5.1, ffs-7.1, mcr-5.1, mcr-7.1; 
%claims_pct_cll_to_clh(measure_id=ffs5_1,  claim_cat=A, denom_line=%str(stc_cd='009'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_1,  claim_cat=P, denom_line=%str(stc_cd='009'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_1,  claim_cat=R, denom_line=%str(stc_cd='009'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);

* ffs-5.3, ffs-7.3, mcr-5.3, mcr-7.3;
%claims_pct_cll_to_clh(measure_id=ffs5_3,  claim_cat=A, denom_line=%str(stc_cd='045'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_3,  claim_cat=P, denom_line=%str(stc_cd='045'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_3,  claim_cat=R, denom_line=%str(stc_cd='045'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);

* ffs-5.5, ffs-7.5, mcr-5.5, mcr-7.5;
%claims_pct_cll_to_clh(measure_id=ffs5_5,  claim_cat=A, denom_line=%str(stc_cd='047'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_5,  claim_cat=P, denom_line=%str(stc_cd='047'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_5,  claim_cat=R, denom_line=%str(stc_cd='047'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);

* ffs-5.8, ffs-7.8, mcr-5.8, mcr-7.8;
%claims_pct_cll_to_clh(measure_id=ffs5_8,  claim_cat=A, denom_line=%str(stc_cd='059'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_8,  claim_cat=P, denom_line=%str(stc_cd='059'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_8,  claim_cat=R, denom_line=%str(stc_cd='059'), numer_line=%quote(%is_missing_2(nrsng_fac_days_cnt, 5)),       claim_type=lt);

* ffs-5.2, ffs-7.2, mcr-5.2, mcr-7.2;
%claims_pct_cll_to_clh(measure_id=ffs5_2,  claim_cat=A, denom_line=%str(stc_cd='044'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_2,  claim_cat=P, denom_line=%str(stc_cd='044'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_2,  claim_cat=R, denom_line=%str(stc_cd='044'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);

* ffs-5.6, ffs-7.6, mcr-5.6, mcr-7.6;
%claims_pct_cll_to_clh(measure_id=ffs5_6,  claim_cat=A, denom_line=%str(stc_cd='048'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_6,  claim_cat=P, denom_line=%str(stc_cd='048'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_6,  claim_cat=R, denom_line=%str(stc_cd='048'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);

* ffs-5.7, ffs-7.7, mcr-5.7, mcr-7.7;
%claims_pct_cll_to_clh(measure_id=ffs5_7,  claim_cat=A, denom_line=%str(stc_cd='050'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_7,  claim_cat=P, denom_line=%str(stc_cd='050'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_7,  claim_cat=R, denom_line=%str(stc_cd='050'), numer_line=%quote(%is_missing_2(mdcd_cvrd_ip_days_cnt, 5)), claim_type=lt);
* ffs-5.4, ffs-7.4, mcr-5.4, mcr-7.4;
%claims_pct_cll_to_clh(measure_id=ffs5_4,  claim_cat=A, denom_line=%str(stc_cd='046'), numer_line=%quote(%is_missing_2(ICF_IID_DAYS_CNT,5)),           claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr5_4,  claim_cat=P, denom_line=%str(stc_cd='046'), numer_line=%quote(%is_missing_2(ICF_IID_DAYS_CNT,5)),           claim_type=lt);
%claims_pct_cll_to_clh(measure_id=mcr7_4,  claim_cat=R, denom_line=%str(stc_cd='046'), numer_line=%quote(%is_missing_2(ICF_IID_DAYS_CNT,5)),           claim_type=lt);


%claims_pct(measure_id=mcr62_1,  claim_cat=al, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,2) not in ('27','28')),      
            level=clh, claim_type=ip);
%claims_pct(measure_id=ffs52_1,  claim_cat=ak, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,2) not in ('27','28')),      
            level=clh, claim_type=ip);
%claims_pct(measure_id=mcr62_2,  claim_cat=al, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,4) not in ('283Q', '283X', '282E', '385H', '281P') 
                  and substring(blg_prvdr_txnmy_cd,1,2) not in ('31', '32')),
            level=clh, claim_type=lt);
%claims_pct(measure_id=ffs52_2,  claim_cat=ak, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,4) not in ('283Q', '283X', '282E', '385H', '281P') 
                  and substring(blg_prvdr_txnmy_cd,1,2) not in ('31', '32')),
            level=clh, claim_type=lt);
%claims_pct(measure_id=mcr62_3,  claim_cat=al, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,2) not in ('18','33')), 
            level=clh, claim_type=rx);
%claims_pct(measure_id=ffs52_3,  claim_cat=ak, 
            denom=%quote(%not_missing_1(blg_prvdr_txnmy_cd,12)),  
            numer=%quote(substring(blg_prvdr_txnmy_cd,1,2) not in ('18','33')), 
            level=clh, claim_type=rx);
%claims_pct(measure_id=mcr62_4,  claim_cat=al, 
            denom=%quote(%not_missing_1(rev_cd,4)),  
            numer=%quote(substring(rev_cd,1,4) >= '0100' and substring(rev_cd,1,4) <= '0219' and  rev_cd rlike '^\\d{4}$'), 
            level=cll, claim_type=ot);
%claims_pct(measure_id=ffs52_4,  claim_cat=ak, 
            denom=%quote(%not_missing_1(rev_cd,4)),  
            numer=%quote(substring(rev_cd,1,4) >= '0100' and substring(rev_cd,1,4) <= '0219' and  rev_cd rlike '^\\d{4}$'), 
            level=cll, claim_type=ot);
%claims_pct(measure_id=mcr62_5,  claim_cat=al, 
            denom=%quote(%not_missing_1(bill_type_cd,4)),  
            numer=%quote(not substring(bill_type_cd,1,3) like '011'),      
            level=clh, claim_type=ip);
%claims_pct(measure_id=ffs52_5,  claim_cat=ak, 
            denom=%quote(%not_missing_1(bill_type_cd,4)),  
            numer=%quote(not substring(bill_type_cd,1,3) like '011'),      
            level=clh, claim_type=ip);
%claims_pct(measure_id=mcr62_6,  claim_cat=al, 
            denom=%quote(%not_missing_1(bill_type_cd,4)),  
            numer=%quote(not substring(bill_type_cd,1,2) like '02' and not substring(bill_type_cd,1,2) like '06'),
            level=clh, claim_type=lt);
%claims_pct(measure_id=ffs52_6,  claim_cat=ak, 
            denom=%quote(%not_missing_1(bill_type_cd,4)),  
            numer=%quote(not substring(bill_type_cd,1,2) like '02' and not substring(bill_type_cd,1,2) like '06'),
            level=clh, claim_type=lt);
%claims_pct(measure_id=ffs53_1,  claim_cat=ao, 
            denom=%str(1=1),  
            numer=%quote(mdcr_pd_amt rlike '[1-9]' or 
                   tot_mdcr_coinsrnc_amt rlike '[1-9]' or
             tot_mdcr_ddctbl_amt rlike '[1-9]'
                         ),
            level=CLH, claim_type=ip);
%claims_pct(measure_id=mcr63_1,  claim_cat=ap, 
            denom=%str(1=1),  
            numer=%quote(mdcr_pd_amt rlike '[1-9]' or 
                   tot_mdcr_coinsrnc_amt rlike '[1-9]' or
             tot_mdcr_ddctbl_amt rlike '[1-9]'
                         ),
            level=clh, claim_type=ip);
%claims_pct(measure_id=ffs53_2,  claim_cat=ao, 
            denom=%str(1=1),  
            numer=%quote(mdcr_pd_amt rlike '[1-9]' or 
                   tot_mdcr_coinsrnc_amt rlike '[1-9]' or
             tot_mdcr_ddctbl_amt rlike '[1-9]'
                         ),
            level=clh, claim_type=lt);
%claims_pct(measure_id=mcr63_2,  claim_cat=ap, 
            denom=%str(1=1),  
            numer=%quote(mdcr_pd_amt rlike '[1-9]' or 
                   tot_mdcr_coinsrnc_amt rlike '[1-9]' or
             tot_mdcr_ddctbl_amt rlike '[1-9]'
                         ),
            level=clh, claim_type=lt);


*------------------------------------------------------------ MCR ONLY MEASURES ------------------------------------------------------------------------------------;

* mcr 9.4 and mcr 13.4;
%claims_pct(measure_id=mcr9_4,  claim_cat=D, denom=%str(1=1), numer=%quote(%not_missing_1(plan_id_num,12)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr13_4, claim_cat=J, denom=%str(1=1), numer=%quote(%not_missing_1(plan_id_num,12)), level=CLL, claim_type=ot);

* mcr-10.4; 
%claims_pct(measure_id=mcr10_4, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote((PRCDR_CD_IND = '01') and 
                        (%not_missing_1(PRCDR_CD,8)) and ((PRCDR_CD rlike '[0-9]{5}') or (PRCDR_CD rlike '[0-9]{4}([A-Za-z])'))), level=CLL, claim_type=ot);

* mcr10.11, mcr10.12, mcr10.13, mcr10.14, mcr10.15;
%claims_pct(measure_id=mcr10_11, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote(PRCDR_CD_IND = '06' and (%not_missing_1(PRCDR_CD,8)) and PRCDR_CD rlike '[A-V][A-Z][0-9]{3}'),      level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_12, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote(PRCDR_CD_IND = '06' and (%not_missing_1(PRCDR_CD,8)) and PRCDR_CD rlike '[A-V][0-9]{4}'),           level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_13, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote(PRCDR_CD_IND = '06' and (%not_missing_1(PRCDR_CD,8)) and ((NOT SUBSTRING(PRCDR_CD,1,1) rlike '[A-Z]|[a-z]') OR (NOT SUBSTRING(PRCDR_CD,2,1) rlike '[A-Z]|[a-z]|[0-9]') OR (NOT SUBSTRING(PRCDR_CD,3,3) rlike '[0-9]{3}') OR (length(PRCDR_CD) <> 5))), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_14, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote(PRCDR_CD_IND = '06' and (%not_missing_1(PRCDR_CD,8)) and PRCDR_CD rlike '[W-Z][A-Z][0-9]{3}'),      level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr10_15, claim_cat=P, denom=%str(STC_CD IN ('012','025','026')), numer=%quote(PRCDR_CD_IND = '06' and (%not_missing_1(PRCDR_CD,8)) and PRCDR_CD rlike '[W-Z][0-9]{4}'),           level=CLL, claim_type=ot);

* mcr-21.3, mcr-24.3;                  
%claims_pct(measure_id=mcr21_3, claim_cat=P, denom=%str(1=1), numer=%quote(%not_missing_1(BLG_PRVDR_NUM,30)), level=CLL, claim_type=ot);
%claims_pct(measure_id=mcr24_3, claim_cat=R, denom=%str(1=1), numer=%quote(%not_missing_1(BLG_PRVDR_NUM,30)), level=CLL, claim_type=ot);     



/*********************************************************************************************************************************************************************/

/*extract measure from AREMAC into sas*/
create table batch_900a as
select * from connection to tmsis_passthrough
(select * from &wrktable..&taskprefix._clms_900a);

%dropwrktables(clms_900a);

%mend run_901_all_claims_pct;
