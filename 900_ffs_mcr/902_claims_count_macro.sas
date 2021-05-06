/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

 /******************************************************************************************
 Program: 902_claims_count_macro.sas 
 Project: MACBIS Task 2
 Purpose: Defines and calls count macros for module 900 (ffs and managed care)
          Designed to be %included in module level driver         
          
 
 Author:  Richard Chapman
 Date Created: 3/1/2017
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call calculates a single measure and inserts that measure into an AREMAC table.
         Most are a single observation, except for frequency measures or other measures that have
		 one observation per oberved value. The AREMAC table is only extracted at the end of the 
		  last submodule in the 900 series.
 
 Modifications:
 ******************************************************************************************/
 
/******************************************************************************************
   Macro: Counts
   
   Can be run on either measures that are strictly line level or strictly header level
******************************************************************************************/
 
%macro countt(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  constraint=, /*Logical constraints that restrict which observations get included in the count*/ 
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );

  execute(
    insert into &wrktable..&taskprefix._clms_900b
    select 
		%str(%')&state.%str(%') as submtg_state_cd, 
		%str(%')&measure_id.%str(%') as measure_id,
		'902' as submodule,
		null as numer,
		null as denom,
        coalesce(numer,0) as mvalue,
		null as valid_value
    from (
      select  
		 sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as numer
      from &temptable..&taskprefix._base_&level._&claim_type.
    %if %lowcase(&level.) = cll %then %do;
        where childless_header_flag = 0
      %end;
  ) a
  )by tmsis_passthrough; 

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_900 
  values("&measure_id.",null);
  

%mend countt;

/******************************************************************************************
  Macro containing all calls to create measures. This macro gets run in the module driver
 ******************************************************************************************/

%macro run_902_all_countt;

  %dropwrktables(clms_900b);
  execute(
	  create table &wrktable..&taskprefix._clms_900b (

	     submtg_state_cd STRING
	    ,measure_id STRING
		,submodule STRING
	    ,numer DOUBLE
	    ,denom DOUBLE
	    ,mvalue DOUBLE
		,valid_value STRING
	)
  
  )by tmsis_passthrough;

*ffs10.85, ffs11.24, ffs9.103, mcr10.24, mcr14.24;
%countt(measure_id=ffs9_103, claim_cat=A, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=ffs10_85, claim_cat=B, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=ffs11_24, claim_cat=F, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=mcr10_24, claim_cat=P, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=mcr14_24, claim_cat=R, constraint=%str(1=1), level=CLL, claim_type=OT);

* ffs12.5, mcr11.6, mcr15.5;
%countt(measure_id=ffs12_5,  claim_cat=G, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=mcr11_6,  claim_cat=T, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=mcr15_5,  claim_cat=V, constraint=%str(1=1), level=CLL, claim_type=OT);

*sumffs.20, sumffs.9, summcr.9;
%countt(measure_id=sumffs_9,  claim_cat=C, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=sumffs_20, claim_cat=I, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=summcr_9,  claim_cat=O, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=summcr_20, claim_cat=U, constraint=%str(1=1), level=CLL, claim_type=OT);

*ffs1.30, ffs2.13, ffs3.18, mcr1.18, mcr3.18;
%countt(measure_id=ffs1_30,   claim_cat=A, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=ffs2_13,   claim_cat=B, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=ffs3_18,   claim_cat=F, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=mcr1_18,   claim_cat=P, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=mcr3_18,   claim_cat=R, constraint=%str(1=1), level=CLH, claim_type=IP);

* ffs5.30, ffs6.10, ffs7.20, mcr5.21, mcr7.20;
%countt(measure_id=ffs5_30,   claim_cat=A, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=ffs6_10,   claim_cat=B, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=ffs7_20,   claim_cat=F, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=mcr5_21,   claim_cat=P, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=mcr7_20,   claim_cat=R, constraint=%str(1=1), level=CLH, claim_type=LT);

* ffs14.15, ffs16.8, mcr17.8. mcr19.8;
%countt(measure_id=ffs14_15,  claim_cat=A, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=ffs16_8,   claim_cat=F, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=mcr17_8,   claim_cat=P, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=mcr19_8,   claim_cat=R, constraint=%str(1=1), level=CLH, claim_type=RX);

* ffs4.13, mcr2.25, mcr4.13;
%countt(measure_id=ffs4_13,   claim_cat=G, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=mcr2_25,   claim_cat=T, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=mcr4_13,   claim_cat=V, constraint=%str(1=1), level=CLH, claim_type=IP);

* ffs8.10, mcr6.26, mcr8.9;
%countt(measure_id=ffs8_10,   claim_cat=G, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=mcr6_26,   claim_cat=T, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=mcr8_9,    claim_cat=V, constraint=%str(1=1), level=CLH, claim_type=LT);

* sumffs.14, sumffs.3, summcr.14, summcr.3;
%countt(measure_id=sumffs_3,  claim_cat=C, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=sumffs_14, claim_cat=I, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=summcr_3,  claim_cat=O, constraint=%str(1=1), level=CLH, claim_type=IP);
%countt(measure_id=summcr_14, claim_cat=U, constraint=%str(1=1), level=CLH, claim_type=IP);

* sumffs.17, sumffs.6, summcr.17, summcr.6;
%countt(measure_id=sumffs_6,  claim_cat=C, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=sumffs_17, claim_cat=I, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=summcr_6,  claim_cat=O, constraint=%str(1=1), level=CLH, claim_type=LT);
%countt(measure_id=summcr_17, claim_cat=U, constraint=%str(1=1), level=CLH, claim_type=LT);

* sumffs.22, sumffs.11, summcr.22, summcr.11;
%countt(measure_id=sumffs_11, claim_cat=C, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=sumffs_22, claim_cat=I, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=summcr_11, claim_cat=O, constraint=%str(1=1), level=CLH, claim_type=RX);
%countt(measure_id=summcr_22, claim_cat=U, constraint=%str(1=1), level=CLH, claim_type=RX);


/************************* mcr ONLY ****************************************************/

%countt(measure_id=mcr9_1,  claim_cat=D, constraint=%str(stc_cd='119'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr9_2,  claim_cat=D, constraint=%str(stc_cd='120'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr9_3,  claim_cat=D, constraint=%str(stc_cd='122'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr9_5,  claim_cat=D, constraint=%str(stc_cd='120' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr9_6,  claim_cat=D, constraint=%str(stc_cd='122' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr9_7,  claim_cat=D, constraint=%str(stc_cd='119' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr9_8,  claim_cat=D, constraint=%str(stc_cd='120' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_9,  claim_cat=D, constraint=%str(stc_cd='122' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_10, claim_cat=D, constraint=%str(stc_cd='119' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_11, claim_cat=D, constraint=%str(stc_cd='120' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_12, claim_cat=D, constraint=%str(stc_cd='122' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_13, claim_cat=D, constraint=%str(stc_cd='119' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr9_14, claim_cat=D, constraint=%str(stc_cd='120' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr9_15, claim_cat=D, constraint=%str(stc_cd='122' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr9_16, claim_cat=D, constraint=%str(stc_cd='119' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr9_17,  claim_cat=D, constraint=%str(1=1), level=CLL, claim_type=OT);

%countt(measure_id=mcr13_1,  claim_cat=J, constraint=%str(stc_cd='119'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr13_2,  claim_cat=J, constraint=%str(stc_cd='120'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr13_3,  claim_cat=J, constraint=%str(stc_cd='122'),                                                                                                        level=CLL, claim_type=OT);
%countt(measure_id=mcr13_5,  claim_cat=J, constraint=%str(stc_cd='120' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr13_6,  claim_cat=J, constraint=%str(stc_cd='122' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr13_7,  claim_cat=J, constraint=%str(stc_cd='119' and SRVC_ENDG_DT <= date_sub(add_months(TMSIS_RPTG_PRD,1),1) and SRVC_ENDG_DT >= TMSIS_RPTG_PRD), level=CLL, claim_type=OT);
%countt(measure_id=mcr13_8,  claim_cat=J, constraint=%str(stc_cd='120' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_9,  claim_cat=J, constraint=%str(stc_cd='122' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_10, claim_cat=J, constraint=%str(stc_cd='119' and SRVC_ENDG_DT > date_sub(add_months(TMSIS_RPTG_PRD,1),1) ),                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_11, claim_cat=J, constraint=%str(stc_cd='120' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_12, claim_cat=J, constraint=%str(stc_cd='122' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_13, claim_cat=J, constraint=%str(stc_cd='119' and SRVC_ENDG_DT < date_sub(TMSIS_RPTG_PRD,30) ),                                                    level=CLL, claim_type=OT);
%countt(measure_id=mcr13_14, claim_cat=J, constraint=%str(stc_cd='120' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr13_15, claim_cat=J, constraint=%str(stc_cd='122' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr13_16, claim_cat=J, constraint=%str(stc_cd='119' and SRVC_ENDG_DT < TMSIS_RPTG_PRD and SRVC_ENDG_DT >= date_sub(TMSIS_RPTG_PRD,30) ),                 level=CLL, claim_type=OT);
%countt(measure_id=mcr13_17,  claim_cat=J, constraint=%str(1=1),            level=CLL, claim_type=OT);

%countt(measure_id=summcr_23, claim_cat=E, constraint=%str(1=1), level=CLL, claim_type=OT);
%countt(measure_id=summcr_24, claim_cat=K, constraint=%str(1=1), level=CLL, claim_type=OT);

%mend run_902_all_countt;

