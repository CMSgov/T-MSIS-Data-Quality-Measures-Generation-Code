/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
Program: 202_exp_avg_macro.sas
Project: MACBIS Task 2
Purpose: Defines and calls average macros for module 200 (expenditures)
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
             -confirmed that the following measures did not need programming updates to match specs:
                EXP16.4, EXP22.1, EXP18.4, EXP24.1
             -updated EXP11.5
             -added adjstmt_ind to group by statement for avg_cll_to_clh_rollup macro, to account for new
              definition of unique claim
              
******************************************************************************************/

/*Macro to calculate the average of a variable directly from the header level or the line level*/  
%macro avg(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  constraint=, /*Logical constraints to apply to the denominator, as defined in specs. Often set to %wtr(1=1), which will not apply any constraints to denom*/
  avgvar=,     /*variable on which to take the average*/
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );
  
  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'202' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
      from (
    
      /*inner query first step: flags each line or header (depending on cll/clh parameter) as meeting criteria for inclusion in the average                               
        
        inner query second step: sum() across all lines/headers to create numer and denom values. 
                                 we are calculating an average, so the denominator is the number of lines,
                                    and the numerator is the sum of the variable we want to eventually average.                                    
                                 returns a single observation to outer query.
        
        outer query does the final step of dividing numer (sum) by denom (n) to create an average*/      
    
	    select %str(%')&measure_id.%str(%') as measure_id
	           ,sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then 1        else 0 end) as denom
	           ,sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then &avgvar. else 0 end) as numer
	    from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	  	  where childless_header_flag = 0
	    %end;
	  ) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");
  
%mend avg;

/*macro to calculate the average of a variable at the header level, but only among headers
  with it least one line that meets a certain criteria. thus, unlike other macro,
  requires first identifying qualifying headers at the line level, then rolling up
  to the header level among the qualifying set, then taking the average*/

%macro avg_cll_to_clh(
  measure_id=,        /*measure id of the measure you want to create. eg: exp1.1*/
  claim_cat=,         /*which claim category the measure should use. (a,b,c, etc.). defined in specs. */
  line_constraint=,   /*logical constraints to apply to the denominator at the line level. used to identify headers with at least one line that meets the constraint*/
  clm_avgvar=,        /*header-level variable that we are going to take the average of*/
  claim_type=         /*which claim type. ip, lt, rx, ot*/
  );
  
  /*step 1: roll up from line level to header level.
            cannot just use the pre-created header level file
            because the constraints require us to check to
            see if the claim has at least one line that
            meets certain contraints

            ok to take the max of the claim_cat binaries and the
            variable that we are going to average because these
            values do not vary within claim. (could similarly take
            min or avg if we wanted to and would get same result).  */
            
 
  execute(
    create or replace temporary view &taskprefix._clh as

	  select tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind
           ,max(claim_cat_&claim_cat.) as claim_cat_&claim_cat.
	         ,max(case when (&line_constraint.) then 1 else 0 end) as meets_line_constraint
	         ,max(&clm_avgvar.) as &clm_avgvar.

    from &temptable..&taskprefix._base_cll_&claim_type.

	  group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;

  
  /*step 2: take average at the header level among claims
            that meet all denominator criteria
            
            inner/outer queries function similar to avg macro
  */
  
  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'202' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
     from (
	 	select
	           sum(case when (meets_line_constraint=1) and (claim_cat_&claim_cat. = 1) then 1            else 0 end) as denom
	          ,sum(case when (meets_line_constraint=1) and (claim_cat_&claim_cat. = 1) then &clm_avgvar. else 0 end) as numer
	    from #temp.&taskprefix._clh
	  ) a
	
  )by tmsis_passthrough;
  
  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");
 

%mend avg_cll_to_clh;

  
/******************************************************************************************
  macro that contains all the calls to create each measure
  
  this macro gets called in the module driver
 ******************************************************************************************/
 
%macro run_202_all_avg;
  %avg(measure_id=exp1_4,  claim_type=ip, level=clh,  avgvar=tot_mdcd_pd_amt, constraint=%str(tot_mdcd_pd_amt<2000000),                                                     claim_cat=a);
  
  /*v1.2: updated 11_5*/
  %avg(measure_id=exp11_5, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt, 
			constraint=%quote(%not_missing_1(hcpcs_srvc_cd,1) and 0<mdcd_pd_amt and mdcd_pd_amt< 200000), claim_cat=a);

  %avg(measure_id=exp16_4, claim_type=rx, level=clh,  avgvar=tot_mdcd_pd_amt, constraint=%str(tot_mdcd_pd_amt < 300000),                                                    claim_cat=a);

  %avg(measure_id=exp11_29, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '003'), claim_cat=a);

  %avg(measure_id=exp12_9,  claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '013'), claim_cat=b);
  %avg(measure_id=exp12_10, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '014'), claim_cat=b);
  %avg(measure_id=exp12_11, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '015'), claim_cat=b);
  %avg(measure_id=exp12_12, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '016'), claim_cat=b);
  %avg(measure_id=exp12_13, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '017'), claim_cat=b);
  %avg(measure_id=exp12_14, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '018'), claim_cat=b);
  %avg(measure_id=exp12_15, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '019'), claim_cat=b);
  %avg(measure_id=exp12_16, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '002'), claim_cat=b);
  %avg(measure_id=exp12_17, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '020'), claim_cat=b);

  %avg(measure_id=exp12_18, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '021'), claim_cat=b);
  %avg(measure_id=exp12_19, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '022'), claim_cat=b);
  %avg(measure_id=exp12_20, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '023'), claim_cat=b);
  %avg(measure_id=exp12_21, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '024'), claim_cat=b);
  %avg(measure_id=exp12_22, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '025'), claim_cat=b);
  %avg(measure_id=exp12_23, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '026'), claim_cat=b);
  %avg(measure_id=exp12_24, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '027'), claim_cat=b);
  %avg(measure_id=exp12_25, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '028'), claim_cat=b);
  %avg(measure_id=exp12_26, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '029'), claim_cat=b);
  %avg(measure_id=exp12_27, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '003'), claim_cat=b);

  %avg(measure_id=exp12_28, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '030'), claim_cat=b);
  %avg(measure_id=exp12_29, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '031'), claim_cat=b);
  %avg(measure_id=exp12_30, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '032'), claim_cat=b);
  %avg(measure_id=exp12_31, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '035'), claim_cat=b);
  %avg(measure_id=exp12_32, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '036'), claim_cat=b);
  %avg(measure_id=exp12_33, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '037'), claim_cat=b);
  %avg(measure_id=exp12_34, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '038'), claim_cat=b);
  %avg(measure_id=exp12_35, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '039'), claim_cat=b);
  %avg(measure_id=exp12_36, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '004'), claim_cat=b);
  %avg(measure_id=exp12_37, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '040'), claim_cat=b);

  %avg(measure_id=exp12_38, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '041'), claim_cat=b);
  %avg(measure_id=exp12_39, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '042'), claim_cat=b);
  %avg(measure_id=exp12_40, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '043'), claim_cat=b);
  %avg(measure_id=exp12_41, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '049'), claim_cat=b);
  %avg(measure_id=exp12_42, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '005'), claim_cat=b);
  %avg(measure_id=exp12_43, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '050'), claim_cat=b);
  %avg(measure_id=exp12_44, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '051'), claim_cat=b);
  %avg(measure_id=exp12_45, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '052'), claim_cat=b);
  %avg(measure_id=exp12_46, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '053'), claim_cat=b);
  %avg(measure_id=exp12_47, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '054'), claim_cat=b);

  %avg(measure_id=exp12_48, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '055'), claim_cat=b);
  %avg(measure_id=exp12_49, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '056'), claim_cat=b);
  %avg(measure_id=exp12_50, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '057'), claim_cat=b);
  %avg(measure_id=exp12_51, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '006'), claim_cat=b);
  %avg(measure_id=exp12_52, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '061'), claim_cat=b);
  %avg(measure_id=exp12_53, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '062'), claim_cat=b);
  %avg(measure_id=exp12_54, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '063'), claim_cat=b);
  %avg(measure_id=exp12_55, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '064'), claim_cat=b);
  %avg(measure_id=exp12_56, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '065'), claim_cat=b);
  %avg(measure_id=exp12_57, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '066'), claim_cat=b);

  %avg(measure_id=exp12_58, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '067'), claim_cat=b);
  %avg(measure_id=exp12_59, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '068'), claim_cat=b);
  %avg(measure_id=exp12_60, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '069'), claim_cat=b);
  %avg(measure_id=exp12_61, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '007'), claim_cat=b);
  %avg(measure_id=exp12_62, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '070'), claim_cat=b);
  %avg(measure_id=exp12_63, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '071'), claim_cat=b);
  %avg(measure_id=exp12_64, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '072'), claim_cat=b);
  %avg(measure_id=exp12_65, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '073'), claim_cat=b);
  %avg(measure_id=exp12_66, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '074'), claim_cat=b);
  %avg(measure_id=exp12_67, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '075'), claim_cat=b);

  %avg(measure_id=exp12_68, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '076'), claim_cat=b);
  %avg(measure_id=exp12_69, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '077'), claim_cat=b);
  %avg(measure_id=exp12_70, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '078'), claim_cat=b);
  %avg(measure_id=exp12_71, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '079'), claim_cat=b);
  %avg(measure_id=exp12_72, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '008'), claim_cat=b);
  %avg(measure_id=exp12_73, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '080'), claim_cat=b);
  %avg(measure_id=exp12_74, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '081'), claim_cat=b);
  %avg(measure_id=exp12_75, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '082'), claim_cat=b);
  %avg(measure_id=exp12_76, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '083'), claim_cat=b);
  %avg(measure_id=exp12_77, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '085'), claim_cat=b);

  %avg(measure_id=exp12_78, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '087'), claim_cat=b);
  %avg(measure_id=exp12_79, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '088'), claim_cat=b);
  %avg(measure_id=exp12_80, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '089'), claim_cat=b);

  %avg(measure_id=exp27_2, claim_type=ot, level=clh,  avgvar=tot_mdcd_pd_amt,     constraint=%str(tot_mdcd_pd_amt>0 and tot_mdcd_pd_amt<200000), claim_cat=b);

  %avg(measure_id=exp22_1, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(1=1), claim_cat=d);
  %avg(measure_id=exp22_3, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '119'), claim_cat=d);
  %avg(measure_id=exp20_2, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '121'), claim_cat=d);
  %avg(measure_id=exp22_5, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '120'), claim_cat=d);
  %avg(measure_id=exp22_7, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '122'), claim_cat=d);

  %avg(measure_id=exp3_4,  claim_type=ip, level=clh,  avgvar=tot_mdcd_pd_amt, constraint=%str(tot_mdcd_pd_amt<2000000),   claim_cat=f);
  %avg(measure_id=exp18_4, claim_type=rx, level=clh,  avgvar=tot_mdcd_pd_amt, constraint=%str(tot_mdcd_pd_amt < 300000),  claim_cat=f);

  %avg_cll_to_clh(measure_id=exp10_10, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '044'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_11, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '045'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_12, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '046'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_13, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '047'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_14, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '048'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_15, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '050'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_16, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '059'),    claim_cat=h);
  %avg_cll_to_clh(measure_id=exp10_17, claim_type=lt, clm_avgvar=tot_mdcd_pd_amt, line_constraint=%str(stc_cd = '009'),    claim_cat=h);
   
  %avg(measure_id=exp24_1, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(1=1),            claim_cat=j);
  %avg(measure_id=exp24_3, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '119'), claim_cat=j);
  %avg(measure_id=exp21_2, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '121'), claim_cat=j);
  %avg(measure_id=exp24_5, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '120'), claim_cat=j);
  %avg(measure_id=exp24_7, claim_type=ot, level=cll,  avgvar=mdcd_pd_amt,     constraint=%str(stc_cd = '122'), claim_cat=j);
%mend run_202_all_avg; 


/******************************************************************************************
  macro that contains list of all measures created in this module. will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_202 =
exp1_4	
exp11_5	
exp16_4	
exp11_29
exp12_9	
exp12_10
exp12_11
exp12_12
exp12_13
exp12_14
exp12_15
exp12_16
exp12_17
exp12_18
exp12_19
exp12_20
exp12_21
exp12_22
exp12_23
exp12_24
exp12_25
exp12_26
exp12_27
exp12_28
exp12_29
exp12_30
exp12_31
exp12_32
exp12_33
exp12_34
exp12_35
exp12_36
exp12_37
exp12_38
exp12_39
exp12_40
exp12_41
exp12_42
exp12_43
exp12_44
exp12_45
exp12_46
exp12_47
exp12_48
exp12_49
exp12_50
exp12_51
exp12_52
exp12_53
exp12_54
exp12_55
exp12_56
exp12_57
exp12_58
exp12_59
exp12_60
exp12_61
exp12_62
exp12_63
exp12_64
exp12_65
exp12_66
exp12_67
exp12_68
exp12_69
exp12_70
exp12_71
exp12_72
exp12_73
exp12_74
exp12_75
exp12_76
exp12_77
exp12_78
exp12_79
exp12_80
exp27_2	
exp22_1	
exp22_3	
exp20_2	
exp22_5	
exp22_7	
exp3_4	
exp18_4	
exp24_1	
exp24_3	
exp21_2	
exp24_5	
exp24_7	
;
