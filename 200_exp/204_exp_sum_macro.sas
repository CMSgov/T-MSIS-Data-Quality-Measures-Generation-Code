/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
Program: 204_exp_sum_macro.sas     
Project: MACBIS Task 2
urpose: Defines and calls sum macros for module 200 (expenditures)
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
               -Updated EXP1.14, EXP6.21, EXP11.85, EXP12.81, SUMEXP.1, SUMEXP.3, SUMEXP.5,
                        SUMEXP.7, EXP22.8, EXP3.5, EXP8.4, EXP13.5, EXP18.5, SUMEXP.2, 
                        SUMEXP.4, SUMEXP.6, SUMEXP.8, EXP24.8, EXP11.84,
               -added adjstmt_ind to group by statement for cll_to_clh macro, to account for new
                definition of unique claim
******************************************************************************************/

/*Macro to take the sum of a variable across either header level or line level
  
  Still uses a construction of numerator and denominator, even though thats not
  strictly necessary, because it helps to be able to figure out whether a measure
  =0 because no lines qualify vs. because the variable being summed is always =0

*/

%macro summ(
  measure_id=,  /*measure id of the measure you want to create. eg: exp1.1*/
  claim_cat=,   /*which claim category the measure should use. (a,b,c, etc.). defined in specs. */
  constraint=,  /*logical constraints to apply to the denominator. macro will only sum observations where this constraint is true*/
  sumvar=,      /*variable that we are going to take the sum of*/
  level=,       /*whether to apply to header level or line level data. =clh for header level, =cll for line level*/
  claim_type=   /*which claim type. ip, lt, rx, ot*/
  );

  
  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'204' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer else 0 end as mvalue
      
    from (
    
        /*inner query first step: flags each line or header (depending on cll/clh parameter) as meeting criteria for inclusion in the sum                               
        
          inner query second step: sum() across all lines/headers to create numer and denom values. 
                                 we are really just summing the numerator. denominator represents the number of obs we have summed over (the number that met inclusion critera).
                                   this is helpful for debugging, to know how many observations met the criteria.                          
                                 returns a single observation to outer query.
        
          outer query takes the final value of the numerator as the sum*/  
    
	    select  
				sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then 1        else 0 end) as denom
	           ,sum(case when (&constraint.) and (claim_cat_&claim_cat. = 1) then &sumvar. else 0 end) as numer
	    from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	  	   where childless_header_flag = 0
	    %end;
	) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");
 

%mend summ;

%macro summ_cll_to_clh(
  measure_id=,       /*measure id of the measure you want to create. eg: exp1.1*/
  claim_cat=,        /*which claim category the measure should use. (a,b,c, etc.). defined in specs. */
  line_constraint=,  /*logical constraints to apply to the denominator at the line level. used to identify headers with at least one line that meets the constraint*/
  clm_sumvar=,       /*header-level variable that we are going to take the sum of*/
  claim_type=        /*which claim type. ip, lt, rx, ot*/
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
	         ,max(&clm_sumvar.) as &clm_sumvar.

    from &temptable..&taskprefix._base_cll_&claim_type.

	  group by tmsis_run_id, tmsis_rptg_prd, submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind

  )by tmsis_passthrough;


  /*step 2: take sum at the header level among claims
            that meet all denominator criteria*/
            
            
    /*inner query first step: flags header as to whether or not it has a line that meets criteria for inclusion in sum
        
      inner query second step: sum() across all lines/headers to create numer and denom values. 
                                 we are really just summing the numerator. denominator represents the number of obs we have summed over (the number that met inclusion critera).
                                   this is helpful for debugging, to know how many observations met the criteria.                          
                                 returns a single observation to outer query.
        
          outer query takes the final value of the numerator as the sum*/              

  
  execute(
	insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'204' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer else 0 end as mvalue
      
    from (
	  select 
	     sum(case when (meets_line_constraint=1) and (claim_cat_&claim_cat. = 1) then 1            else 0 end) as denom
	    ,sum(case when (meets_line_constraint=1) and (claim_cat_&claim_cat. = 1) then &clm_sumvar. else 0 end) as numer
	  from #temp.&taskprefix._clh
	) a
	
  )by tmsis_passthrough;
  
  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");

%mend summ_cll_to_clh;

%macro run_204_all_summ;
/*v1.2: updated exp1_14*/
%summ(measure_id=exp1_14, claim_cat=a, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=ip);

/*v1.2: updated exp6_21*/
%summ(measure_id=exp6_21, claim_cat=a, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=lt);

/*v1.2: updated 11_84*/
%summ(measure_id=exp11_84,  claim_cat=a, sumvar=mdcd_pd_amt, constraint=%quote(%not_missing_1(hcpcs_srvc_cd,1)), level=cll, claim_type=ot);

/*v1.2: updated 11_85*/
%summ(measure_id=exp11_85,  claim_cat=a, sumvar=mdcd_pd_amt, constraint=%str(1=1), level=cll, claim_type=ot);

%summ(measure_id=exp16_13, claim_cat=a, sumvar=tot_mdcd_pd_amt, constraint=%str(tot_mdcd_pd_amt <> 0), level=clh, claim_type=rx);

/*v1.2 updated exp12_81*/
%summ(measure_id=exp12_81, claim_cat=b, sumvar=mdcd_pd_amt, constraint=%str(1=1), level=cll, claim_type=ot);

/*v1.2: updated sumexp1, 3, 5, 7*/
%summ(measure_id=sumexp_1, claim_cat=c, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=ip);
%summ(measure_id=sumexp_3, claim_cat=c, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=lt);
%summ(measure_id=sumexp_5, claim_cat=c, sumvar=mdcd_pd_amt,     constraint=%str(1=1), level=cll, claim_type=ot);
%summ(measure_id=sumexp_7, claim_cat=c, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=rx);

%summ(measure_id=exp22_2, claim_cat=d, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '119'), level=cll, claim_type=ot);
%summ(measure_id=exp20_1, claim_cat=d, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '121'), level=cll, claim_type=ot);
%summ(measure_id=exp22_4, claim_cat=d, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '120'), level=cll, claim_type=ot);
%summ(measure_id=exp22_6, claim_cat=d, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '122'), level=cll, claim_type=ot);

/*v1.2: updated exp22_8*/
%summ(measure_id=exp22_8, claim_cat=d, sumvar=mdcd_pd_amt, constraint=%str(1=1), level=cll, claim_type=ot);

/*v1.2: updated exp3_5, 8_4, 13_5, 18_5*/
%summ(measure_id=exp3_5,  claim_cat=f, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=ip);
%summ(measure_id=exp8_4,  claim_cat=f, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=lt);
%summ(measure_id=exp13_5, claim_cat=f, sumvar=mdcd_pd_amt,     constraint=%str(1=1), level=cll, claim_type=ot);
%summ(measure_id=exp18_5, claim_cat=f, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=rx);

/*v1.2: updated sumexp_2, 4, 6, 8*/
%summ(measure_id=sumexp_2, claim_cat=i, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=ip);
%summ(measure_id=sumexp_4, claim_cat=i, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=lt);
%summ(measure_id=sumexp_6, claim_cat=i, sumvar=mdcd_pd_amt,     constraint=%str(1=1), level=cll, claim_type=ot);
%summ(measure_id=sumexp_8, claim_cat=i, sumvar=tot_mdcd_pd_amt, constraint=%str(1=1), level=clh, claim_type=rx);

%summ(measure_id=exp24_2, claim_cat=j, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '119'), level=cll, claim_type=ot);
%summ(measure_id=exp21_1, claim_cat=j, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '121'), level=cll, claim_type=ot);
%summ(measure_id=exp24_4, claim_cat=j, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '120'), level=cll, claim_type=ot);
%summ(measure_id=exp24_6, claim_cat=j, sumvar=mdcd_pd_amt, constraint=%str(stc_cd = '122'), level=cll, claim_type=ot);

/*v1.2: updated exp24_8*/
%summ(measure_id=exp24_8, claim_cat=j, sumvar=mdcd_pd_amt, constraint=%str(1=1), level=cll, claim_type=ot);
%mend run_204_all_summ;


/******************************************************************************************
  macro that contains list of all measures created in this module. will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_204 =
 exp1_14	
exp6_21	
exp11_84	
exp11_85	
exp16_13	
exp12_81	
sumexp_1	
sumexp_3	
sumexp_5	
sumexp_7	
exp22_2	
exp20_1	
exp22_4	
exp22_6	
exp22_8	
exp3_5	
exp8_4	
exp13_5	
exp18_5	
sumexp_2	
sumexp_4	
sumexp_6	
sumexp_8	
exp24_2	
exp21_1	
exp24_4	
exp24_6	
exp24_8	
 
 ;
