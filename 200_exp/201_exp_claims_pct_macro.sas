/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
 /******************************************************************************************
 Program: 201_exp_claims_pct_macros.sas
 Project: MACBIS Task 2
 Purpose: Defines and calls claims percentage macros for module 200 (expenditures)
          Designed to be %included in module level driver         
          Uses v1.1 of specs, converted to do measures creation in AREMAC instead of SAS EBI
 
 Author:  Richard Chapman
 Date Created: 3/1/2017
 Current Programmer:
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call creates a single measure, and extracts that measure into a SAS work dataset
         from AREMAC. These tables are named EXP##.#. Most are a single observation, except for
         frequency measures or other measures that have one observation per oberved value.
 
 Modifications:
 
 ******************************************************************************************/
 
 
 
 /*Macro to create claims percentage measures*/
 %macro claims_pct(
  measure_id=, /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_cat=,  /*Which claim category the measure should use. (A,B,C, etc.). Defined in specs. */
  denom=,      /*Logical constraints to apply to the denominator, as defined in specs. Often set to %wtr(1=1), which will not apply any contstraints to denom*/
  numer=,      /*Logical constraints to apply to the denominator, as defined in specs.*/
  level=,      /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  claim_type=  /*Which claim type. IP, LT, RX, OT*/
  );
  
  execute(
    insert into &wrktable..&taskprefix._exp_200
    select 
		 %str(%')&state.%str(%') as submtg_state_cd
    	,%str(%')&measure_id.%str(%') as measure_id
		,'201' as submodule
        ,coalesce(numer,0) as numer
        ,coalesce(denom,0) as denom
	    ,case when coalesce(denom,0) <> 0 then numer/denom else NULL end as mvalue
    from (
      /*inner query first step: flags each line or header (depending on cll/clh parameter) as meeting criteria for numerator and/or denominator
                                (but, in this case, to meet numerator criteria must also meet denominator criteria).
        
        inner query second step: sum() across all lines/headers to create numer and denom values. returns a single observation to outer query.
        
        outer query does the final step of dividing numer by denom*/        
                     
	    select 
	            sum(case when               (&denom.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as denom
	           ,sum(case when (&numer.) and (&denom.) and (claim_cat_&claim_cat. = 1) then 1 else 0 end) as numer
	  from &temptable..&taskprefix._base_&level._&claim_type.
	  %if %lowcase(&level.) = cll %then %do;
	  	where childless_header_flag = 0
	  %end;
	) a
	
  )by tmsis_passthrough;

  *add measure to list of measures in SAS dataset;
  insert into dqout.measures_200 
  values("&measure_id.");
 

%mend claims_pct;


/*macro that contains all the calls to %claims_pct
  
  this macro gets called in the module driver*/

%macro run_201_all_claims_pct;


  /*Start by creating an AREMAC table to hold results*/
  %dropwrktables(exp_200);
  execute(
	  create table &wrktable..&taskprefix._exp_200 (

	     submtg_state_cd STRING
	    ,measure_id STRING
		,submodule STRING
	    ,numer DOUBLE
	    ,denom DOUBLE
	    ,mvalue DOUBLE
	)
  
  )by tmsis_passthrough;



  %claims_pct(measure_id=exp1_1,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_bill_amt=0),                                  level=clh, claim_type=ip);
  %claims_pct(measure_id=exp1_2,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),    level=clh, claim_type=ip);
  %claims_pct(measure_id=exp1_3,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt > 2000000),                       level=clh, claim_type=ip);
  %claims_pct(measure_id=exp6_1,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),    level=clh, claim_type=lt);
  %claims_pct(measure_id=exp6_2,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_bill_amt = 0),                                level=clh, claim_type=lt);
  %claims_pct(measure_id=exp6_3,  claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt > 20000),                         level=clh, claim_type=lt);
  %claims_pct(measure_id=exp11_1, claim_cat=a, denom=%str(1=1),   numer=%str(bill_amt=0),                                      level=cll, claim_type=ot);
  %claims_pct(measure_id=exp11_2, claim_cat=a, denom=%str(1=1),   numer=%str(mdcd_pd_amt=0 or mdcd_pd_amt is null),            level=cll, claim_type=ot);
  %claims_pct(measure_id=exp11_3, claim_cat=a, denom=%str(1=1),   numer=%str(mdcd_pd_amt > 100000),                            level=cll, claim_type=ot);
  %claims_pct(measure_id=exp11_4, claim_cat=a, denom=%str(stc_cd in ('002', '061')),          numer=%str(mdcd_pd_amt=0),       level=cll, claim_type=ot);
  %claims_pct(measure_id=exp16_1, claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt > 300000),                        level=clh, claim_type=rx);
  %claims_pct(measure_id=exp16_2, claim_cat=a, denom=%str(1=1),   numer=%str(tot_bill_amt=0),                                  level=clh, claim_type=rx);
  %claims_pct(measure_id=exp16_3, claim_cat=a, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt =0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=rx);
  %claims_pct(measure_id=exp2_1,  claim_cat=b, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt> 2000000),                        level=clh, claim_type=ip);
  %claims_pct(measure_id=exp2_2,  claim_cat=b, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),    level=clh, claim_type=ip);
  %claims_pct(measure_id=exp7_1,  claim_cat=b, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt>20000),                           level=clh, claim_type=lt);
  %claims_pct(measure_id=exp7_2,  claim_cat=b, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),    level=clh, claim_type=lt);
  %claims_pct(measure_id=exp27_1, claim_cat=b, denom=%str(1=1),   numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),    level=clh, claim_type=ot);

  %claims_pct(measure_id=exp22_9, claim_cat=d, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ot);

  %claims_pct(measure_id=exp3_1,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_bill_amt=0),                                 level=clh, claim_type=ip);
  %claims_pct(measure_id=exp3_2,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ip);
  %claims_pct(measure_id=exp3_3,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt > 2000000),                      level=clh, claim_type=ip);
  %claims_pct(measure_id=exp8_1,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=lt);
  %claims_pct(measure_id=exp8_2,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_bill_amt = 0),                               level=clh, claim_type=lt);
  %claims_pct(measure_id=exp8_3,  claim_cat=f, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt > 20000),                        level=clh, claim_type=lt);
  %claims_pct(measure_id=exp13_1, claim_cat=f, denom=%str(1=1),    numer=%str(bill_amt=0),                                     level=cll, claim_type=ot);
  %claims_pct(measure_id=exp13_2, claim_cat=f, denom=%str(1=1),    numer=%str(mdcd_pd_amt=0 or mdcd_pd_amt is null),           level=cll, claim_type=ot);
  %claims_pct(measure_id=exp13_3, claim_cat=f, denom=%str(1=1),    numer=%str(mdcd_pd_amt > 100000),                           level=cll, claim_type=ot);
  %claims_pct(measure_id=exp13_4, claim_cat=f, denom=%str(stc_cd in ('002', '061')),          numer=%str(mdcd_pd_amt=0),       level=cll, claim_type=ot);
  %claims_pct(measure_id=exp18_1, claim_cat=f, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt > 300000),                      level=clh, claim_type=rx);
  %claims_pct(measure_id=exp18_2, claim_cat=f, denom=%str(1=1),     numer=%str(tot_bill_amt=0),                                level=clh, claim_type=rx);
  %claims_pct(measure_id=exp18_3, claim_cat=f, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt =0 or tot_mdcd_pd_amt is null), level=clh, claim_type=rx);
  %claims_pct(measure_id=exp28_1, claim_cat=g, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),  level=clh, claim_type=ot);
  %claims_pct(measure_id=exp4_1,  claim_cat=g, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt> 2000000),                      level=clh, claim_type=ip);
  %claims_pct(measure_id=exp4_2,  claim_cat=g, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null ), level=clh, claim_type=ip);
  %claims_pct(measure_id=exp9_1,  claim_cat=g, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt>20000),                         level=clh, claim_type=lt);
  %claims_pct(measure_id=exp9_2,  claim_cat=g, denom=%str(1=1),     numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),  level=clh, claim_type=lt);
   
  %claims_pct(measure_id=exp24_9, claim_cat=j, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),  level=clh, claim_type=ot);

   /**new measures */

  %claims_pct(measure_id=exp29_1, claim_cat=p, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ip);
  %claims_pct(measure_id=exp33_1, claim_cat=p, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=lt);
  %claims_pct(measure_id=exp37_1, claim_cat=p, denom=%str(1=1),    numer=%str(mdcd_pd_amt=0 or mdcd_pd_amt is null),           level=cll, claim_type=ot);
  %claims_pct(measure_id=exp41_1, claim_cat=p, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=rx);

  %claims_pct(measure_id=exp30_1, claim_cat=t, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ip);
  %claims_pct(measure_id=exp34_1, claim_cat=t, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=lt);
  %claims_pct(measure_id=exp38_1, claim_cat=t, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ot);

  %claims_pct(measure_id=exp31_1, claim_cat=r, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ip);
  %claims_pct(measure_id=exp35_1, claim_cat=r, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=lt);
  %claims_pct(measure_id=exp39_1, claim_cat=r, denom=%str(1=1),    numer=%str(mdcd_pd_amt=0 or mdcd_pd_amt is null),           level=cll, claim_type=ot);
  %claims_pct(measure_id=exp42_1, claim_cat=r, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=rx);


  %claims_pct(measure_id=exp32_1, claim_cat=v, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ip);
  %claims_pct(measure_id=exp36_1, claim_cat=v, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=lt);
  %claims_pct(measure_id=exp40_1, claim_cat=v, denom=%str(1=1),    numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null),   level=clh, claim_type=ot);

  %claims_pct(measure_id=exp44_1, claim_cat=au, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null), level=clh, claim_type=ip);
  %claims_pct(measure_id=exp44_2, claim_cat=au, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null), level=clh, claim_type=lt);
  %claims_pct(measure_id=exp44_3, claim_cat=au, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null), level=clh, claim_type=ot);
  %claims_pct(measure_id=exp44_4, claim_cat=au, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt <> 0 and tot_mdcd_pd_amt is not null), level=clh, claim_type=rx);
  %claims_pct(measure_id=exp45_1, claim_cat=at, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null), level=clh, claim_type=ip);
  %claims_pct(measure_id=exp45_2, claim_cat=at, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null), level=clh, claim_type=lt);
  %claims_pct(measure_id=exp45_3, claim_cat=at, denom=%str(1=1), numer=%str(tot_mdcd_pd_amt=0 or tot_mdcd_pd_amt is null), level=clh, claim_type=ot);

%mend run_201_all_claims_pct;

/******************************************************************************************
  macro that contains list of all measures created in this module. will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_201 =
 exp1_1	
 exp1_2	
 exp1_3	
 exp6_1	
 exp6_2	
 exp6_3	
 exp11_1	
 exp11_2	
 exp11_3	
 exp11_4	
 exp16_1	
 exp16_2	
 exp16_3	
 exp2_1	
 exp2_2	
 exp7_1	
 exp7_2	
 exp27_1

 exp22_9
	
 exp3_1	
 exp3_2	
 exp3_3	
 exp8_1	
 exp8_2	
 exp8_3	
 exp13_1	
 exp13_2	
 exp13_3	
 exp13_4	
 exp18_1	
 exp18_2	
 exp18_3	
 exp28_1	
 exp4_1	
 exp4_2	
 exp9_1	
 exp9_2	

 exp24_9

 exp29_1
 exp33_1
 exp37_1
 exp41_1
 exp30_1
 exp34_1
 exp38_1
 exp31_1
 exp35_1
 exp39_1
 exp42_1
 exp32_1
 exp36_1
 exp40_1

 exp44_1
 exp44_2
 exp44_3
 exp44_4
 exp45_1
 exp45_2
 exp45_3
;
