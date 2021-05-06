/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 310_claims_freq_macro.sas   
 Project: MACBIS Task 2
 Purpose: Defines and calls ratio macros for module 300 (ffs and managed care)
          Designed to be %included in module level driver         

 
 Author:  Richard Chapman
 Date Created: 8/18/2017
 Current Programmer: Jacqueline Agufa
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call creates a single measure, and extracts that measure into a SAS work dataset
         from AREMAC. These tables are named EXP##.#. Most are a single observation, except for
         frequency measures or other measures that have one observation per oberved value.
 
 Modifications:
       Dec 2017-JMA-V1.3
                   -Changed code to show N,1,2,3,4,5,A,B,C,D,E,U,V,W,X,Y,Z,T 
                   -N missing, null and invalid responses 
                   -T total count 
 
 ******************************************************************************************/
 
 
/******************************************************************************************

 ******************************************************************************************/
%global TypeOfService_list;
select quote(put(TypeOfService,z3.),"'") into :TypeOfService_list
   separated by ', '
from stc_cd.Sheet1
;

/*
select quote(put(TypeOfService,z3.),"'") into :TypeOfService_list2
   separated by '|'
from stc_cd.Sheet1
;

select count(*) into :TypeOfService_num
from stc_cd.Sheet1
where TypeOfService is not null
;*/

  
   
%macro frq(
  measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_type=,  /*Which claim type. IP, LT, RX, OT*/
  level=,       /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  var=,         /*variable on which to run the freq*/
  len=,         /*length of list item, must be 2 or more*/
  list=,        /*list of valid values*/
  constraint=,  /*constraint to apply prior to freq*/  
  );

  execute(

      insert into &utl_output

      select
      submtg_state_cd
      , %tslit(&measure_id)
      , '712'
      , null
      , null
      , mvalue1
      , valid_value
      , null /*%tslit(%upcase(&claim_type))*/
        
      from ( 

      select distinct submtg_state_cd, &var. as valid_value, count(&var.) as mvalue1
      from &temptable..&taskprefix._base_&level._&claim_type.
      where (&constraint. and &var. in (&list )) 
      %if %lowcase(&level.) = cll %then %do;
      and childless_header_flag = 0
          %end;
      group by submtg_state_cd, valid_value ) a
          )by tmsis_passthrough;	

  execute(
          
    insert into &utl_output
        
		select
          submtg_state_cd
          , %tslit(&measure_id)
          , '712'
          , null
          , null
          , mvalue1
          , valid_value
          , null /*%tslit(%upcase(&claim_type))*/
        
        from ( 

    select distinct submtg_state_cd, %str(%')A_%str(%') as valid_value, count(&var.) as mvalue1
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint. and &var. in (&list ) ) 
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
    group by submtg_state_cd ) a
    )by tmsis_passthrough;	

  execute(

        insert into &utl_output
        
		select
        submtg_state_cd
        , %tslit(&measure_id)
        , '712'
        , null
        , null
        , mvalue1
        , valid_value
        , null /*%tslit(%upcase(&claim_type))*/
        
        from ( 

        select distinct submtg_state_cd, %str(%')N_%str(%') as valid_value, count(submtg_state_cd) as mvalue1
        from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint. and (&var. is not null and &var. not in (&list ) )) 
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
            group by submtg_state_cd) a
                )by tmsis_passthrough;	

  execute(

       insert into &utl_output
        
		select
        submtg_state_cd
        , %tslit(&measure_id)
        , '712'
        , null
        , null
        , mvalue1
        , valid_value
        , null /*%tslit(%upcase(&claim_type))*/
        
        from ( 

    select distinct submtg_state_cd, %str(%')M_%str(%') as valid_value, count(submtg_state_cd) as mvalue1
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint. and (&var. is null)) 
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
    group by submtg_state_cd) a
    )by tmsis_passthrough;	

 execute(

        insert into &utl_output
        
		select
        submtg_state_cd
        , %tslit(&measure_id)
        , '712'
        , null
        , null
        , mvalue1
        , valid_value
        , null /*%tslit(%upcase(&claim_type))*/
        
        from ( 

    select distinct submtg_state_cd, %str(%')T_%str(%') as valid_value, sum (1) as mvalue1
    from &temptable..&taskprefix._base_&level._&claim_type.
	where (&constraint.) 
	%if %lowcase(&level.) = cll %then %do;
	    and childless_header_flag = 0
	%end;
	group by submtg_state_cd ) a

  )by tmsis_passthrough;
  
  %insert_freq_msr(msrid=&measure_id, list1=%quote('N_', 'M_', 'A_'), list2=%quote(&list));        

%mend frq;

/******************************************************************************************
  Macro that contains all the calls to create each measure
  
  This macro gets called in the module driver
 ******************************************************************************************/

%macro utl_all_clms_freq_stc_cd;

  %frq(measure_id=ALL28_1, claim_type=IP, level=CLL, var=stc_cd, len=3, list=%quote(&TypeOfService_list), constraint=%str(claim_cat_aj = 1));
  %frq(measure_id=ALL29_1, claim_type=LT, level=CLL, var=stc_cd, len=3, list=%quote(&TypeOfService_list), constraint=%str(claim_cat_aj = 1));
  %frq(measure_id=ALL30_1, claim_type=OT, level=CLL, var=stc_cd, len=3, list=%quote(&TypeOfService_list), constraint=%str(claim_cat_aj = 1));
  %frq(measure_id=ALL31_1, claim_type=RX, level=CLL, var=stc_cd, len=3, list=%quote(&TypeOfService_list), constraint=%str(claim_cat_aj = 1));

%mend utl_all_clms_freq_stc_cd;

/******************************************************************************************
  Macro that contains list of all measures created in this module. Will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_712 =
 ALL28_1
 ALL29_1
 ALL30_1
 ALL31_1
;          
