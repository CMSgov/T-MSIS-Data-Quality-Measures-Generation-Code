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
                   -T total count ;
 
 ******************************************************************************************/
 
 
/******************************************************************************************

 ******************************************************************************************/
 
%macro frq(
  measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
  claim_type=,  /*Which claim type. IP, LT, RX, OT*/
  level=,       /*Whether to apply to header level or line level data. =CLH for header level, =CLL for line level*/
  var=,         /*variable on which to run the freq*/
  constraint=,  /*constraint to apply prior to freq*/  
  );

    execute (
        
        insert into &utl_output
        
		select
        submtg_state_cd
        , %tslit(&measure_id)
        , '710'
        , null
        , null
        , mvalue
        , valid_value
        , null /*%tslit(%upcase(&claim_type))*/
        
        from ( 

        select distinct submtg_state_cd, &var. as valid_value, count(&var.) as mvalue
        from &temptable..&taskprefix._base_&level._&claim_type.
        where (&constraint. and &var. in ('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z')) 
        %if %lowcase(&level.) = cll %then %do;
        and childless_header_flag = 0
            %end;
        group by submtg_state_cd, valid_value ) a
            
            ) by tmsis_passthrough;	

    execute(

            insert into &utl_output
            
		select
        submtg_state_cd
            , %tslit(&measure_id)
            , '710'
            , null
            , null
            , mvalue
            , valid_value
            , null /*%tslit(%upcase(&claim_type))*/

            from (
          
            select distinct submtg_state_cd, %str(%')A_%str(%') as valid_value, count(&var.) as mvalue
            from &temptable..&taskprefix._base_&level._&claim_type.
            where (&constraint. and &var. in ('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z') ) 
            %if %lowcase(&level.) = cll %then %do;
            and childless_header_flag = 0
                %end;
            group by submtg_state_cd ) a
                
        ) by tmsis_passthrough;	

    execute(

            insert into &utl_output
            
		select
        submtg_state_cd
            , %tslit(&measure_id)
            , '710'
            , null
            , null
            , mvalue
            , valid_value
            , null /*%tslit(%upcase(&claim_type))*/

            from (
                select distinct submtg_state_cd, %str(%')N_%str(%') as valid_value, count(submtg_state_cd) as mvalue
                from &temptable..&taskprefix._base_&level._&claim_type.
                where (&constraint. and (&var. is null or &var. not in ('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z') )) 
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
            , '710'
            , null
            , null
            , mvalue
            , valid_value
            , null /*%tslit(%upcase(&claim_type))*/

            from (

                    select distinct submtg_state_cd, %str(%')T_%str(%') as valid_value, sum (1) as mvalue
                    from &temptable..&taskprefix._base_&level._&claim_type.
                    where (&constraint.) 
                    %if %lowcase(&level.) = cll %then %do;
                    and childless_header_flag = 0
                        %end;
                    group by submtg_state_cd ) a
                        
                        )by tmsis_passthrough;
  

    %insert_freq_msr(msrid=&measure_id, list1=%quote('N_', 'A_'), list2=%quote('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z'));
  
%mend frq;

/******************************************************************************************
  Macro that contains all the calls to create each measure
  
  This macro gets called in the module driver
 ******************************************************************************************/

%macro utl_all_clms_freq_sql;

  %frq(measure_id=ALL22_1, claim_type=IP, level=CLH, var=clm_type_cd, constraint=%str(1=1));
  %frq(measure_id=ALL23_1, claim_type=LT, level=CLH, var=clm_type_cd, constraint=%str(1=1));
  %frq(measure_id=ALL24_1, claim_type=OT, level=CLH, var=clm_type_cd, constraint=%str(1=1));
  %frq(measure_id=ALL25_1, claim_type=RX, level=CLH, var=clm_type_cd, constraint=%str(1=1));

%mend utl_all_clms_freq_sql;

/******************************************************************************************
  Macro that contains list of all measures created in this module. Will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_710 =
 ALL22_1
 ALL23_1
 ALL24_1
 ALL25_1
;          
