/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 711_utl_all_clms_freq_sql.sas   
 Project: MACBIS Task 2
 Purpose: Defines and calls ratio macros for module 300 (ffs and managed care)
          Designed to be %included in module level driver         

 
 Author:  Joel Smith
 Date Created: 11/04/2020
 Current Programmer: Joel Smith
 
 Input: must be called from module level driver, which creates full set of temporary AREMAC tables
        using the standard pull code. Also requires macro variables defined from macros called in 
        module level driver.
 
 Output: Each macro call creates a single measure, and extracts that measure into a SAS work dataset
         from AREMAC. These tables are named ALL##.#. Most are a single observation, except for
         frequency measures or other measures that have one observation per oberved value.
 
 Modifications:
 
 ******************************************************************************************/
 
 
/******************************************************************************************

 ******************************************************************************************/
 
   select quote(Taxonomy, "'")  into :Taxonomy_List 
       separated by ", "
   from prvtxnmy.Sheet1
       ;

   %let taxonomy_fvar=%str(
       CASE WHEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 4) IN ('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053') THEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 4)
       WHEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 2) IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '36', '37', '38')
       THEN SUBSTRING(blg_prvdr_txnmy_cd, 1, 2)
       ELSE NULL END
       );

   %let bill_fvar=%str(CASE WHEN substring(bill_type_cd, 1, 3) in 
   ('011', '012', '013', '014', '015', '016', '018', '021', '022', '023', '024', '025', '026', '028', '031', '032', '033', '034', '035', '036', '038', '041', '042', '043', '044', '045', '046', '048', '061', '062', '063', '064', '065', '066', '068', '071', '072', '073', '074', '075', '076', '077', '078', '079', '081', '082', '083', '084', '085', '086', '087', '089')
   then substring(bill_type_cd, 1, 3) else null end);

        %macro frq_list(
            measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
            claim_type=,  /*Which claim type. IP, LT, RX, OT*/
            var=,         /*variable on which to run the freq*/
            fvar=,        /*function of variable on which to run the freq*/
            len=,         /*length of list item, must be 2 or more*/
            list=,         /*list of valid values*/
            constraint=%str(claim_cat_aj=1)
            );

            execute(
                insert into &utl_output

                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
                
                from ( 

                select distinct submtg_state_cd, &fvar. as valid_value, count(&var.) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint. and &fvar. in (&list))
                group by submtg_state_cd, valid_value) a
                    )by tmsis_passthrough;

            execute(
                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
                
                from ( 

                    select distinct submtg_state_cd, %str(%')A_%str(%') as valid_value, count(&var.) as mvalue
                    from &temptable..&taskprefix._base_clh_&claim_type.
                    where (&constraint. and &fvar. in (&list) )
                    group by submtg_state_cd) a
                )by tmsis_passthrough;

            execute(
                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
                
                from ( 

                select distinct submtg_state_cd, %str(%')N_%str(%') as valid_value, count(submtg_state_cd) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint. and (&fvar. is null ) and &var is not null )
                group by submtg_state_cd) a
                )by tmsis_passthrough;

            execute(
                
                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
        
                from ( 

                select distinct submtg_state_cd, %str(%')M_%str(%') as valid_value, count(submtg_state_cd) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint. and (&var. is null ))
                group by submtg_state_cd) a
                )by tmsis_passthrough;

            execute(

                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
                
                from ( 

                select distinct submtg_state_cd, %str(%')T_%str(%') as valid_value, sum (1) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint.)
                group by submtg_state_cd) a
                )by tmsis_passthrough;

            %insert_freq_msr(msrid=&measure_id
                , list1=%quote('N_', 'M_', 'A_')
                , list2=%quote('011', '012', '013', '014', '015', '016', '018', '021', '022', 
                               '023', '024', '025', '026', '028', '031', '032', '033', '034', 
                               '035', '036', '038', '041', '042', '043', '044', '045', '046', 
                               '048', '061', '062', '063', '064', '065', '066', '068', '071', 
                               '072', '073', '074', '075', '076', '077', '078', '079', '081', 
                               '082', '083', '084', '085', '086', '087', '089'));
            
            %mend;

        %macro frq_list_tax(
            measure_id=,  /*measure id of the measure you want to create. eg: EXP1.1*/
            claim_type=,  /*Which claim type. IP, LT, RX, OT*/
            var=,         /*variable on which to run the freq*/
            dist_var=,    /*function of variable on which to run the freq*/
            len=,         /*length of list item, must be 2 or more*/
            dist_list=,   /*list of distrib valid values*/
            constraint=%str(claim_cat_aj=1)
            );

            %local list;
            %let list=&Taxonomy_List;
            execute(
                
                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , case when valid_value IN ('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053') then valid_value
                  else concat(valid_value, 'XX') end
                , %tslit(%upcase(&claim_type))
                
                from ( 

                select distinct submtg_state_cd, &dist_var. as valid_value, count(&var.) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint. and &dist_var. in (&dist_list) and &var in (&Taxonomy_List))
                group by submtg_state_cd, valid_value) a
                    )by tmsis_passthrough;

            execute(
                
                insert into &utl_output
        
                select 
                submtg_state_cd
                , %tslit(&measure_id)
                , '711'
                , null
                , null
                , mvalue
                , valid_value
                , %tslit(%upcase(&claim_type))
                
                from ( 

                select distinct submtg_state_cd, %str(%')A_%str(%') as valid_value, count(&var.) as mvalue
                from &temptable..&taskprefix._base_clh_&claim_type.
                where (&constraint. and &dist_var. in (&dist_list) and &var in (&Taxonomy_List) )
                group by submtg_state_cd) a
                )by tmsis_passthrough;

                                                /*or &var not in (&Taxonomy_List)*/
                execute(

                    insert into &utl_output
        
                select 
                    submtg_state_cd
                    , %tslit(&measure_id)
                    , '711'
                    , null
                    , null
                    , mvalue
                    , valid_value
                    , %tslit(%upcase(&claim_type))
                    
                    from ( 

                    select distinct submtg_state_cd, %str(%')N_%str(%') as valid_value, count(submtg_state_cd) as mvalue
                    from &temptable..&taskprefix._base_clh_&claim_type.
                    where &constraint. and &var is not null and (&dist_var. is null or &var not in (&Taxonomy_List) )
                    group by submtg_state_cd) a
                    )by tmsis_passthrough;

                execute(
                    
                    insert into &utl_output
                    
                select 
                    submtg_state_cd
                    , %tslit(&measure_id)
                    , '711'
                    , null
                    , null
                    , mvalue
                    , valid_value
                    , %tslit(%upcase(&claim_type))
                    
                    from ( 
                    
                    select distinct submtg_state_cd, %str(%')M_%str(%') as valid_value, count(submtg_state_cd) as mvalue
                    from &temptable..&taskprefix._base_clh_&claim_type.
                    where (&constraint. and (&var. is null ))
                    group by submtg_state_cd) a
                    )by tmsis_passthrough;

                execute(
                    
                    insert into &utl_output
        
                select 
                    submtg_state_cd
                    , %tslit(&measure_id)
                    , '711'
                    , null
                    , null
                    , mvalue
                    , valid_value
                    , %tslit(%upcase(&claim_type))
                    
                    from ( 

                    select distinct submtg_state_cd, %str(%')T_%str(%') as valid_value, sum (1) as mvalue
                    from &temptable..&taskprefix._base_clh_&claim_type.
                    where (&constraint.)
                    group by submtg_state_cd) a
                    )by tmsis_passthrough;

                %insert_freq_msr(msrid=&measure_id
                    , list1=%quote('N_', 'M_', 'A_')
                    , list2=%quote('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053', 
                                   '10XX', '11XX', '12XX', '13XX', '14XX', '15XX', '16XX', '17XX', '18XX', '20XX', '21XX', '22XX', 
                                   '23XX', '24XX', '25XX', '26XX', '27XX', '28XX', '29XX', '30XX', '31XX', '32XX', '33XX', '34XX', 
                                   '36XX', '37XX', '38XX'));
    %mend;

        
%macro utl_all_clms_freq_oth;

        %frq_list(
            measure_id=ALL32_1_ip
            , claim_type=ip
            , var=bill_type_cd
            , fvar=&bill_fvar
            , len=3
            , list=%str('011', '012', '013', '014', '015', '016', '018', '021', '022', '023', '024', '025', '026', '028', '031', '032', '033', '034', '035', '036', '038', '041', '042', '043', '044', '045', '046', '048', '061', '062', '063', '064', '065', '066', '068', '071', '072', '073', '074', '075', '076', '077', '078', '079', '081', '082', '083', '084', '085', '086', '087', '089')
            );
        %frq_list(
            measure_id=ALL32_1_lt
            , claim_type=lt
            , var=bill_type_cd
            , fvar=&bill_fvar
            , len=3
            , list=%str('011', '012', '013', '014', '015', '016', '018', '021', '022', '023', '024', '025', '026', '028', '031', '032', '033', '034', '035', '036', '038', '041', '042', '043', '044', '045', '046', '048', '061', '062', '063', '064', '065', '066', '068', '071', '072', '073', '074', '075', '076', '077', '078', '079', '081', '082', '083', '084', '085', '086', '087', '089')
            );
        %frq_list(
            measure_id=ALL32_1_ot
            , claim_type=ot
            , var=bill_type_cd
            , fvar=&bill_fvar
            , len=3
            , list=%str('011', '012', '013', '014', '015', '016', '018', '021', '022', '023', '024', '025', '026', '028', '031', '032', '033', '034', '035', '036', '038', '041', '042', '043', '044', '045', '046', '048', '061', '062', '063', '064', '065', '066', '068', '071', '072', '073', '074', '075', '076', '077', '078', '079', '081', '082', '083', '084', '085', '086', '087', '089')
            );

        %frq_list_tax(
            measure_id=ALL33_1_ip
            , claim_type=ip
            , var=blg_prvdr_txnmy_cd
            , dist_var=&taxonomy_fvar
            , len=4
            , dist_list=%str('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053', '10', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '36', '37', '38')
            );
        %frq_list_tax(
            measure_id=ALL33_1_lt
            , claim_type=lt
            , var=blg_prvdr_txnmy_cd
            , dist_var=&taxonomy_fvar
            , len=4
            , dist_list=%str('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053', '10', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '36', '37', '38')
            );
        %frq_list_tax(
            measure_id=ALL33_1_ot
            , claim_type=ot
            , var=blg_prvdr_txnmy_cd
            , dist_var=&taxonomy_fvar
            , len=4
            , dist_list=%str('1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053', '10', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '36', '37', '38')
            );

    %mend;

/******************************************************************************************
  Macro that contains list of all measures created in this module. Will be used
  to set all measure-level datasets together in driver module.
 ******************************************************************************************/
 %let set_list_711 =
 ALL32_1
 ALL33_1
;        
