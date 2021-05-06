/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 802_miss_claim_pct.sas
 Project: MACBIS Task 12

 Author:  Jacqueline Agufa
  
 Input: Temporary AREMAC tables created in 800_miss_driver_sql.sas
 
 Output: All missingness claims percentage measures combined in miss_pct SAS work dataset
         
 Modifications: 
 ******************************************************************************************/


%macro _802;
    select Measure_ID_Short2   into :miss_measure 
           separated by ' ' 
    from Miss_Claims; 

    select Data_element_var_updt    into :miss_numer_var 
           separated by ' ' 
    from Miss_Claims; 

    select File_Type2          into :miss_claim_type 
           separated by ' ' 
    from Miss_Claims; 

	select Claims_cat_type2          into :miss_claims_cat_type 
           separated by ' ' 
    from Miss_Claims; 

    select Data_element_ln_hdr into :miss_level 
           separated by ' ' 
    from Miss_Claims; 
    
    select size_length         into :miss_var_len 
           separated by ' ' 
    from Miss_Claims; 

    %dropwrkviews(all_miss_claims);
      
    execute
	(
     create or replace view &wrktable..&taskprefix._all_miss_claims as
       	select %str(%')&state.%str(%') as submtg_state_cd, measure_id, 1 as miss_position, 
             %str(%')%scan(&miss_numer_var.,1)%str(%') as miss_varname, numer, denom,
	         case when denom <> 0 then numer/denom else NULL end as mvalue
        from 
            (
	          select %str(%')%scan(&miss_measure., 1)%str(%') as measure_id
	          ,count(1) as denom

			  %if ("%scan(&miss_numer_var.,1)")= ("admsn_type_cd")
              %then %do;
              ,sum(case when ( %quote( %miss_misslogic_c9( %scan(&miss_numer_var.,1) ) ) )  then 1 else 0 end) as numer
              %end;
              %else %if ("%scan(&miss_numer_var.,1)")= ("prcdr_cd_ind")
              %then %do;
              ,sum(case when ( %quote( %miss_misslogic_c88_99( %scan(&miss_numer_var.,1) ) ) )  then 1 else 0 end) as numer
              %end;
              %else %do; 
              ,sum(case when ( %quote( %miss_misslogic( %scan(&miss_numer_var.,1) ) ) )  then 1 else 0 end) as numer
              %end;

              from &temptable..&taskprefix._base_%scan(&miss_level.,1)_%scan(&miss_claim_type.,1)
			  where claim_cat_%scan(&miss_claims_cat_type.,1) = 1
			  %if %lowcase(%scan(&miss_level.,1)) = cll %then %do;
			  	and childless_header_flag = 0
			  %end;

	          ) a
     %do a=2 %to %sysfunc(countw(&miss_measure.));
     /*%do a=2 %to 3;*/
     	union all
     	select %str(%')&state.%str(%') as submtg_state_cd, measure_id, &a. as miss_position, 
           %str(%')%scan(&miss_numer_var.,&a.)%str(%') as miss_varname, numer, denom, 
	       case when denom <> 0 then numer/denom else NULL end as mvalue
        from (
	          select %str(%')%scan(&miss_measure., &a. ) %str(%') as measure_id
	          ,count(1) as denom
			  %if ("%scan(&miss_numer_var.,&a.)")= ("admsn_type_cd")
              %then %do;
              ,sum(case when ( %quote( %miss_misslogic_c9( %scan(&miss_numer_var.,&a.) ) ) ) then 1 else 0 end) as numer
              %end;
              %else %if ("%scan(&miss_numer_var.,&a.)")= ("prcdr_cd_ind")
              %then %do;
              ,sum(case when ( %quote( %miss_misslogic_c88_99( %scan(&miss_numer_var.,&a.) ) ) ) then 1 else 0 end) as numer
              %end;
              %else %do; 
              ,sum(case when ( %quote( %miss_misslogic( %scan(&miss_numer_var.,&a.) ) ) ) then 1 else 0 end) as numer
              %end;
               from &temptable..&taskprefix._base_%scan(&miss_level.,&a.)_%scan(&miss_claim_type.,&a.)
			  where claim_cat_%scan(&miss_claims_cat_type.,&a.) = 1
			  %if %lowcase(%scan(&miss_level.,&a.)) = cll %then %do;
			  	and childless_header_flag = 0
			  %end;

	        ) b
    %end;
  

  )by tmsis_passthrough; 

   /*extract measure from AREMAC into sas*/
  create table all_miss_claims as
  select * from connection to tmsis_passthrough
  (select * from &wrktable..&taskprefix._all_miss_claims);

  /*drop measure-level temp table from AREMAC*/
  %dropwrkviews(all_miss_claims);
 
     
%mend _802;











