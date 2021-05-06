/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 803_miss_non_claims_pct.sas
 Project: MACBIS Task 12

 Author:  Jacqueline Agufa
  
 Input: Temporary AREMAC tables created in 800_miss_driver_sql.sas
 
 Output: All missingness non-claims percentage measures combined in miss_pct SAS work dataset
         
 Modifications: 
 ******************************************************************************************/

    

%macro _803;
	/****** Measure eg MIS1_1  ********/
    select Measure_ID_Short2   into :miss_measure_nc 
           separated by ' ' 
    from Miss_Non_claims; 

	/****** Variable eg race_cd ********/
    select Data_element_var    into :miss_numer_var_nc 
           separated by ' ' 
    from Miss_Non_claims; 

	/****** File Type eg ELG ********/
	select File_Type2          into :miss_file_type_nc 
         separated by ' ' 
    from Miss_Non_Claims; 

	/****** Non-Claims Table  ********/
    select Non_claims_Table2 into :miss_claims_Table2_nc 
           separated by ' ' 
    from Miss_Non_claims; 
    
	/****** Variable length  ********/
    select size_length         into :miss_var_len_nc 
           separated by ' ' 
    from Miss_Non_Claims; 


    %dropwrktables(all_miss_non_claims);

	%macro miss_non_claims;
		%do a=1 %to %sysfunc(countw(&miss_measure_nc.));

			%dropwrktables(miss_non_claims_&a.);

		    execute
			(
		     create table &wrktable..&taskprefix._miss_non_claims_&a. as
		       	select %str(%')&state.%str(%') as submtg_state_cd, %str(%')%scan(&miss_measure_nc., &a.)%str(%') as measure_id, 
		             &a. as miss_position,  %str(%')%scan(&miss_numer_var_nc.,&a.)%str(%') as miss_varname, numer, denom, 
			         case when denom <> 0 then numer/denom else NULL end as mvalue
		        from 
				    (select count(1) as denom, sum(has_numerator) as numer
					from
		            (
		    		 select 
		  			     %if ("%scan(&miss_numer_var_nc., &a.)") = ("ethncty_cd")
		                 %then %do;
			              min(case when  %quote( %miss_misslogic_c6( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
		                 %end;
					     %else %if ("%scan(&miss_numer_var_nc., &a.)")= ("race_cd")
		                 %then %do;
			              min(case when  %quote( %miss_misslogic_c017( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
		                 %end;
		                 %else %if ("%scan(&miss_numer_var_nc., &a.)")= ("gndr_cd")
		                 %then %do;
			              min(case when  %quote( %miss_misslogic_cU( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
		                 %end;
		                 %else %if ("%scan(&miss_numer_var_nc., &a.)")= ("prvdr_clsfctn_cd")
		                 %then %do;
			              min(case when  %quote( %miss_misslogic_c88( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
		                 %end;
		                 %else %if (("%scan(&miss_numer_var_nc., &a.)")= ("prvdr_lctn_id") and
						            (("%scan(&miss_claims_Table2_nc., &a.)") =("prvdr_lctn_cntct") or
									 ("%scan(&miss_claims_Table2_nc., &a.)") =("prvdr_lcnsg") or
									 ("%scan(&miss_claims_Table2_nc., &a.)") =("prvdr_id") 
                                     ))
		                 %then %do;
						  min(case when  %quote( %miss_misslogic_ex000( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
                         %end;
                         %else %do; 
			              min(case when  %quote( %miss_misslogic( %scan(&miss_numer_var_nc., &a.) ) )   then 1 else 0 end) as has_numerator
		                 %end;


		                 %if ("%scan(&miss_file_type_nc.,&a.)") = ("ELG") 
		                 %then %do;
			              from &temptable..&taskprefix._msng_%scan(&miss_claims_Table2_nc.,&a.)
		                  group by msis_ident_num    
		                 %end;
						 %else %if ("%scan(&miss_file_type_nc.,&a.)") = ("MCR")
		                 %then %do;
			              from &temptable..&taskprefix._msng_%scan(&miss_claims_Table2_nc.,&a.)
		                  group by state_plan_id_num  
		                 %end;
		                 %else %if ("%scan(&miss_file_type_nc.,&a.)") = ("PRV")
		                 %then %do;
			              from &temptable..&taskprefix._msng_%scan(&miss_claims_Table2_nc.,&a.)
		                  group by submtg_state_prvdr_id 
		                  %end;
						  %else %if ("%scan(&miss_file_type_nc.,&a.)") = ("TPL") 
		                  %then %do;
			              from &temptable..&taskprefix._msng_%scan(&miss_claims_Table2_nc.,&a.)
		                  group by msis_ident_num   
						  %end;

		        		) c
		                ) d	  

		  )by tmsis_passthrough; 
          
          %emptytable(miss_non_claims_&a.);

		%end;
	%mend miss_non_claims;
	%miss_non_claims;

	%macro assemble_non_claims;

		execute(
			create table &wrktable..&taskprefix._all_miss_non_claims as
			select * from &wrktable..&taskprefix._miss_non_claims_1
			%do a=2 %to %sysfunc(countw(&miss_measure_nc.));
				union
				select * from &wrktable..&taskprefix._miss_non_claims_&a.
			%end;
		) by tmsis_passthrough;

	   /*extract measure from AREMAC into sas*/
	  create table all_miss_non_claims as
	  select * from connection to tmsis_passthrough
	  (select * from &wrktable..&taskprefix._all_miss_non_claims);
	  
	  /*drop measure-level temp table from AREMAC*/
	  %dropwrktables(all_miss_non_claims);

  	  %do a=1 %to %sysfunc(countw(&miss_measure_nc.));
	  	%dropwrktables(miss_non_claims_&a.);
	  %end;

	%mend assemble_non_claims;
	%assemble_non_claims;
 

%mend _803;
