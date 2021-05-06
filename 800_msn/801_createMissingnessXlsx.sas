/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

******************************************************************************************
* Program: readMissingnessXlsx.sas
* Purpose: Read Variables from excel files create macros 
* Date:    03/24/2018
*
* Input Files:Data Elements for State DQ Missingness Measures v1.4.xlsx
* Output Files:MissingnessVariables.xlsx
*

* Updated: 
*        :  

******************************************************************************************;


%macro _801;
    
    ***read Data Elements for State DQ Missingness Measures v1.4.xlsx 
       and create the variables needed for the macros;
    create table missingVars as
    select *
           ,tranwrd('Measure ID Short'n, ".", "_") as Measure_ID_Short2                                     /*MIS11.48 to MIS11_48*/
           ,tranwrd(scan(Data_element,1,"."),"tmsis_","") as Non_claims_Table2                                                                    /*PRV00001 to PRV01*/
		   ,'File Type'n as File_Type2                                                                      /*IP or OT */ 
           ,'Claims_cat_type'n as Claims_cat_type2                                                          /*AA or W */ 
		   ,case when 'File Type - Summary'n="Claims" then substr(Data_element,7,3) else " " end as Data_element_ln_hdr /*CLH or CLL */ 
		   ,scan(Data_element,-1,".") as Data_element_var                                                   /*variable name like stc_cd */ 
		   ,case when size is not null then substr(size,index(size,"(" )+1, index(size,")" )-index(size,"(" )-1 )
		        else size end as size_length                                                                /* Length of the variable */
		  

    from missVar.'Final List'n
    where 'Active Indicator'n = 'Y'
     ;
	*Create missing claims and non-claims tables;

	create table Miss_Non_claims as
	   select * 
	   from missingVars
	   where 'File Type - Summary'n="Non-claims";


	create table Miss_Claims as
	   select *,
           case when Data_element_var='orgnl_clm_num'    then 'orgnl_clm_num_orig'  
                when Data_element_var='adjstmt_clm_num'  then 'adjstmt_clm_num_orig'  
                when Data_element_var='adjdctn_dt_num'   then 'adjdctn_dt_orig'  
                when Data_element_var='orgnl_line_num'   then 'orgnl_line_num_orig'  
                when Data_element_var='adjstmt_line_num' then 'adjstmt_line_num_orig'  
                when Data_element_var='line_adjstmt_ind' then 'line_adjstmt_ind_orig' 
                else Data_element_var
           end as Data_element_var_updt
	   from missingVars
	  
	   where 'File Type - Summary'n="Claims";

%mend _801; 

