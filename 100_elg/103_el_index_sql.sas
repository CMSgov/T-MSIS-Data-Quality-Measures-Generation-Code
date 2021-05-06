/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 103_el_index_sql.sas
 Project: MACBIS Task 2

 Author:  Kerianne Hourihan & Sabitha Gopalsamy
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: All index measures combined in el_index SAS work dataset
         
 Modifications: 9/5 : SGO updated #EL1.4,#EL1.5,#EL1.7 and #EL1.8 for V1.2 updates
				12/6/17 : SGO updated #EL10.1 for V1.3 updates
				1/18/17 : SGO Added Emptytable macros 
 
 ******************************************************************************************/

/*index of dissimilarity*/


%macro _build_index_measure_tables(measure=,lbl=,input_dsn=,var=,condition=);

%dropwrkviews(&measure.a &measure.b &measure.a_prior &measure.b_prior &measure._comp);
%dropwrktables(&measure.);

execute(
	   create or replace view &wrktable..&taskprefix._&measure.a as   
	   select &var.,
	          count(distinct msis_ident_num) as rec_count
		from &temptable..&taskprefix._&input_dsn.
		where &condition.
		group by &var.
		)by tmsis_passthrough;


execute(
	   create or replace view &wrktable..&taskprefix._&measure.b as   
	   select 
			   &var. as valid_value
	   		  ,rec_count
			  ,sum(rec_count) over () as sum_rec_count
		from &wrktable..&taskprefix._&measure.a
		) by tmsis_passthrough;


execute(
	   create or replace view &wrktable..&taskprefix._&measure.a_prior as   
	   select &var.,
	          count(distinct msis_ident_num) as rec_count
		from &temptable..&taskprefix._&input_dsn._prior
		where &condition.
		group by &var.
		)by tmsis_passthrough;

execute(
	   create or replace view &wrktable..&taskprefix._&measure.b_prior as   
	   select 
			   &var. as valid_value
	   		  ,rec_count
			  ,sum(rec_count) over () as sum_rec_count
		from &wrktable..&taskprefix._&measure.a_prior
		) by tmsis_passthrough;


	execute(
	create or replace view &wrktable..&taskprefix._&measure._comp as
	select 
		 coalesce(b1.valid_value,b2.valid_value) as valid_value
		,case when b1.sum_rec_count > 0 and b2.sum_rec_count > 0
		 	then abs(((b1.rec_count/b1.sum_rec_count) - (b2.rec_count/b2.sum_rec_count))/2)
		 	else null end as pct_comp
	from &wrktable..&taskprefix._&measure.b b1
	full join &wrktable..&taskprefix._&measure.b_prior b2
	on b1.valid_value = b2.valid_value
	) by tmsis_passthrough;


	execute(
	create table &wrktable..&taskprefix._&measure as
	select %str(%')&state.%str(%') as submtg_state_cd,
		 %str(%')&lbl.%str(%') as measure
		,round(sum(pct_comp),2) as pct
	from &wrktable..&taskprefix._&measure._comp
) by tmsis_passthrough;

%emptytable(&measure.);

%dropwrkviews(&measure.a &measure.a_prior &measure.b &measure.b_prior &measure._comp);

%mend _build_index_measure_tables;

%macro _103;

proc sql;
	%tmsis_connect;

%_build_index_measure_tables(measure=el104,lbl=el1.4,input_dsn=tmsis_elgbl_cntct,var=elgbl_cnty_cd,
								condition=((%nmisslogic(elgbl_cnty_cd,3)=1) and elgbl_adr_type_cd = '01'));
								
%_build_index_measure_tables(measure=el105,lbl=el1.5,input_dsn=tmsis_elgbl_cntct,var=elgbl_zip_cd,
								condition=((%nmisslogic(elgbl_zip_cd,9)=1) and elgbl_adr_type_cd = '01'));	

%_build_index_measure_tables(measure=el107,lbl=el1.7,input_dsn=tmsis_race_info,var=race_cd,
								condition=(race_cd in %str(('001','002','003','004','005','006','007','008','009','010',
														'011','012','013','014','015','016','017','999')) 
														and (%nmisslogic(race_cd,3)=1)));	
%_build_index_measure_tables(measure=el108,lbl=el1.8,input_dsn=tmsis_ethncty_info,var=ethncty_cd,
								condition=(ethncty_cd in %str(('0','1','2','3','4','5','6','9'))
														 and (%nmisslogic(ethncty_cd,1) = 1)));

%_build_index_measure_tables(measure=el1001,lbl=el10.1,input_dsn=tmsis_mc_prtcptn_data,var=enrld_mc_plan_type_cd,
								condition=(enrld_mc_plan_type_cd in %str(('00','01','02','03','04','05','06','07','08','09','10',
                                   '11','12','13','14','15','16','17','18','60','70','80','99')) and (%nmisslogic(enrld_mc_plan_type_cd,2)=1)));


  /*extract measures from AREMAC into sas*/
    create table el_index as
	 select * from connection to tmsis_passthrough
	( 
		select * from &wrktable..&taskprefix._el104
		union all
		select * from &wrktable..&taskprefix._el105
		union all
		select * from &wrktable..&taskprefix._el107
		union all
		select * from &wrktable..&taskprefix._el108
		union all
		select * from &wrktable..&taskprefix._el1001
	);
                    
	%dropwrktables(el104 el105 el107 el108 el1001);

%tmsis_disconnect;
quit;
%status_check;

%mend _103;
