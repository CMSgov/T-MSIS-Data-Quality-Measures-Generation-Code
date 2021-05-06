/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 104_el_freq_sql.sas
 Project: MACBIS Task 2

 Author:  Sabitha Gopalsamy
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: All frequency measures combined in el_freq SAS work dataset
         
 Modifications: 9/5 : SGO commented  #EL7.1,#EL9.1 measures and added a new measure
					  #EL12.1 for V1.2 updates
				12/26 : SGO added rows 'A', 'N', and 'T' in all FREQ measures for V1.3 updates
				1/18/17 : SGO Added Emptytable macros
 
 ******************************************************************************************/

/*frequency*/

%macro _104;

proc sql;
%tmsis_connect;

%macro _build_freq_measure_tables(measure=,lbl=,input_dsn=,var=,vvalue=,condition=);

%dropwrkviews(&measure._tmp &measure._tmp1);
%dropwrktables(&measure.);

execute(
	   create or replace view &wrktable..&taskprefix._&measure._tmp as select  
			  coalesce(&var.,char(46)) as valid_value,
			  count(distinct msis_ident_num) as pct
		from &temptable..&taskprefix._&input_dsn. 
		where &condition.
		group by &var.
		)by tmsis_passthrough;

execute(
	   create or replace view &wrktable..&taskprefix._&measure._tmp1 as   

		select valid_value, pct 
		from  &wrktable..&taskprefix._&measure._tmp

	  	union all

		%if &lbl. = el6.24 %then
			select 'T_' as valid_value,
		; %else
	   		select 'T' as valid_value,
		;
	   		count(distinct msis_ident_num) as pct
		from &temptable..&taskprefix._&input_dsn.
		where &condition.

		union all

		%if &lbl. = el6.24 %then
			select 'A_' as valid_value,
		; %else
			select 'A' as valid_value,
		;
	   		count(distinct case when valid_value in (&vvalue.) then msis_ident_num else null end) as pct
		from (select *, coalesce(&var.,char(46)) as valid_value  from &temptable..&taskprefix._&input_dsn.) a
		where &condition.

		union all

		%if &lbl. = el6.24 %then
			select 'N_' as valid_value,
		; %else
			select 'N' as valid_value,
		;
	   		count(distinct case when ((valid_value not in (&vvalue.)) or (valid_value is null)) then msis_ident_num else null end) as pct
		from (select *, coalesce(&var.,char(46)) as valid_value from &temptable..&taskprefix._&input_dsn.) b
		where &condition.

		)by tmsis_passthrough;

  execute(
	   create table &wrktable..&taskprefix._&measure. as   
	   select %str(%')&state.%str(%') as submtg_state_cd,
			  %str(%')&lbl.%str(%') as measure,
			   valid_value,
			   pct
		from &wrktable..&taskprefix._&measure._tmp1
		where valid_value in (&vvalue.) or valid_value in ('A','N','T','A_','N_','T_')
	)by tmsis_passthrough;
    
    %emptytable(&measure.);  
  
	%dropwrkviews(&measure._tmp &measure._tmp1);

%mend _build_freq_measure_tables;

%_build_freq_measure_tables(measure=el201,lbl=el2.1,input_dsn=tmsis_var_dmgrphc_elgblty,
							 var = imgrtn_stus_cd,vvalue = %str('1','2','3','8','9'),condition=%str(1=1));

*el4.1;
%droptemptables(el401a);

execute(
		create table &temptable..&taskprefix._el401a as
	    select a.enrlmt_type_cd,
		       coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num
	    from &temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data as a 
		inner join &temptable..&taskprefix._tmsis_var_dmgrphc_elgblty as b
	    on a.msis_ident_num=b.msis_ident_num
	    where b.chip_cd='2'
		)by tmsis_passthrough;


%_build_freq_measure_tables(measure=el401,lbl=el4.1,input_dsn=el401a,var=enrlmt_type_cd,
								vvalue = %str('1','2','9'),condition=%str(1=1));


*el12.1;/* sgo added this measure for v1.2 update */
%_build_freq_measure_tables(measure=el1201,lbl=el12.1,input_dsn=tmsis_elgblty_dtrmnt,
							 var = elgblty_grp_cd,vvalue = %str('01','02','03','04','05','06','07','08','09',
							 '11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26',
							 '27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42',
							 '43','44','45','46','47','48','49','50','51','52','53','54','55','56',/*'57','58',*/
							 '59','60','61','62','63','64','65','66','67','68','69','70','71','72','73','74',
							 '75','76'),condition=%str(1=1));

*el6.24; /*knh added for v1.5 update*/
%_build_freq_measure_tables(measure=el624,lbl=el6.24,input_dsn=tmsis_elgblty_dtrmnt,var=rstrctd_bnfts_cd,
							vvalue=%str('1','2','3','4','5','6','7','A','B','C','D','F'),condition=%str(1=1));


create table el_freq as
select * from connection to tmsis_passthrough
(
	select * from &wrktable..&taskprefix._el201
	union all
	select * from &wrktable..&taskprefix._el401
	union all
	select * from &wrktable..&taskprefix._el1201
	union all
	select * from &wrktable..&taskprefix._el624
);

%dropwrktables(el201 el401 el1201 el624);
%droptemptables(el401a);

%tmsis_disconnect;
quit;
%status_check;

%mend _104;

