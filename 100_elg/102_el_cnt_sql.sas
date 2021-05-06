/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 102_el_cnt_sql.sas
 Project: MACBIS Task 2

 Author:  Sabitha Gopalsamy
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: All count measures combined in el_cnt SAS work dataset
         
 Modifications: 9/5 : SGO added a new measure #EL3.13 for V1.2 update
				12/11: SGO added a new measure SUMEL1.3 for V1.3 update
				1/18/17 : SGO Added Emptytable macros 
				10/18/18: KNH Added measure EL6.23 for v1.5 update
 
 ******************************************************************************************/

/*COUNT*/

%macro _102;

%let tblList = el112 el301 el303 el501 el502 sumel01 el605 el606 el607 el608 el609 el610 sumel02 el612 el613 el614 el615 
				el616 el617 el618 el619 el620 el621 el1002 el611t el611 el313 sumel03 el623
				el327 el328 el329 el330 /*el151 el152*/
;

proc sql;
	%tmsis_connect;

	%dropwrktables(&tblList);

%macro _build_count_measure_tables(measure=,tab=,measure_lbl=,input_dsn=, condition=);

execute(
		create table &wrktable..&taskprefix._&measure. as
		select %if &measure = el313 or &measure = el327 or &measure = el328 or &measure = el329 or &measure = el330 %then %do ;
					count(distinct elgblty_grp_cd) as pct,
				%end ;
				%else %do ;
					count(distinct msis_ident_num) as pct,
				%end ;
				%str(%')&state.%str(%') as submtg_state_cd,
				%str(%')&measure_lbl. %str(%') as measure
		from &temptable..&taskprefix._&input_dsn.
		where &condition.
		)by tmsis_passthrough;

%mend _build_count_measure_tables;

%_build_count_measure_tables(measure=el112,tab=1,measure_lbl=el1.12,
							input_dsn=tmsis_var_dmgrphc_elgblty,condition=(ctznshp_ind='1'));

%_build_count_measure_tables(measure=el301,tab=3,measure_lbl=el3.1,
							input_dsn=tmsis_elgblty_dtrmnt,condition=elgblty_grp_cd in %str(('01','02','03','04','05','06','07','08','09','72','73','74','75','11','12','13',
    '14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34',
    '35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55',
    '56','59','60','61','62','63','64','65','66','67','68','69','70','71','76')));

%_build_count_measure_tables(measure=el303,tab=3,measure_lbl=el3.3,
							input_dsn=tmsis_dsblty_info,condition=(dsblty_type_cd in %str(('01','02','03','04','05','06','07'))));

%_build_count_measure_tables(measure=el501,tab=5,measure_lbl=el5.1,
							input_dsn=tmsis_var_dmgrphc_elgblty,condition=(chip_cd='2'));

%_build_count_measure_tables(measure=el502,tab=5,measure_lbl=el5.2,
							input_dsn=tmsis_var_dmgrphc_elgblty,condition=(chip_cd='3'));

%_build_count_measure_tables(measure=sumel03,tab=12,measure_lbl=sumel.3,
							input_dsn=tmsis_var_dmgrphc_elgblty,condition=(chip_cd in %str(('2','3'))));

%_build_count_measure_tables(measure=sumel01,tab=12,measure_lbl=sumel.1,
							input_dsn=tmsis_prmry_dmgrphc_elgblty,condition=%str(1=1));

%_build_count_measure_tables(measure=el605,tab=6,measure_lbl=el6.5,
							input_dsn=tmsis_hh_sntrn_prtcptn_info,condition=(hh_sntrn_prtcptn_efctv_dt is not null));

%_build_count_measure_tables(measure=el606,tab=6,measure_lbl=el6.6,
							input_dsn=tmsis_hh_chrnc_cond,condition=(hh_chrnc_cd in %str(('A','B','C','D','E','F','G','H'))));

%_build_count_measure_tables(measure=el607,tab=6,measure_lbl=el6.7,
							input_dsn=tmsis_hcbs_chrnc_cond_non_hh,condition=(ndc_uom_chrnc_non_hh_cd in %str(('001','002','003','004','005','006','007','008','009','010'))));

%_build_count_measure_tables(measure=el608,tab=6,measure_lbl=el6.8,
							input_dsn=tmsis_lckin_info,condition=(lckin_efctv_dt is not null));

%_build_count_measure_tables(measure=el609,tab=6,measure_lbl=el6.9,
							input_dsn=tmsis_ltss_prtcptn_data,condition=(ltss_elgblty_efctv_dt is not null));

%_build_count_measure_tables(measure=el610,tab=6,measure_lbl=el6.10,
							input_dsn=tmsis_mfp_info,condition=(mfp_enrlmt_efctv_dt is not null));

%_build_count_measure_tables(measure=sumel02,tab=12,measure_lbl=sumel.2,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd in %str(('01','02','03','04','05','06','08','09','10'))));

%_build_count_measure_tables(measure=el612,tab=6,measure_lbl=el6.12,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '01'));

%_build_count_measure_tables(measure=el613,tab=6,measure_lbl=el6.13,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '02'));

%_build_count_measure_tables(measure=el614,tab=6,measure_lbl=el6.14,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '03'));

%_build_count_measure_tables(measure=el615,tab=6,measure_lbl=el6.15,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '04'));

%_build_count_measure_tables(measure=el616,tab=6,measure_lbl=el6.16,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '05'));

%_build_count_measure_tables(measure=el617,tab=6,measure_lbl=el6.17,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '06'));

%_build_count_measure_tables(measure=el618,tab=6,measure_lbl=el6.18,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '08'));

%_build_count_measure_tables(measure=el619,tab=6,measure_lbl=el6.19,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '09'));


%_build_count_measure_tables(measure=el620,tab=6,measure_lbl=el6.20,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(dual_elgbl_cd = '10'));

%_build_count_measure_tables(measure=el621,tab=6,measure_lbl=el6.21,
							input_dsn=tmsis_sect_1115a_demo_info,condition=(sect_1115a_demo_ind = '1'));

%_build_count_measure_tables(measure=el1002,tab=10,measure_lbl=el10.2,
							input_dsn=tmsis_mc_prtcptn_data,condition=(enrld_mc_plan_type_cd in %str(('01','02','03','04','05','06','07','08','09','10',
                                   '11','12','13','14','15','16','17','18','60','70','80'))));
 
/* sgo added el3.13 for v1.2 updates */
%_build_count_measure_tables(measure=el313,tab=3,measure_lbl=el3.13,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(elgblty_grp_cd in %str(('01','02','03','04','05','06','07','08',
	'09','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26'))));

/*khourihan added el6.23 for v1.5 updates*/
%_build_count_measure_tables(measure=el623,tab=6,measure_lbl=el6.23,input_dsn=tmsis_elgblty_dtrmnt,
							condition=(rstrctd_bnfts_cd in %str(('1','7','A','B','D')) or rstrctd_bnfts_cd is null));

/* jvs added el3.27-el3.30 for v2.1 updates */
%_build_count_measure_tables(measure=el327,tab=3,measure_lbl=el3.27,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(elgblty_grp_cd in %str(('01', '05', '06', '07', '08', '09'))));
%_build_count_measure_tables(measure=el328,tab=3,measure_lbl=el3.28,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(elgblty_grp_cd in %str(('02', '03'))));
%_build_count_measure_tables(measure=el329,tab=3,measure_lbl=el3.29,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(elgblty_grp_cd in %str(('23', '24', '25', '26'))));
%_build_count_measure_tables(measure=el330,tab=3,measure_lbl=el3.30,
							input_dsn=tmsis_elgblty_dtrmnt,condition=(elgblty_grp_cd in %str(('11', '12'))));
  
*el6.11;
execute(
		create table &wrktable..&taskprefix._el611t as
		select count(distinct a.msis_ident_num) as pct
		from (select distinct msis_ident_num from &temptable..&taskprefix._tmsis_mfp_info )a
		inner join
		(select distinct msis_ident_num from &temptable..&taskprefix._tmsis_elgblty_dtrmnt where rstrctd_bnfts_cd = 'D' )b
		on a.msis_ident_num=b.msis_ident_num
	   	where a.msis_ident_num is not null
		and b.msis_ident_num is not null
		)by tmsis_passthrough;

 execute( create table &wrktable..&taskprefix._el611 as
		select pct,
			   %str(%')&state.%str(%') as submtg_state_cd,
			   %str(%')el6.11 %str(%') as measure
		from &wrktable..&taskprefix._el611t
		)by tmsis_passthrough;
        
%emptytable(el611);  

/*************************************************************************************************/
* Calculate EL15.1 and EL15.2 - comparing the TMSIS value to Performance Indicator Benchmark data;
* PI data is processed in the Universal macro.                                                    ;
* The output for these measures mimic the output for pct rather than output for count.            ;
* The output files are stacked in the 100_elg_driver program to craete elg_100 file               ;
/*************************************************************************************************/

*el 15.1;

/*
execute(
		create table &wrktable..&taskprefix._el151 as
		select  a.submtg_state_cd
		       ,(a.cnt_EL623 - b.PI_15_1) as numerator
			   ,b.PI_15_1 as denominator
			   ,case when b.PI_15_1 > 0 then round(((a.cnt_EL623 - b.PI_15_1)/b.PI_15_1),2) 
                     else null end as pct
			   ,'el15.1' as measure
       
		from  
             (select submtg_state_cd
		             ,pct as cnt_EL623
              from &wrktable..&taskprefix._el623) a
		left join 

             (select submtg_state_cd
                     ,coalesce(enrlmt_mdcd_tot_cnt,0) + 
					  coalesce(enrlmt_chip_tot_cnt,0) as PI_15_1
              from &temptable..&taskprefix._perfom_ind) b

		on a.submtg_state_cd=b.submtg_state_cd
	   	
		)by tmsis_passthrough;

%emptytable(el151);  
*/
/**Take the output to the 100_elg_driver and stack with other files **/
/**Not inlcuded in el_cnt below                                     **/
/*create table el_pct_15_1 as
select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._el151);

*/

*el 15.2;
/*
execute(
		create table &wrktable..&taskprefix._el152 as
		select  a.submtg_state_cd
		       ,(a.cnt_sumel03 - b.PI_15_2) as numerator
			   ,b.PI_15_2 as denominator
			   ,case when b.PI_15_2 > 0 then round(((a.cnt_sumel03 - b.PI_15_2)/b.PI_15_2),2) 
                     else null end as pct
			   ,'el15.2' as measure
       
		from  
             (select submtg_state_cd
		             ,pct as cnt_sumel03
              from &wrktable..&taskprefix._sumel03) a
		left join 

             (select submtg_state_cd
                     ,coalesce(enrlmt_chip_tot_cnt,0) as PI_15_2
              from &temptable..&taskprefix._perfom_ind) b

		on a.submtg_state_cd=b.submtg_state_cd
	   	
		)by tmsis_passthrough;

%emptytable(el152); 
*/
/**Take the output to the 100_elg_driver and stack with other files **/
/**Not inlcuded in el_cnt below                                     **/
/*
create table el_pct_15_2 as
select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._el152);


*/

create table el_cnt as
select * from connection to tmsis_passthrough
(
	select * from &wrktable..&taskprefix._el112
	union all	
	select * from &wrktable..&taskprefix._el301
	union all
	select * from &wrktable..&taskprefix._el303
	union all
	select * from &wrktable..&taskprefix._el501
	union all
	select * from &wrktable..&taskprefix._el502
	union all
	select * from &wrktable..&taskprefix._sumel01
	union all
	select * from &wrktable..&taskprefix._el605
	union all
	select * from &wrktable..&taskprefix._el606
	union all
	select * from &wrktable..&taskprefix._el607
	union all
	select * from &wrktable..&taskprefix._el608
	union all
	select * from &wrktable..&taskprefix._el609
	union all
	select * from &wrktable..&taskprefix._el610
	union all
	select * from &wrktable..&taskprefix._sumel02
	union all
	select * from &wrktable..&taskprefix._el612
	union all
	select * from &wrktable..&taskprefix._el613
	union all
	select * from &wrktable..&taskprefix._el614
	union all
	select * from &wrktable..&taskprefix._el615
	union all
	select * from &wrktable..&taskprefix._el616
	union all
	select * from &wrktable..&taskprefix._el617
	union all
	select * from &wrktable..&taskprefix._el618
	union all
	select * from &wrktable..&taskprefix._el619
	union all
	select * from &wrktable..&taskprefix._el620
	union all
	select * from &wrktable..&taskprefix._el621
	union all
	select * from &wrktable..&taskprefix._el1002
	union all
	select * from &wrktable..&taskprefix._el611
	union all
	select * from &wrktable..&taskprefix._el313 /* sgo added el3.13 for v1.2 updates */
	union all
	select * from &wrktable..&taskprefix._sumel03 /* sgo added sumel03 for v1.3 updates */
	union all
	select * from &wrktable..&taskprefix._el623 /* knh added el6.23 for v1.5 updates */
	/* jvs added el327-el330 for v2.1 updates */
	union all
	select * from &wrktable..&taskprefix._el327
	union all
	select * from &wrktable..&taskprefix._el328
	union all
	select * from &wrktable..&taskprefix._el329
	union all
	select * from &wrktable..&taskprefix._el330
);

%dropwrktables(&tblList);


%tmsis_disconnect;
quit;
%status_check;

%mend _102;

