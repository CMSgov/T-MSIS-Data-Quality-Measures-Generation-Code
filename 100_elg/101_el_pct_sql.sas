/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 101_el_pct_sql.sas
 Project: MACBIS task 2

 Author:  Sabitha Gopalsamy
  
 Input: temporary tables created in universal_macros.sas
 
 Output: All percentage measures combined in el_pct Sas work dataset
         
 Modifications: 9/5 : SGO updated the measures #el1.1,#el1.2,#el1.9,#el1.10,#el1.16 and added 
					   new measures #el3.12 and #el3.14 for V1.2 update
                9/22: SVerghese: added %misslogic(elgblty_grp_cd,2) for el3.12;
				10/9 : SGO Fixed the issue with #el3.14 and updated #el1.9 measures
				12/6/17 : V1.3 updates
						  - SGO commented out %misslogic(race_cd,3) in #el1.10 
						  - SGO added new measures #el3.15, #el3.16 and #el1.20
				1/18/17 : SGO Added Emptytable macros 
                5/1/2017:SVerghese: Changed tmsis_tpl_mdcd_prsn_mn to tmsis_tpl_prsn_mn in el11.1
				10/17/18:KNH removed age 65 from el3.9 for V1.5 update
                1/22/2019: JVSmith: V1.6 update (look for here!)
 ******************************************************************************************/

*el16;
%macro el16(tbl, msr);
    execute(
        create table &wrktable..&taskprefix._el&msr.t as
        select  %str(%')&state.%str(%') as submtg_state_cd,
        sum(denom) as denom,
        sum(numer) as numer,
        case when sum(denom) > 0 then
        round((sum(numer) /sum(denom)),3)
        else null end as pct
        from
        (
        select
        msis_id as msis_ident_num,
        %misslogic(msis_id, 20) as numer,
        1 as denom
        from &permview..&tbl._view
		where %run_id_filter
        ) a
        ) by tmsis_passthrough;
    %mend;


%macro nonclaimspct(msr=,numer=,denom=,tbl=,round=);
	
	execute(
		create table &wrktable..&taskprefix._&msr. as
		select
			 %str(%')&state.%str(%') as submtg_state_cd
			,sum(numer) as numer
			,count(1) as denom
			,round(sum(numer) / count(1),&round.) as pct
		from
		(
			select 
				 msis_ident_num
				,max(&numer.) as numer
			from &tbl.
			where &denom.
			group by msis_ident_num
		) a
	) by tmsis_passthrough;

%mend;

%macro nonclaimspct2tbl(msr=,numer=,numertbl=,denom=,denomtbl=,round=);


	execute(
		create table &wrktable..&taskprefix._&msr. as 
		select 
			 %str(%')&state.%str(%') as submtg_state_cd
			,sum(numer) as numer
			,count(1) as denom
			,round(sum(numer) / count(1),&round.) as pct
        from 
		(
			select 
				a.msis_ident_num
			   ,max(&numer.) as numer
	    	from &denomtbl. a
			left join &numertbl. b
			on a.msis_ident_num = b.msis_ident_num
			where &denom.
			group by a.msis_ident_num
		) c
	)by tmsis_passthrough;

%mend;

%macro nonclaimspct2tblwvr(msr=,numer=,numertbl=,denom=,denomtbl=,round=);


	execute(
		create table &wrktable..&taskprefix._&msr. as 
		select 
			 %str(%')&state.%str(%') as submtg_state_cd
			,sum(numer) as numer
			,count(1) as denom
			,round(sum(numer) / count(1),&round.) as pct
        from 
		(
			select 
			&numer. as numer
	    	from &denomtbl. a
			left join &numertbl. b
			on a.msis_ident_num = b.msis_ident_num
			where &denom.
		) c
	)by tmsis_passthrough;

%mend;

%macro nonclaimspctwvr(msr=,numer=,denom=,tbl=,round=);
	
	execute(
		create table &wrktable..&taskprefix._&msr. as
		select
			 %str(%')&state.%str(%') as submtg_state_cd
			,sum(numer) as numer
			,count(1) as denom
			,round(sum(numer) / count(1),&round.) as pct
		from
		(
			select 
				 &numer. as numer
			from &tbl.
			where &denom.
		) a
	) by tmsis_passthrough;

%mend;

%macro nonclaimspct_notany(msr=,numer=,denom=,tbl=,round=);
	
	execute(
		create table &wrktable..&taskprefix._&msr. as
		select
			 %str(%')&state.%str(%') as submtg_state_cd
			,sum(case when numer_max = 0 then 1 else 0 end) as numer
			,count(1) as denom
			,round(sum(case when numer_max = 0 then 1 else 0 end) / count(1),&round.) as pct
		from
		(
			select 
				 msis_ident_num
				,max(&numer.) as numer_max
			from &tbl.
			where &denom.
			group by msis_ident_num
		) a
	) by tmsis_passthrough;

%mend;

%macro _101;

%let tbllist = el1005t el1006t el1007t el1008t el101t el102t el106t el109t el1101t el110t el111t el113t el114t el115t el116t 
		       el117t el118t el119t el121t el1601t el1602t el1603t el1604t el1605t el1606t el1607t el1608t el1609t 
               el302t el304t el305t el306t el307t el308t el309t el310t el311t el312t el315t el316t el317t el318t el622t el625t
			   el319t el320t el321t el322t el323t el324t el626t el627t
			   el1701t el1702t el1703t el325t el326t el628t el629t 
			   el631t el632t el125t el126t el127t el331t el332t el630t el633t el634t el122t el123t el124t el128t
			   el12x
;

proc sql;
%tmsis_connect;

%dropwrktables(&tbllist);

%nonclaimspct(msr=el101t,
				numer=case when (%ssn_nmisslogic(ssn_num,9) = 1) and (%nmisslogic(msis_ident_num,20) = 1) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
				round=2);

%nonclaimspct(msr=el102t,
				numer=case when (%ssn_nmisslogic(ssn_num,9) = 1) and ssn_vrfctn_ind='1' then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
				round=2);

%nonclaimspct(msr=el106t,
				numer=case when gndr_cd='F' then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
				round=2);

%nonclaimspct(msr=el109t,
				numer=case when (ethncty_cd not in %str(('0','1','2','3','4','5')) or ethncty_cd is null) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_ethncty_info,
				round=2);

%nonclaimspct(msr=el110t,
				numer=case when (race_cd not in %str(('001','002','003','004','005','006','007',
											'008','009','010','011','012','013','014',
											'015','016')) or race_cd is null) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_race_info,
				round=2);

%nonclaimspct(msr=el111t,
				numer=case when race_cd='003' then 1 else 0 end,
				denom=crtfd_amrcn_indn_alskn_ntv_ind='2',
				tbl=&temptable..&taskprefix._tmsis_race_info,
				round=2);
 			
%nonclaimspct(msr=el113t,
				numer=case when (ctznshp_ind='1') and (ctznshp_vrfctn_ind = '1') then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
				round=2);

%nonclaimspct(msr=el114t,
				numer=case when (imgrtn_vrfctn_ind='1') then 1 else 0 end,
				denom=imgrtn_stus_cd in %str(('1','2','3')),
				tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
				round=2);

%nonclaimspct(msr=el115t,
				numer=case when death_dt >= &m_start. and death_dt <= &m_end. then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._elig_in_month_prmry,
				round=4);

%nonclaimspct(msr=el116t,
				numer=%nmisslogic(msis_case_num,12),
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

%nonclaimspct(msr=el117t,
				numer=case when age = 0 then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
				round=4);

%nonclaimspct(msr=el119t,
				numer=case when age >=65 then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
				round=4);

%nonclaimspct(msr=el118t,
				numer=case when age >=0 and age <= 20 then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
				round=2);

*measures using 2 tables;
%nonclaimspct2tbl(msr=el302t,
					numer=case when enrld_mc_plan_type_cd='01' then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_mc_prtcptn_data,
					denom=age >= 65,
					denomtbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty
					,round=2);

%nonclaimspct2tbl(msr=el304t,
					numer=case when age >= 13 and age <= 64 then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					denom=elgblty_grp_cd in %str(('05','53','67','68')),
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el305t,
					numer=case when age < 26 then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					denom=elgblty_grp_cd in %str(('08','09','30')),
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el306t,
					numer=case when age >= 16 and age <= 64 then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					denom=elgblty_grp_cd in %str(('48','49')),
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

*back to one table;
%nonclaimspct(msr=el307t,
				numer=case when dual_elgbl_cd in %str(('01','02','03','04','05','06','08','09','10')) then 1 else 0 end,
				denom=elgblty_grp_cd in %str(('23','24','25','26')) and msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

%nonclaimspct(msr=el308t,
				numer=case when elgblty_grp_cd in %str(('08','09','30')) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

 *two tables;
%nonclaimspct2tbl(msr=el309t,
					numer=case when age>=16 and age< 65 then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					denom=elgblty_grp_cd = '34',
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el310t,
					numer=case when gndr_cd = 'F' then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					denom=elgblty_grp_cd = '34',
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el311t,
					numer=case when dual_elgbl_cd in %str(('01','02','03','04','05','06','08','09','10')) then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,					
					denom=age >= 65,
					denomtbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
					round=2);

*one table;
%nonclaimspct(msr=el312t,
				numer=case when (%misslogic(elgblty_grp_cd,2) = 1) or elgblty_grp_cd not in 
										 %str(('01','02','03','04','05','06','07','08','09',
												 '11','12','13','14','15','16','17','18','19','20',
												 '21','22','23','24','25','26','27','28','29','30',
											     '31','32','33','34','35','36','37','38','39','40',
											     '41','42','43','44','45','46','47','48','49','50',
												 '51','52','53','54','55','56','59','60',
												 '61','62','63','64','65','66','67','68','69','70',
											     '71','72','73','74','75','76')) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

%nonclaimspct(msr=el315t,
				numer=case when dual_elgbl_cd in %str(('01','02','03','04','05','06','08','09','10')) then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

*two tbles;
%nonclaimspct2tbl(msr=el316t,
					numer=case when b.enrlmt_type_cd='2' then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data,
					denom=chip_cd = '2',
					denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
					round=2);

%nonclaimspct2tbl(msr=el1006t,
				  numer=case when enrld_mc_plan_type_cd='01' then 1 else 0 end,
				  numertbl=&temptable..&taskprefix._tmsis_mc_prtcptn_data,
				  denom=chip_cd in %str(('2','3')),
				  denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
				  round=2);

%nonclaimspct2tbl(msr=el1101t,
					numer=case when tpl_insrnc_cvrg_ind='1' then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_tpl_mdcd_prsn_mn,
					denom=dual_elgbl_cd in %str(('02','04','08')),
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el1005t,
					numer=case when enrld_mc_plan_type_cd='01' then 1 else 0 end,	
					numertbl=&temptable..&taskprefix._tmsis_mc_prtcptn_data,
					denom=rstrctd_bnfts_cd in %str(('2','3','4','5','6')),
					denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					round=2);

%nonclaimspct2tbl(msr=el622t,
					numer=case when rstrctd_bnfts_cd='6' then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					denom=wvr_type_cd='24',
					denomtbl=&temptable..&taskprefix._tmsis_wvr_prtcptn_data,
					round=2);

* one table;					
%nonclaimspct(msr=el1008t,
				numer=%misslogic(enrld_mc_plan_type_cd,2),
				denom=(%nmisslogic(mc_plan_id, 12) = 1),
				tbl=&temptable..&taskprefix._tmsis_mc_prtcptn_data,
				round=2);

%nonclaimspct(msr=el1007t,
				numer=%misslogic(mc_plan_id, 12),
				denom=enrld_mc_plan_type_cd in 
			            %str(('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', 
			              	    '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', 
			             	    '60', '70', '80')),
				tbl=&temptable..&taskprefix._tmsis_mc_prtcptn_data,
				round=2);

%nonclaimspct(msr=el121t,
				numer=case when age > 120 or age < -1 then 1 else 0 end,
				denom=msis_ident_num is not null,
				tbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty,
				round=3);

*two tables;
%nonclaimspct2tbl(msr=el317t,
					numer=case when (elgblty_grp_cd is null or elgblty_grp_cd not in %str(('31', '61', '62', '63', '64', '65', '66', '67', '68'))) 
						then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					denom=chip_cd in %str(('2', '3')),
					denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
					round=3);

%nonclaimspct2tbl(msr=el318t,
					numer=case when (elgblty_grp_cd in %str(('61', '62', '63', '64', '65', '66', '67', '68'))) then 1 else 0 end,
					numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
					denom=chip_cd = '1',
					denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
					round=3); 
*one table;
%nonclaimspct(msr=el625t,
				numer=case when elgblty_grp_cd is null or elgblty_grp_cd not in %str(('35', '70')) then 1 else 0 end,
				denom=rstrctd_bnfts_cd = '6',
				tbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
				round=2);

%el16(tmsis_prmry_dmgrphc_elgblty, 1601);
%el16(tmsis_var_dmgrphc_elgblty, 1602);
%el16(tmsis_elgbl_cntct, 1603);
%el16(tmsis_elgblty_dtrmnt, 1604);
%el16(tmsis_wvr_prtcptn_data, 1605);
%el16(tmsis_mc_prtcptn_data, 1606);
%el16(tmsis_ethncty_info, 1607);
%el16(tmsis_race_info, 1608);
%el16(tmsis_enrlmt_time_sgmt_data, 1609);

* v1.7;
title "creating fmg state code lists";
    create table fmg as
        select 
        quote(put(stfips(put(trim(state), $stabr.)), z2.), "'") as submtg_state_cd
        , case upcase('el group 72'n) when 'YES' then 1 else 0 end as _72
        , case upcase('el group 73'n) when 'YES' then 1 else 0 end as _73
        , case upcase('el group 74'n) when 'YES' then 1 else 0 end as _74
        , case upcase('el group 75'n) when 'YES' then 1 else 0 end as _75
        from fmg.'eligibilitygroups'n
        ;
    select submtg_state_cd into :el_grp_72 separated by ', ' from fmg where _72=1;
    select submtg_state_cd into :el_grp_73 separated by ', ' from fmg where _73=1;
    select submtg_state_cd into :el_grp_74_75 separated by ', ' from fmg where _74=1 or _75=1;
title;
    execute (
        create table &wrktable..&taskprefix._el319t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
	        select msis_ident_num
		        , max(case when submtg_state_cd in (&el_grp_72) then 1 else 0 end) as denom_val
		        , max(case when submtg_state_cd in (&el_grp_72)
		        		and elgblty_grp_cd='72' then 1 else 0 end) as numer_val
	        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
	        where msis_ident_num is not null
	        group by msis_ident_num
	        ) c
        ) by tmsis_passthrough;

    execute (
        create table &wrktable..&taskprefix._el320t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when submtg_state_cd in (&el_grp_73) then 1 else 0 end) as denom_val
        , max(case when  submtg_state_cd in (&el_grp_73) and elgblty_grp_cd='73' then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) c
        ) by tmsis_passthrough;

    execute (
        create table &wrktable..&taskprefix._el321t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when submtg_state_cd in (&el_grp_74_75) then 1 else 0 end) as denom_val
        , max(case when submtg_state_cd in (&el_grp_74_75) and elgblty_grp_cd in ('74', '75') then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) c
        ) by tmsis_passthrough;

    execute (
        create table &wrktable..&taskprefix._el322t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when submtg_state_cd not in (&el_grp_72) then 1 else 0 end) as denom_val
        , max(case when submtg_state_cd not in (&el_grp_72)
        and elgblty_grp_cd='72' then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) c
        ) by tmsis_passthrough;

    execute (
        create table &wrktable..&taskprefix._el323t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when submtg_state_cd not in (&el_grp_73) then 1 else 0 end) as denom_val
        , max(case when  submtg_state_cd not in (&el_grp_73) and elgblty_grp_cd='73' then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) c
        ) by tmsis_passthrough;

    execute (
        create table &wrktable..&taskprefix._el324t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when submtg_state_cd not in (&el_grp_74_75) then 1 else 0 end) as denom_val
        , max(case when submtg_state_cd not in (&el_grp_74_75) and elgblty_grp_cd in ('74', '75') then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) c
        ) by tmsis_passthrough;

	
    execute (
        create table &wrktable..&taskprefix._el626t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when rstrctd_bnfts_cd = '3' then 1 else 0 end) as denom_val
        , max(case when rstrctd_bnfts_cd = '3' and
        (dual_elgbl_cd not in ('01', '03', '05', '06') or dual_elgbl_cd is null)
        then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) a
        ) by tmsis_passthrough;

	
    execute (
        create table &wrktable..&taskprefix._el627t as
        select %str(%')&state.%str(%') as submtg_state_cd
        , sum(denom_val) as denom
        , sum(numer_val) as numer
        , case when sum(denom_val) > 0 then
        round(sum(numer_val) / sum(denom_val), 3)
        else null end as pct
        from (
        select msis_ident_num
        , max(case when dual_elgbl_cd in ('01', '03', '05', '06') then 1 else 0 end) as denom_val
        , max(case when dual_elgbl_cd in ('01', '03', '05', '06') and (rstrctd_bnfts_cd <> '3' or rstrctd_bnfts_cd is null)
        then 1 else 0 end) as numer_val
        from &temptable..&taskprefix._tmsis_elgblty_dtrmnt
        where msis_ident_num is not null
        group by msis_ident_num
        ) a
        ) by tmsis_passthrough;
        
* v2.1;
        %nonclaimspct2tbl(msr=el1701t
            , numer=%str(case when b.msis_id is null then 1 else 0 end)
            , numertbl=&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty
            , denom=%str(a.msis_ident_num is not null)
            , denomtbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data
            , round=3);
        %nonclaimspct2tbl(msr=el1702t
            , numer=%str(case when b.msis_id is null then 1 else 0 end)
            , numertbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty
            , denom=%str(a.msis_ident_num is not null)
            , denomtbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data
            , round=3);
        %nonclaimspct2tbl(msr=el1703t
            , numer=%str(case when b.msis_id is null then 1 else 0 end)
            , numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt
            , denom=%str(a.msis_ident_num is not null)
            , denomtbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data
            , round=3);
        %nonclaimspct2tbl(msr=el325t
           , numer=%str(case when elgblty_grp_cd is null or elgblty_grp_cd not in ('07', '31', '61') then 1 else 0 end)
           , numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt
           , denom=%str(chip_cd = '2')
           , denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty
           , round=3);
       %nonclaimspct2tbl(msr=el326t
           , numer=%str(case when elgblty_grp_cd is null or elgblty_grp_cd not in ('61', '62', '63', '64', '65', '66', '67', '68') then 1 else 0 end)
           , numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt
           , denom=%str(chip_cd = '3')
           , denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty
           , round=3);
       %nonclaimspctwvr(msr=el628t
           , numer=%str(case when substring(wvr_id, 1, 5) not in ('11-W-', '21-W-') or concat(substring(wvr_id, 6, 5), substring(wvr_id, 12)) not rlike '^[0-9]+$' or substring(wvr_id, 11, 1) <> '/' or wvr_id is null then 1 else 0 end)
           , denom=%str(wvr_type_cd in ('01', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30'))
           , tbl=&temptable..&taskprefix._tmsis_wvr_prtcptn_data
           , round=3);
       %nonclaimspctwvr(msr=el629t
           , numer=%str(case when substring(wvr_id, 1, 1) not rlike '[A-Za-z]' or substring(wvr_id, 3, 1) <> '.' or substring(wvr_id, 4) not rlike '^[0-9]+$' or wvr_id is null then 1 else 0 end)
           , denom=%str(wvr_type_cd in ('02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '32', '33'))
           , tbl=&temptable..&taskprefix._tmsis_wvr_prtcptn_data
           , round=3);

* v2.2;
%nonclaimspct2tbl(msr=el630t,
    numer=case when b.chip_cd <> '3' or b.chip_cd is NULL then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    denom=a.rstrctd_bnfts_cd = 'C',
    denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
    round=2);

%nonclaimspct2tbl(msr=el631t,
    numer=case when b.rstrctd_bnfts_cd <> 'D' or b.rstrctd_bnfts_cd is NULL then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
    denom=a.mfp_enrlmt_efctv_dt is not null,
    denomtbl=&temptable..&taskprefix._tmsis_mfp_info,
    round=2);

%nonclaimspct2tbl(msr=el632t,
    numer=case when b.msis_ident_num is null then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_mfp_info,
    denom=a.rstrctd_bnfts_cd = 'D',
    denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
    round=2);

%nonclaimspct2tbl(msr=el633t,
    numer=case when b.imgrtn_stus_cd not in ('1', '2', '3') or b.imgrtn_stus_cd is null then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    denom=a.rstrctd_bnfts_cd = '2',
    denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
    round=2);

%nonclaimspct2tbl(msr=el634t,
    numer=case when b.ctznshp_ind = '1' then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    denom=a.rstrctd_bnfts_cd = '2',
    denomtbl=&temptable..&taskprefix._tmsis_elgblty_dtrmnt,
    round=2);

execute (
    create table &wrktable..&taskprefix._el12x as
    select a.*
    , case when b.ZipCode    is null then 1 else 0 end as nonmatchzip
    , case when c.CountyFIPS is null then 1 else 0 end as nonmatchcounty
    , case when d.ZipCode    is null then 1 else 0 end as nonmatchzip_elgbl
    , case when e.CountyFIPS is null then 1 else 0 end as nonmatchcounty_elgbl
    from &temptable..&taskprefix._tmsis_elgbl_cntct a
    
    left join &permview..zipstate_crosswalk b
    on substring(a.elgbl_zip_cd, 1, 5) = b.ZipCode and a.submtg_state_cd = b.StateFIPS
    left join &permview..countystate_lookup c
    on a.elgbl_cnty_cd = c.CountyFIPS and a.submtg_state_cd = c.StateFIPS

    left join &permview..zipstate_crosswalk d
    on substring(a.elgbl_zip_cd, 1, 5) = d.ZipCode and a.elgbl_state_cd = d.StateFIPS
    left join &permview..countystate_lookup e
    on a.elgbl_cnty_cd = e.CountyFIPS and a.elgbl_state_cd = e.StateFIPS
    
    ) by tmsis_passthrough;

%nonclaimspct(msr=el123t,
    numer=case when elgbl_state_cd <> submtg_state_cd or nonmatchcounty=1 or nonmatchzip=1 then 1 else 0 end,
    denom= elgbl_adr_type_cd = '01',
    tbl=&wrktable..&taskprefix._el12x, 
    round=2);

%nonclaimspct(msr=el124t,
    numer=case when submtg_state_cd <> elgbl_state_cd or nonmatchcounty=1 or nonmatchzip=1 then 1 else 0 end,
    denom= elgbl_adr_type_cd <> '01' or elgbl_adr_type_cd is null,
    tbl=&wrktable..&taskprefix._el12x,
    round=2);

%nonclaimspct(msr=el125t,
    numer=case when ctznshp_ind <> '1' or ctznshp_ind is NULL then 1 else 0 end,
    denom=imgrtn_stus_cd = '8',
    tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    round=2);

%nonclaimspct(msr=el126t,
    numer=case when imgrtn_stus_cd <> '8' or imgrtn_stus_cd is NULL then 1 else 0 end,
    denom=ctznshp_ind = '1',
    tbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    round=2);

%nonclaimspct_notany(msr=el127t,
    numer=case when race_cd = '003' then 1 else 0 end,
    denom= crtfd_amrcn_indn_alskn_ntv_ind = '1',
    tbl=&temptable..&taskprefix._tmsis_race_info,
    round=2);

%nonclaimspct(msr=el128t,
    numer=case when nonmatchcounty_elgbl=1 or nonmatchzip_elgbl=1 then 1 else 0 end,
    denom= msis_ident_num is not null,
    tbl=&wrktable..&taskprefix._el12x,
    round=2);

%nonclaimspct2tbl(msr=el331t,
    numer=case when b.enrlmt_type_cd = '2' then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data,
    denom=a.chip_cd = '1',
    denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    round=2);

%nonclaimspct2tbl(msr=el332t,
    numer=case when b.enrlmt_type_cd = '1' then 1 else 0 end,
    numertbl=&temptable..&taskprefix._tmsis_enrlmt_time_sgmt_data,
    denom=a.chip_cd = '3',
    denomtbl=&temptable..&taskprefix._tmsis_var_dmgrphc_elgblty,
    round=2);

execute(
    create table &wrktable..&taskprefix._el122t as
    select
    %str(%')&state.%str(%') as submtg_state_cd
    ,sum(case when has01=0 or alwaysnull=1 then 1 else 0 end) as numer
    ,count(1) as denom
    ,round(sum(case when has01=0 or alwaysnull=1 then 1 else 0 end) / count(1),2) as pct
    from
    (
    select
    msis_ident_num
    , max(case when elgbl_adr_type_cd = '01' then 1 else 0 end) as has01
    , min(case when elgbl_adr_type_cd is null then 1 else 0 end) as alwaysnull
    , max(case when msis_ident_num is not null then 1 else 0 end) as denom
    from &temptable..&taskprefix._tmsis_elgbl_cntct
    group by msis_ident_num
    ) a
    ) by tmsis_passthrough;

/* check for empty tables */ 
%emptytable(el101t);
%emptytable(el102t);
%emptytable(el106t);
%emptytable(el109t);
%emptytable(el110t);
%emptytable(el111t);
%emptytable(el113t);
%emptytable(el114t);
%emptytable(el115t);
%emptytable(el116t);
%emptytable(el117t);
%emptytable(el118t);
%emptytable(el119t);
%emptytable(el302t);
%emptytable(el304t);
%emptytable(el305t);
%emptytable(el306t);
%emptytable(el307t);
%emptytable(el308t);
%emptytable(el309t);
%emptytable(el310t);
%emptytable(el311t);
%emptytable(el312t);
%emptytable(el315t);
%emptytable(el316t);
%emptytable(el1006t);
%emptytable(el1101t);
%emptytable(el1005t);
%emptytable(el622t);

* v1.6 update;
%emptytable(el1008t);
%emptytable(el1007t);
%emptytable(el121t);
%emptytable(el317t);
%emptytable(el318t);
%emptytable(el625t);
%emptytable(el1601t);
%emptytable(el1602t);
%emptytable(el1603t);
%emptytable(el1604t);
%emptytable(el1605t);
%emptytable(el1606t);
%emptytable(el1607t);
%emptytable(el1608t);
%emptytable(el1609t);

* v1.7;
%emptytable(el319t);
%emptytable(el320t);
%emptytable(el321t);
%emptytable(el322t);
%emptytable(el323t);
%emptytable(el324t);
%emptytable(el626t);
%emptytable(el627t);        

* v2.1;
%emptytable(el1701t);
%emptytable(el1702t);
%emptytable(el1703t);
%emptytable(el325t);
%emptytable(el326t);
%emptytable(el628t);
%emptytable(el629t);

* v2.2;
%emptytable(el631t);
%emptytable(el632t);
%emptytable(el125t);
%emptytable(el126t);
%emptytable(el127t);
%emptytable(el331t);
%emptytable(el332t);
%emptytable(el630t);
%emptytable(el633t);
%emptytable(el634t);
%emptytable(el122t);
%emptytable(el123t);
%emptytable(el124t);
%emptytable(el128t);

%let pull = %str(submtg_state_cd, numer as numerator, denom as denominator, pct as pct);

create table el_pct as
select * from connection to tmsis_passthrough
(
		select &pull., 'el1.1' as measure from &wrktable..&taskprefix._el101t
		union all
		select &pull., 'el1.2' as measure from &wrktable..&taskprefix._el102t
		union all
		select &pull., 'el1.6' as measure from &wrktable..&taskprefix._el106t
		union all
		select &pull., 'el1.9' as measure from &wrktable..&taskprefix._el109t
		union all
		select &pull., 'el1.10' as measure from &wrktable..&taskprefix._el110t
		union all
		select &pull., 'el1.11' as measure from &wrktable..&taskprefix._el111t
		union all
		select &pull., 'el1.13' as measure from &wrktable..&taskprefix._el113t
		union all
		select &pull., 'el1.14' as measure from &wrktable..&taskprefix._el114t
		union all
		select &pull., 'el1.15' as measure from &wrktable..&taskprefix._el115t
		union all
		select &pull., 'el1.16' as measure from &wrktable..&taskprefix._el116t
		union all
		select &pull., 'el1.17' as measure from &wrktable..&taskprefix._el117t
		union all
		select &pull., 'el1.18' as measure from &wrktable..&taskprefix._el118t
		union all
		select &pull., 'el1.19' as measure from &wrktable..&taskprefix._el119t
		union all
		select &pull., 'el3.2' as measure from &wrktable..&taskprefix._el302t
		union all
		select &pull., 'el3.4' as measure from &wrktable..&taskprefix._el304t
		union all
		select &pull., 'el3.5' as measure from &wrktable..&taskprefix._el305t
		union all
		select &pull., 'el3.6' as measure from &wrktable..&taskprefix._el306t
		union all
		select &pull., 'el3.7' as measure from &wrktable..&taskprefix._el307t
		union all
		select &pull., 'el3.8' as measure from &wrktable..&taskprefix._el308t
		union all
		select &pull., 'el3.9' as measure from &wrktable..&taskprefix._el309t
		union all
		select &pull., 'el3.10' as measure from &wrktable..&taskprefix._el310t 
		union all
		select &pull., 'el3.11' as measure from &wrktable..&taskprefix._el311t
		union all
		select &pull., 'el3.12' as measure from &wrktable..&taskprefix._el312t 
		union all
		select &pull., 'el3.15' as measure from &wrktable..&taskprefix._el315t 
		union all
		select &pull., 'el3.16' as measure from &wrktable..&taskprefix._el316t 
		union all
		select &pull., 'el10.6' as measure from &wrktable..&taskprefix._el1006t 
		union all
		select &pull., 'el11.1' as measure from &wrktable..&taskprefix._el1101t  
		union all
		select &pull., 'el10.5' as measure from &wrktable..&taskprefix._el1005t 
		union all
		select &pull., 'el6.22' as measure from &wrktable..&taskprefix._el622t 
	    union all
		select &pull., 'el10.8' as measure from &wrktable..&taskprefix._el1008t 
		union all
		select &pull., 'el10.7' as measure from &wrktable..&taskprefix._el1007t 
		union all
		select &pull., 'el1.21' as measure from &wrktable..&taskprefix._el121t 
		union all
		select &pull., 'el3.17' as measure from &wrktable..&taskprefix._el317t 
		union all
		select &pull., 'el3.18' as measure from &wrktable..&taskprefix._el318t 
		union all
		select &pull., 'el6.25' as measure from &wrktable..&taskprefix._el625t 
		union all
		select &pull., 'el16.1' as measure from &wrktable..&taskprefix._el1601t 
		union all
		select &pull., 'el16.2' as measure from &wrktable..&taskprefix._el1602t 
		union all
		select &pull., 'el16.3' as measure from &wrktable..&taskprefix._el1603t 
		union all
		select &pull., 'el16.4' as measure from &wrktable..&taskprefix._el1604t 
	    union all
		select &pull., 'el16.5' as measure from &wrktable..&taskprefix._el1605t 
		union all
		select &pull., 'el16.6' as measure from &wrktable..&taskprefix._el1606t 
		union all
		select &pull., 'el16.7' as measure from &wrktable..&taskprefix._el1607t 
		union all
		select &pull., 'el16.8' as measure from &wrktable..&taskprefix._el1608t 
		union all
		select &pull., 'el16.9' as measure from &wrktable..&taskprefix._el1609t 
		union all 
        select &pull., 'el3.19' as measure from &wrktable..&taskprefix._el319t
        union all
        select &pull., 'el3.20' as measure from &wrktable..&taskprefix._el320t
        union all
        select &pull., 'el3.21' as measure from &wrktable..&taskprefix._el321t
        union all
        select &pull., 'el3.22' as measure from &wrktable..&taskprefix._el322t
        union all
        select &pull., 'el3.23' as measure from &wrktable..&taskprefix._el323t
        union all
        select &pull., 'el3.24' as measure from &wrktable..&taskprefix._el324t
        union all
        select &pull., 'el6.26' as measure from &wrktable..&taskprefix._el626t
        union all
        select &pull., 'el6.27' as measure from &wrktable..&taskprefix._el627t
        union all
        select &pull., 'el17.1' as measure from &wrktable..&taskprefix._el1701t
        union all
        select &pull., 'el17.2' as measure from &wrktable..&taskprefix._el1702t
        union all
        select &pull., 'el17.3' as measure from &wrktable..&taskprefix._el1703t
        union all
        select &pull., 'el3.25' as measure from &wrktable..&taskprefix._el325t
        union all
        select &pull., 'el3.26' as measure from &wrktable..&taskprefix._el326t
        union all
        select &pull., 'el6.28' as measure from &wrktable..&taskprefix._el628t
        union all
        select &pull., 'el6.29' as measure from &wrktable..&taskprefix._el629t
        union all
        select &pull., 'el6.31' as measure from &wrktable..&taskprefix._el631t 
        union all
        select &pull., 'el6.32' as measure from &wrktable..&taskprefix._el632t 
        union all
        select &pull., 'el1.25' as measure from &wrktable..&taskprefix._el125t 
        union all
        select &pull., 'el1.26' as measure from &wrktable..&taskprefix._el126t 
        union all
        select &pull., 'el1.27' as measure from &wrktable..&taskprefix._el127t 
        union all
        select &pull., 'el3.31' as measure from &wrktable..&taskprefix._el331t 
        union all
        select &pull., 'el3.32' as measure from &wrktable..&taskprefix._el332t 
        union all
        select &pull., 'el6.30' as measure from &wrktable..&taskprefix._el630t 
        union all
        select &pull., 'el6.33' as measure from &wrktable..&taskprefix._el633t 
        union all
        select &pull., 'el6.34' as measure from &wrktable..&taskprefix._el634t 
        union all
        select &pull., 'el1.22' as measure from &wrktable..&taskprefix._el122t 
        union all
        select &pull., 'el1.23' as measure from &wrktable..&taskprefix._el123t 
        union all
        select &pull., 'el1.24' as measure from &wrktable..&taskprefix._el124t 
        union all
        select &pull., 'el1.28' as measure from &wrktable..&taskprefix._el128t
);

%dropwrktables(&tbllist);

%tmsis_disconnect;
quit;
%status_check;

%mend _101;




