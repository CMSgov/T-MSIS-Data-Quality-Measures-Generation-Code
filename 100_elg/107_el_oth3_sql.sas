/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 107_el_oth3_sql.sas
 Project: MACBIS Task 2

 Author: Kerianne Hourihan 
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: el_801 and el_802 SAS work dataset
         
 Modifications: 9/13 : SGO added updated #EL8.2 measure for V1.2 updates
				9/25 : SGO added adjustment indicator when counting the capitation and encounters
				9/27 : SGO upated the code to create two versions of Multipleplantypes flag
				12/26 : SGO added Capitation and Encounter type variables for V1.3 update 
						and commented #EL8.1 measures as we are dropping that in V1.3
				1/9/18 : Based on email from Kayshin/Alyssa @ 8:59 AM on 1/9 SGO changed Capitation and Encounter 
						  type variables from NULL to No ca/No enc if a planid don't have any capitations or encounters
 				1/18/17 : SGO Added Emptytable macros
 ******************************************************************************************/

/****************************************************************************************************/
/* Sections 5.8-5.9 */
%macro _107;

	%let tblList = mc_plans ip lt ot rx plan_ids mc_data_1 mc_data plan_link enroll_data_1 enroll_data_2 enroll_data 
				   enrollment cap_hmo cap_php cap_pccm cap_phi cap_oth cap_type capitation
					ent_ip ent_lt ent_ot ent_rx;

	proc sql;

	%tmsis_connect;

	%dropwrkviews(&tblList);
	%dropwrktables(el_802);

	*EL8.2;

	*Bring in unique plan IDs for MC & CLAIMS;

	sysecho "107_EL Getting plan ids";

	*Bring in MC PRTCPTN plan ids;
	execute(
			create or replace view &wrktable..&taskprefix._mc_plans as
			select distinct mc_plan_id as plan_id
		    from &temptable..&taskprefix._tmsis_mc_prtcptn_data
			/*where mc_plan_id is not null*/
	) by tmsis_passthrough; 

	*Bring in the CLAIMS plan ids from claim header;
	%macro clm(ft=,infile=);
	execute(
			 create or replace view &wrktable..&taskprefix._&ft. as
			 select distinct plan_id_num as plan_id
			 from &temptable..&taskprefix._&infile.
			 where clm_type_cd in ('2','B','3','C') 
			 and adjstmt_ind='0'
			 order by plan_id
	)by tmsis_passthrough;  

	%mend clm;
	%clm(ft= ip,infile=base_clh_ip);
	%clm(ft= lt,infile=base_clh_lt);
	%clm(ft= rx,infile=base_clh_rx);
	%clm(ft= ot,infile=base_cll_ot); *CLL level for OT file;

	execute(
		create or replace view &wrktable..&taskprefix._plan_ids as
		select distinct  /*note: this distinct is needed if multiple plan ids satisfy misslogicprv_id */
			case when (%misslogicprv_id(plan_id,12) = 1) then null else plan_id end as plan_id
		from(
			select distinct plan_id from &wrktable..&taskprefix._ip ip
			union 
			select distinct plan_id from &wrktable..&taskprefix._lt lt
			union  
			select distinct plan_id from &wrktable..&taskprefix._ot ot
			union  
			select distinct plan_id from &wrktable..&taskprefix._rx rx
			union 
			select distinct plan_id from &wrktable..&taskprefix._mc_plans mc
		) a
		order by plan_id
	)by tmsis_passthrough;
 
	create table plan_ids as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._plan_ids);

	/* SGO updated the logic for selecting plan_type_mc based on V1.2 update */
	execute(
		create or replace view &wrktable..&taskprefix._mc_data_1 as 
		select plan_id,
				plan_type_mc,
				row_number() over (partition by plan_id order by plan_id asc, plan_type_mc_cnt desc, plan_type_mc asc) as row
		from (		
			select state_plan_id_num as plan_id,
	   				mc_plan_type_cd as plan_type_mc,
					count(1) as plan_type_mc_cnt
			from (
				SELECT case when (%misslogicprv_id(state_plan_id_num,12)=1) then NULL else state_plan_id_num end as state_plan_id_num,
						mc_plan_type_cd				
				FROM &temptable..&taskprefix._tmsis_mc_mn_data
				) a
			group by state_plan_id_num,mc_plan_type_cd
			) b
	)by tmsis_passthrough;

	create table mc_data_1 as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._mc_data_1);
	
	execute(
		create or replace view &wrktable..&taskprefix._mc_data as
		select *
		from 
		(
		select a.*,
				case when b.count_plan_types > 1 then 1 else 0 end as MultiplePlanTypes_mc
		from &wrktable..&taskprefix._mc_data_1 a
			left join
			(select plan_id, count(*) as count_plan_types
			from &wrktable..&taskprefix._mc_data_1
			group by plan_id) b
		on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0') 
		) c
		where row = 1
	)by tmsis_passthrough;

 
	create table mc_data as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._mc_data);

	 
	*Set Managed Care Plan Type from MC file if matching Plan ID;
	execute(
		create or replace view &wrktable..&taskprefix._plan_link as
		select 
			 a.plan_id
		    ,b.plan_type_mc
			,b.MultiplePlanTypes_mc
			,case when b.plan_type_mc is not null then 'YES' else 'NO' end as linked
		from &wrktable..&taskprefix._plan_ids a
		left join &wrktable..&taskprefix._mc_data b
		on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
	)by tmsis_passthrough;

	sysecho "107_EL Getting enrollment";
 
	*(4) Count # of enrollees for the unique plan ID; 
		/* SGO updated the logic for selecting plan_type_el based on V1.2 update */
	execute(
		create or replace view &wrktable..&taskprefix._enroll_data_1 as 
		select plan_id,
				plan_type_el,
		row_number() over (partition by plan_id order by plan_id asc, plan_type_el_cnt desc, plan_type_el asc) as row
		from (		
			select mc_plan_id as plan_id,
	   				enrld_mc_plan_type_cd as plan_type_el,
					count(*) as plan_type_el_cnt
			from (
				SELECT case when (%misslogicprv_id(mc_plan_id,12)=1) then NULL else mc_plan_id end as mc_plan_id,
						enrld_mc_plan_type_cd
				FROM &temptable..&taskprefix._tmsis_mc_prtcptn_data
				) a
			group by mc_plan_id,enrld_mc_plan_type_cd
			) b
		)by tmsis_passthrough;
	
	execute(
		create or replace view &wrktable..&taskprefix._enroll_data_2 as
		select *
		from 
		(
		select a.*,
				case when b.count_plan_types > 1 then 1 else 0 end as MultiplePlanTypes_el
		from &wrktable..&taskprefix._enroll_data_1 a
			left join
				( 	select plan_id, count(1) as count_plan_types /* to flag multiple code id's */
					from &wrktable..&taskprefix._enroll_data_1
					group by plan_id
				) b
		on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
		) c
		where row = 1
	)by tmsis_passthrough;

	execute(
		create or replace view &wrktable..&taskprefix._enroll_data as
		select a.*,
			   c.enrollment
		from &wrktable..&taskprefix._enroll_data_2 a
			left join
				(
				select plan_id, 
						count(distinct msis_ident_num) as enrollment
				from (
					SELECT case when (%misslogicprv_id(mc_plan_id,12)=1) then NULL else mc_plan_id end as plan_id,
							msis_ident_num
					FROM &temptable..&taskprefix._tmsis_mc_prtcptn_data
					where mc_plan_enrlmt_efctv_dt is not NULL
					) b
				group by plan_id
				) c
		on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
	)by tmsis_passthrough;

 
	create table enroll_data as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._enroll_data);

	execute(
		    create or replace view &wrktable..&taskprefix._enrollment as
		    select 
				 a.plan_id 
				,b.plan_type_el
				,b.MultiplePlanTypes_el
				,a.plan_type_mc
				,a.MultiplePlanTypes_mc
				,a.linked
				,b.enrollment
		    from &wrktable..&taskprefix._plan_link a
			left join &wrktable..&taskprefix._enroll_data b
			on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
		)by tmsis_passthrough;

	sysecho "107_EL Getting cap claims";

	***CAPITATION PAYMENTS : OT ***;
	%macro cap_tables(table=,stc_cd=);
	execute(
		   create or replace view &wrktable..&taskprefix._cap_&table. as
		   select 
				 case when (%misslogicprv_id(plan_id_num,12) = 1) 
						then null else plan_id_num end as plan_id
		         ,count(1) as cap_&table.
		   from &temptable..&taskprefix._base_cll_ot
		   where stc_cd = %str(%')&stc_cd.%str(%') and
		         mdcd_pd_amt > 0 and
		         clm_type_cd in ('2','B') and
		         adjstmt_ind = '0'
		   group by plan_id
		)by tmsis_passthrough;

	create table cap_&table. as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._cap_&table.);


	%mend cap_tables;
	%cap_tables (table=hmo,stc_cd=119); *(5) Count # of capitation HMO/HIO/PACE payments for plan ID;
	%cap_tables (table=php,stc_cd=122); *(6) Count # of capitation PHP payments for plan ID;
	%cap_tables (table=pccm,stc_cd=120); *(7) Count # of capitation PCCM payments for plan ID;
	%cap_tables (table=phi,stc_cd=121); *(8) Count # of capitation PHI payments for plan ID;



	*(9) Count # of capitation OTHER payments for plan ID;
	execute(
			create or replace view &wrktable..&taskprefix._cap_oth as
		   select 
				 case when (%misslogicprv_id(plan_id_num,12) = 1) 
						then null else plan_id_num end as plan_id
				,count(1) as cap_oth
		   from &temptable..&taskprefix._base_cll_ot
		   where stc_cd not in ('119','120','121','122') and
		         mdcd_pd_amt > 0 and
		         clm_type_cd in ('2','B') and
		         adjstmt_ind = '0'
		   group by plan_id
		)by tmsis_passthrough;

	create table cap_oth as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._cap_oth);

	execute(
		create or replace view &wrktable..&taskprefix._cap_type as
	   select 
	   		 case when (%misslogicprv_id(plan_id_num,12) = 1) 
				then null else plan_id_num end as plan_id,
	          max(case when clm_type_cd = '2' then 1 else 0 end) as clm_type_cd_2,
			  max(case when clm_type_cd = 'B' then 1 else 0 end) as clm_type_cd_B
	   from &temptable..&taskprefix._base_cll_ot
	   where mdcd_pd_amt > 0 and
	         clm_type_cd in ('2','B') and
	         adjstmt_ind = '0'
	   group by plan_id
	)by tmsis_passthrough;


	create table cap_type as
	select * from connection to tmsis_passthrough
	(select * from &wrktable..&taskprefix._cap_type);

	execute(
		create or replace view &wrktable..&taskprefix._capitation as
		select 
			 a.plan_id
			,a.plan_type_el
			,a.MultiplePlanTypes_el
			,a.plan_type_mc
			,a.MultiplePlanTypes_mc
			,a.linked
			,coalesce(a.enrollment,0) as enrollment
			,coalesce(b.cap_hmo,0) as cap_hmo
			,coalesce(c.cap_php,0) as cap_php
			,coalesce(d.cap_pccm,0) as cap_pccm
			,coalesce(e.cap_phi,0) as cap_phi
			,coalesce(f.cap_oth,0) as cap_oth
			,(coalesce(b.cap_hmo,0) + coalesce(c.cap_php,0) + coalesce(d.cap_pccm,0)
				+ coalesce(e.cap_phi,0) + coalesce(f.cap_oth,0)) as cap_tot
			,case 	when g.clm_type_cd_2 = 1 and g.clm_type_cd_B = 1 then 'Medicaid & S-CHIP'
					when g.clm_type_cd_2 = 1 and g.clm_type_cd_B = 0 then 'Medicaid'
					when g.clm_type_cd_2 = 0 and g.clm_type_cd_B = 1 then 'S-CHIP'
					else 'No cap' end as capitation_type
		from &wrktable..&taskprefix._enrollment a
		left join &wrktable..&taskprefix._cap_hmo  b on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
		left join &wrktable..&taskprefix._cap_php  c on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
	    left join &wrktable..&taskprefix._cap_pccm d on coalesce(a.plan_id,'0') = coalesce(d.plan_id,'0')
		left join &wrktable..&taskprefix._cap_phi  e on coalesce(a.plan_id,'0') = coalesce(e.plan_id,'0')
		left join &wrktable..&taskprefix._cap_oth  f on coalesce(a.plan_id,'0') = coalesce(f.plan_id,'0')
		left join &wrktable..&taskprefix._cap_type g on coalesce(a.plan_id,'0') = coalesce(g.plan_id,'0')
	) by tmsis_passthrough;


	/***ENCOUNTERS: IP/LT/OT/RX****/

	sysecho "107_EL Getting encounter claims";

	%macro encounter_tables(ft=,input=);
	execute(
		create or replace view &wrktable..&taskprefix._ent_&ft. as 
		  select 
		  		case when (%misslogicprv_id(plan_id_num,12) = 1) 
					then null else plan_id_num end as plan_id,
		  		  max(case when clm_type_cd = '3' then 1 else 0 end) as clm_type_cd_3_&ft. ,
				  max(case when clm_type_cd = 'C' then 1 else 0 end) as clm_type_cd_C_&ft. ,
		  	%if "&ft." = "ot" %then %do;
		         count(1) as NUM_&ft. 
			%end;
			%else %do;
		         count(1) as NUM_&ft. 
			%end;
		  from &temptable..&taskprefix._&input.
		  where clm_type_cd in ('3','C') and
		        adjstmt_ind = '0'
		  group by plan_id
		)by tmsis_passthrough;

	%mend encounter_tables;
    
	*(11) IP: Count # of encounters in claims file for plan ID;
	*(16) IP: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
	%encounter_tables(ft=ip,input=base_clh_ip);
	
    *(12) LT: Count # of encounters in claims file for plan ID;
	*(17) LT: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
	%encounter_tables(ft=lt,input=base_clh_lt);
	
    *(13) OT: Count # of encounters in claims file for plan ID*;
	*(18) OT: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID; 
	%encounter_tables(ft=ot,input=base_cll_ot); *use line level for OT;
	
    *(14) RX: Count # of encounters in claims file for plan ID;
	*(19) RX: Obtain Ratio of Number of Encounters to Number of Enrollees for Plan ID;
	%encounter_tables(ft=rx,input=base_clh_rx);

	sysecho "107_EL Calculating table EL8.2";
	execute(
	create table &wrktable..&taskprefix._el_802 as
	select 
		 'EL8.2' as measure
		,%str(%')&state.%str(%') as submtg_state_cd
		,a.*
		,coalesce(plan_type_el,plan_type_mc,'99') as plan_type
		,coalesce(b.num_ip,0) as enc_ip
		,coalesce(c.num_lt,0) as enc_lt
		,coalesce(d.num_ot,0) as enc_ot
		,coalesce(e.num_rx,0) as enc_rx
		,(coalesce(num_ip,0) + coalesce(num_lt,0) + coalesce(num_ot,0) + coalesce(num_rx,0)) as enc_tot
		,case when enrollment > 0 then (coalesce(b.num_ip,0)/enrollment) else null end as ip_ratio
		,case when enrollment > 0 then (coalesce(c.num_lt,0)/enrollment) else null end as lt_ratio
		,case when enrollment > 0 then (coalesce(d.num_ot,0)/enrollment) else null end as ot_ratio
		,case when enrollment > 0 then (coalesce(e.num_rx,0)/enrollment) else null end as rx_ratio
		,case when enrollment > 0 then cap_tot/enrollment else null end as cap_ratio
		,case when (b.clm_type_cd_3_ip = 1 or c.clm_type_cd_3_lt = 1 or d.clm_type_cd_3_ot = 1 or e.clm_type_cd_3_rx = 1) 
		   		and (b.clm_type_cd_C_ip = 1 or c.clm_type_cd_C_lt = 1 or d.clm_type_cd_C_ot = 1 or e.clm_type_cd_C_rx = 1) 
				then 'Medicaid & S-CHIP'
		  when (b.clm_type_cd_3_ip = 1 or c.clm_type_cd_3_lt = 1 or d.clm_type_cd_3_ot = 1 or e.clm_type_cd_3_rx = 1) 
				then 'Medicaid'
		  when (b.clm_type_cd_C_ip = 1 or c.clm_type_cd_C_lt = 1 or d.clm_type_cd_C_ot = 1 or e.clm_type_cd_C_rx = 1) 
				then 'S-CHIP'
					else 'No enc' end as encounter_type
	from &wrktable..&taskprefix._capitation a
	left join &wrktable..&taskprefix._ent_ip b on coalesce(a.plan_id,'0') = coalesce(b.plan_id,'0')
	left join &wrktable..&taskprefix._ent_lt c on coalesce(a.plan_id,'0') = coalesce(c.plan_id,'0')
	left join &wrktable..&taskprefix._ent_ot d on coalesce(a.plan_id,'0') = coalesce(d.plan_id,'0')
	left join &wrktable..&taskprefix._ent_rx e on coalesce(a.plan_id,'0') = coalesce(e.plan_id,'0')
	) by tmsis_passthrough;
    
    %emptytable(el_802);    

	create table el_802 as
	select * from connection to tmsis_passthrough
	(
		select * 
		from &wrktable..&taskprefix._el_802 
		order by plan_id
	);

%dropwrkviews(&tblList);
%dropwrktables(el_802);

%tmsis_disconnect;

quit;
%status_check;

%mend _107;

