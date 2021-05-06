/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/
/******************************************************************************************
 Program: 108_el_oth4_sql.sas
 Project: MACBIS Task 2

 Author: Kerianne Hourihan 
  
 Input: Temporary tables created in universal_macros.sas
 
 Output: el_503 SAS work dataset
         
 Modifications: 9/13 : SGO added updated the current and prior month join criteria at the end for V1.2 updates
 				1/18/17 : SGO Added Emptytable macros
 ******************************************************************************************/

/*section 5.3 index of dissimilarity by age group for m-chip & s-chip (chip=2/3)*/
%macro _108;

proc sql;
%tmsis_connect;

*el5.3;
/*create age group*/

%macro agegroups(prior=);

%let tbllist = el503prep_&prior el503a_&prior el503b_&prior el503b_&prior._v2;

%dropwrkviews(&tbllist);

execute(
		create or replace view &wrktable..&taskprefix._el503prep_&prior. as
		select a.msis_ident_num
				,a.age
				,case when b.chip_cd = '2' then 1 else 0 end as chip_cd2
				,case when b.chip_cd = '3' then 1 else 0 end as chip_cd3
		from 
			%if &prior = 1 %then %do;
				&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty_prior a
				inner join &temptable..&taskprefix._tmsis_var_dmgrphc_elgblty_prior b
			%end;
			%else %do;
				&temptable..&taskprefix._tmsis_prmry_dmgrphc_elgblty a
				inner join &temptable..&taskprefix._tmsis_var_dmgrphc_elgblty b
			%end;
		on a.msis_ident_num = b.msis_ident_num
		where a.msis_ident_num is not null
		and b.msis_ident_num is not null
	)by tmsis_passthrough;


execute(
	create or replace view &wrktable..&taskprefix._el503a_&prior. as
	select 
		 msis_ident_num
		,min(age) as age
		,case when max(chip_cd2) = 1 and max(chip_cd3) = 0 then '2'
		      when max(chip_cd2) = 0 and max(chip_cd3) = 1 then '3'
			  when max(chip_cd2) = 1 and max(chip_cd3) = 1 then 'both'
			  else 'no chip' end as chip_cd
        ,case when min(age) < 1 then 'under 1'
            when min(age) between 1 and 5 then '1 to 5'
            when min(age) between 6 and 14 then '6 to 14'
            when min(age) between 15 and 18 then '15 to 18'
            when min(age) between 19 and 20 then '19 to 20'
            when min(age) between 21 and 44 then '21 to 44'
            when min(age) between 45 and 64 then '45 to 64'
            when min(age) between 65 and 74 then '65 to 74'
            when min(age) between 75 and 84 then '75 to 84'
            when min(age) >=85 then '85 and over'
            else 'age group unknown'
			end as agegrp
	from &wrktable..&taskprefix._el503prep_&prior.
	group by msis_ident_num
	) by tmsis_passthrough;

	
	execute(
		create or replace view &wrktable..&taskprefix._el503b_&prior. as
		select
			 agegrp
			,chip_cd
			,count(1) as rec_count
		from &wrktable..&taskprefix._el503a_&prior.
		where chip_cd in ('2','3','both')
		group by agegrp, chip_cd
	)by tmsis_passthrough;



/**changed this part **/
/** if someone has "both", they should be counted in chip code=2 and 3 categories */
	execute(
		create or replace view &wrktable..&taskprefix._el503b_&prior._v2 as
		select  
             a.agegrp
			,a.chip_cd
			,a.rec_count
			,coalesce(b.rec_count,0) as rec_count_both
			,a.rec_count + coalesce(b.rec_count,0) as rec_count_tot
		from 

        ( select * from &wrktable..&taskprefix._el503b_&prior. 
		   where chip_cd in ('2','3')) a

		left join

        (select  * from &wrktable..&taskprefix._el503b_&prior.
		  where chip_cd in ('both') ) b

		on a.agegrp=b.agegrp

	)by tmsis_passthrough;

    %dropwrktables(el503c_&prior.);
	execute(
		create table &wrktable..&taskprefix._el503c_&prior. as
		select
			 agegrp as valid_value
			,chip_cd
	   	    ,rec_count_tot
			,rec_count_tot / sum_rec_count as pct
		from
			(select
				 agegrp
				,chip_cd
				,rec_count_tot
				,sum(rec_count_tot) over (partition by chip_cd) as sum_rec_count
			 from &wrktable..&taskprefix._el503b_&prior._v2
			 ) a
		) by tmsis_passthrough;

		%dropwrkviews(&tbllist);


%mend;
%agegroups(prior=0);
%agegroups(prior=1);


%dropwrkviews(el503_comp);
%dropwrktables(el503);
execute(
	create or replace view &wrktable..&taskprefix._el503_comp as
	select 
		 coalesce(c1.valid_value,c2.valid_value) as valid_value
		,coalesce(c1.chip_cd,c2.chip_cd) as chip_cd
		,case when c1.pct is not null and c2.pct is not null
		 	then abs((c1.pct - c2.pct)/2)
		 	else null end as pct_comp
	from &wrktable..&taskprefix._el503c_0 c1
	full join &wrktable..&taskprefix._el503c_1 c2
	on c1.valid_value = c2.valid_value
	and c1.chip_cd = c2.chip_cd
	) by tmsis_passthrough;


execute(
	create table &wrktable..&taskprefix._el503 as
	select %str(%')&state.%str(%') as submtg_state_cd,
		 'el5.3' as measure
		,round(sum(pct_comp),2) as pct
	from &wrktable..&taskprefix._el503_comp
) by tmsis_passthrough;
  
  
%emptytable(el503);

create table el503 as
select * from connection to tmsis_passthrough
(select * from &wrktable..&taskprefix._el503);

%dropwrktables(el503c_0 el503c_1 el503);
%dropwrkviews(el503_comp);

%tmsis_disconnect;
quit;
%status_check;

%mend _108;

   


