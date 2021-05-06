/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/*****************************************************************
Project:     50139 MACBIS Data Analytics Task 2
Program:     universal_macros.sas
Programmer:  Kerianne Hourihan

Measures: None
Inputs:   None
Outputs:  None

Modifications:
*****************************************************************/

%macro timestamp_log();
  %let actual_time = %sysfunc(putn(%sysfunc(datetime()),datetime22.));
  %put *----------------------------------------------------*;
  %put Timestamp: &sysuserid. &actual_time. ;
  %put *----------------------------------------------------*;
%mend;

%macro status_check();

  %put *----------------------------------------------------*;

  %if %symexist(sqlxrc) %then %do;
      %put sqlxrc  = &sqlxrc;
	  %if &sqlxrc ne 0 %then %do;
	     %put restart at &restart;
		 %put restart with pgmtime = &pgmstart;
	     %abort return;
	  %end;
  %end;
  %if %symexist(syserr) %then %do;
      %put syserr = &syserr;
      %if &syserr ne 0 %then %do;
	      %put restart at &restart;
		  %put restart with pgmtime = &pgmstart;
		  %abort return;
	   %end;
  %end;
  %put *----------------------------------------------------*;

%mend;

%macro create_state_vars();

    data _null_;
        set state;
        call symputx('state',state,'G');
        if anydigit(chipstate) > 0 then call symputx('chipstate',chipstate,'G');
        call symputx('stabbrev',stabbrev,'G');
        call symputx('stname',stname,'G');
        call symputx('has_schip',has_schip,'G');
    run;
     data _null_;
        set runid;
        if _n_ = 1 then call symputx('run_id',runid,'G');
        if _n_ = 2 then call symputx('run_id2',runid,'G');
    run;
%mend;
%macro define_runtyp();
data _null_;
if &separate_entity.=. or &separate_entity.=0 then call symput('typerun','');
else if &separate_entity.=1 then call symput('typerun','M');
else if &separate_entity.=2 then call symput('typerun','C');
run;
%mend;

%macro define_state(state);

	sysecho "Creating state variables";

    data state;
    length state chipstate $8.;
        stabbrev = symget("rpt_state");
        separate_entity = symget("separate_entity");
        state = trim(put(stabbrev,$st_fips.));
        if index(state,',') > 0 then do;
            chipstate = scan(state,2,',');
            state = scan(state,1,',');
        end;
        *special handling for PA -- sometimes separate reports needed for Medicaid and CHIP;
		* 2018/06/26: Make it flexible enough to handle other states ;

        if /*stabbrev = 'PA' and */ separate_entity = '1' /*Medicaid only*/ then do;
            chipstate = '';
        end;
        else if /*stabbrev = 'PA' and */ separate_entity = '2' /*CHIP only*/ then do;
            state = chipstate;
            chipstate = '';
        end;
        stname = put(stabbrev,$st2_name.);
        call symputx('state',state);
        if anydigit(chipstate) > 0 then call symputx('chipstate',chipstate);
    run;

    proc sql;
        %tmsis_connect;

		*creates a local (SAS) table;
        create table runid as
        select * from connection to tmsis_passthrough
        (select max(tmsis_run_id) as run_id,
                submtg_state_cd
         from tmsis.tmsis_fhdr_rec_ip
         %if %symexist(chipstate) %then %do;
            where submtg_state_cd in (%str(%')&state.%str(%'),%str(%')&chipstate.%str(%'))
         %end;
         %else %do;
            where submtg_state_cd = %STR(%')&state.%STR(%')
         %end;
         group by submtg_state_cd
         order by submtg_state_cd
         );
         %tmsis_disconnect;
    quit;
	%status_check;

    data runid;
        set runid;
        by submtg_state_cd;
        if _n_ = 1 then do;
            spec_run_id=input(symget("specific_run_id"),6.);
            if spec_run_id > 0 then runid=put(spec_run_id,z5.);
            else runid = put(run_id,z5.);
        end;
        else do;
            spec_run_id=input(symget("specific_run_id2"),6.);
            if spec_run_id > 0 then runid=put(spec_run_id,z5.);
            else runid = put(run_id,z5.);
        end;
    run;

    proc sort data=schip.lookup out=schip (keep=stabbrev has_schip);
    by stabbrev;
    run;

    data state;
        merge state (in=a)
              schip;
        by stabbrev;
        if a;
        has_schip = upcase(has_schip);
    run;

    %create_state_vars;

%mend;

%macro create_month_vars();

    data _null_;
        set report_month;
        *write to macro vars;
        call symputx('rpt_month',rpt_month_name,'G');
        call symputx('rpt_fldr',rpt_fldr_name,'G');
        call symputx('m_start',m_start,'G');
        call symputx('m_end',m_end,'G');
        call symputx('m_label',m_label,'G');
        call symputx('m_col',m_col,'G');
		call symputx('prior_m_start',prior_m_start,'G');
		call symputx('prior_m_end',prior_m_end,'G');
    run;

%mend;

%macro define_dates(rpt_month);

	sysecho "Creating month variables";

    data report_month;

        *char variables;
        rpt_month_c = symget("rpt_month");
        rpt_month = input(rpt_month_c,date9.);
        rpt_month_name = put(rpt_month,monyy7.);
        rpt_fldr_name = put(rpt_month,yymmn7.);

        *reporting month;
        m_start_n = rpt_month;
        m_start = cats("'",put(rpt_month,yymmdd10.),"'");
        m_end = cats("'",put(intnx('month',rpt_month,1) - 1,yymmdd10.),"'");
        m_label = put(m_start_n,monname.)||" "||put(m_start_n,year.);
        m_col = cat('month01',put(m_start_n,monyy7.));

		*prior month (for index of dissimilarity);
		prior_m_start = cats("'",put(intnx('month',rpt_month,-1),yymmdd10.),"'");
		prior_m_end = cats("'",put((rpt_month - 1),yymmdd10.),"'");

    run;

    %create_month_vars;

%mend define_dates;

%macro run_id_filter();

	    %if %symexist(chipstate) %then %do;
	    	submtg_state_orig in (%str(%')&state.%str(%'),%str(%')&chipstate.%str(%'))
	        and tmsis_run_id in (&run_id.,&run_id2.)
	    %end;
	    %else %do;
	    	submtg_state_orig = %str(%')&state.%str(%')
	        and tmsis_run_id = &run_id.
	    %end;

%mend;

%macro emptytable(tbl);

	%local obs i;

	select obs into :obs from connection to tmsis_passthrough
	(select count(1) as obs from &wrktable..&taskprefix._&tbl.);

	%if &obs. = 0 %then %do;
		execute(
			insert into &wrktable..&taskprefix._&tbl. (submtg_state_cd)
				values (%str(%')&state.%str(%'));
		) by tmsis_passthrough;
	%end;

%mend;

%macro temptable_connect;
	LIBNAME TMPTBLDB ODBC  DATASRC=Databricks authdomain="TMSIS_DBK"
	NOPROMPT='dsn=Databricks;HTTPPath=sql/protocolv1/o/0/databricks-bi-sas-prod-T-MSIS-data-quality-measure;'
	SCHEMA=&temptable. insertbuff=1000;
	CONNECT USING TMPTBLDB as tmsis_temptable;
	EXECUTE (set val user = VALUEOF(&sysuserid)) by tmsis_temptable;
%mend temptable_connect;

%macro temptable_disconnect;
  DISCONNECT FROM tmsis_temptable;
%mend temptable_disconnect;

/**Performance indicator PI data **/
/*
%macro create_perfom_ind_tables();

	proc sql;
		%tmsis_connect;

		sysecho "Restricting Perfomance Indicator tables";
        %droptemptables(perfom_ind);
	 	execute(
		create table &temptable..&taskprefix._perfom_ind as

        select a.* 
		,%str(%')&state.%str(%') as submtg_state_cd

        from pimcee.performance_indicator  a
		inner join 
     
				(select state_cd,
                        max(rptg_prd_end_dt) as max_avail_rptg_prd 

				from pimcee.performance_indicator
                where state_cd in (%str(%')&rpt_state.%str(%')) and
                      rptg_prd_end_dt <= &m_end.
                group by state_cd) b

         on a.state_cd=b.state_cd and
            a.rptg_prd_end_dt= b.max_avail_rptg_prd 
					
		) by tmsis_passthrough;

	 select * from connection to tmsis_passthrough
	(select * from &temptable..&taskprefix._perfom_ind limit 100) ;

		%tmsis_disconnect;	
quit;	
	%timestamp_log;

%mend;
*/
/*note: use month=current for the regular monthly tables, month=prior for the prior month (for index of dissimilarity)*/
%macro create_elig_tables(monthind=);

	%if "&monthind." = "current" %then %do;
		%let suffix=;
		%let start_date = &m_start;
		%let end_date = &m_end;
	%end;
	%if "&monthind." = "prior" %then %do;
		%let suffix=_prior;
		%let start_date = &prior_m_start;
		%let end_date = &prior_m_end;
	%end;

	proc sql;

		%tmsis_connect;

			sysecho "Creating base elig tables";

			*base elig;
			%droptemptables(base_elig&suffix.);
			execute(
				create table &temptable..&taskprefix._base_elig&suffix. as
				select *
                    ,%str(%')&state.%str(%') as submtg_state_cd
				from &permview..base_elig_view
				where %run_id_filter
				and ((enrlmt_efctv_dt <= &end_date. and enrlmt_efctv_dt is not null)
		        and (enrlmt_end_dt >= &end_date. or enrlmt_end_dt is null))
			&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._base_elig&suffix.);

		%if "&monthind." = "current" %then %do;
			*ever elig;
			%droptempviews(ever_elig);
			execute(
				create or replace view &temptable..&taskprefix._ever_elig as
				select *
                    ,%str(%')&state.%str(%') as submtg_state_cd                
				    ,1 as ever_eligible
				from &permview..base_elig_view
				where %run_id_filter
				&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._ever_elig);

			*ever elig determinant;
			%droptemptables(ever_elig_dtrmnt);
			execute(
				create table &temptable..&taskprefix._ever_elig_dtrmnt as
				select *
                    ,%str(%')&state.%str(%') as submtg_state_cd                
				from &permview..base_dtrmnt_view
				where %run_id_filter
				&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._ever_elig_dtrmnt);

			*elig in month primary demographics (for EL1.15);
			%droptemptables(elig_in_month_prmry);
			execute(
				create table &temptable..&taskprefix._elig_in_month_prmry as
				select *
					,%str(%')&state.%str(%') as submtg_state_cd
				from
					(select distinct msis_ident_num from &temptable..&taskprefix._ever_elig
						where ((enrlmt_efctv_dt <= &end_date. and enrlmt_efctv_dt is not null)
		        		and (enrlmt_end_dt >= &start_date. or enrlmt_end_dt is null))) a
				left join &permview..tmsis_prmry_dmgrphc_elgblty_view b
				on %run_id_filter
				and a.msis_ident_num = b.msis_id
				and (((prmry_dmgrphc_ele_efctv_dt <= &end_date. and prmry_dmgrphc_ele_efctv_dt is not null)
		    		and (prmry_dmgrphc_ele_end_dt >= &end_date. or prmry_dmgrphc_ele_end_dt is null))
		    		or (prmry_dmgrphc_ele_efctv_dt is null and prmry_dmgrphc_ele_end_dt is null))
				&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._elig_in_month_prmry);

		%end;

		%tmsis_disconnect;
	quit;
	%status_check;

	%timestamp_log;

	proc sql;
		%tmsis_connect;

		sysecho "Creating secondary elig tables";

			%if "&monthind." = "current" %then %do;

				%let tblList = tmsis_prmry_dmgrphc_elgblty tmsis_var_dmgrphc_elgblty tmsis_elgbl_cntct tmsis_elgblty_dtrmnt 
							   tmsis_hh_sntrn_prtcptn_info tmsis_hh_chrnc_cond tmsis_lckin_info tmsis_mfp_info tmsis_ltss_prtcptn_data 				   
							   tmsis_state_plan_prtcptn tmsis_wvr_prtcptn_data tmsis_mc_prtcptn_data tmsis_ethncty_info tmsis_race_info 
							   tmsis_dsblty_info tmsis_sect_1115a_demo_info tmsis_hcbs_chrnc_cond_non_hh tmsis_enrlmt_time_sgmt_data;

				%let dtPrefix = prmry_dmgrphc_ele var_dmgrphc_ele elgbl_adr elgblty_dtrmnt 
								hh_sntrn_prtcptn hh_chrnc lckin mfp_enrlmt ltss_elgblty 
								state_plan_optn wvr_enrlmt mc_plan_enrlmt ethncty_dclrtn race_dclrtn
							    dsblty_type sect_1115a_demo ndc_uom_chrnc_non_hh enrlmt;
			%end;
			%else %if "&monthind." = "prior" %then %do;

				%let tblList = tmsis_prmry_dmgrphc_elgblty tmsis_var_dmgrphc_elgblty tmsis_elgbl_cntct tmsis_mc_prtcptn_data 
							   tmsis_ethncty_info tmsis_race_info;

				%let dtPrefix = prmry_dmgrphc_ele var_dmgrphc_ele elgbl_adr mc_plan_enrlmt 
								ethncty_dclrtn race_dclrtn;


			%end;

			%local i next_tbl efctv_dt end_dt;
			%do i=1 %to %sysfunc(countw(&tblList));
		   		%let next_tbl = %scan(&tblList,  &i);
				%let efctv_dt = %scan(&dtPrefix, &i)_efctv_dt;
				%let end_dt   = %scan(&dtPrefix, &i)_end_dt;
		 
				%droptempviews(&next_tbl.&suffix.);
				execute(
					create or replace view &temptable..&taskprefix._&next_tbl.&suffix. as
					select 
						 coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                        ,%str(%')&state.%str(%') as submtg_state_cd                         
						,b.*
						%if "&next_tbl." = "tmsis_prmry_dmgrphc_elgblty" %then %do;
							%if "&monthind." = "prior" %then %do;
	    						,case when (death_dt is not null and death_dt <= &end_date.) then floor(datediff(death_dt, birth_dt)/365.25) 
			  						  else floor(datediff(&end_date.,birth_dt)/365.25) end as age
							%end;
							%else %do;
	    						,case when (death_dt is not null and death_dt <= &end_date.) then floor(datediff(death_dt, birth_dt)/365.25) 
			  						  else floor(datediff(&end_date.,birth_dt)/365.25) end as age
							%end;
						%end;
					from
					(select distinct msis_ident_num from &temptable..&taskprefix._base_elig&suffix.) a
					left join &permview..&next_tbl._view b
					on a.msis_ident_num = b.msis_id
					and %run_id_filter
					%if "&next_tbl." = "tmsis_elgblty_dtrmnt" %then %do;
        				and prmry_elgblty_grp_ind = '1'
    				%end;
					and (((&efctv_dt. <= &end_date. and &efctv_dt. is not null)
		    		and (&end_dt. >= &end_date. or &end_dt. is null))
		    		or (&efctv_dt. is null and &end_dt. is null))
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._&next_tbl.&suffix.);

			%end;

		%tmsis_disconnect;
	quit;
	%status_check;

	%timestamp_log;

%mend;

%macro create_prov_tables();

	proc sql;
		%tmsis_connect;

			sysecho "Creating prov tables";

			*base_prov_table;
			%droptemptables(base_prov);
			execute(
				create table &temptable..&taskprefix._base_prov as
				select *
                    ,%str(%')&state.%str(%') as submtg_state_cd               
				from &permview..base_prov_view
				where %run_id_filter					
		        and ((prvdr_mdcd_efctv_dt <= &m_end. and prvdr_mdcd_efctv_dt is not null)
           		and (prvdr_mdcd_end_dt >= &m_end. or prvdr_mdcd_end_dt is null))
				&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._base_prov);

			%let tblList = tmsis_prvdr_attr_mn tmsis_prvdr_lctn_cntct tmsis_prvdr_id tmsis_prvdr_txnmy_clsfctn 
						   tmsis_prvdr_mdcd_enrlmt tmsis_prvdr_afltd_pgm;

			%let dtPrefix = prvdr_attr prvdr_lctn_cntct prvdr_id prvdr_txnmy_clsfctn 
							prvdr_mdcd prvdr_afltd_pgm;

			%local i next_tbl efctv_dt end_dt;
			%do i=1 %to %sysfunc(countw(&tblList));
		   		%let next_tbl = %scan(&tblList,  &i);
				%let efctv_dt = %scan(&dtPrefix, &i)_efctv_dt;
				%let end_dt   = %scan(&dtPrefix, &i)_end_dt;
		 
				%droptempviews(&next_tbl.);
				execute(
					create or replace view &temptable..&taskprefix._&next_tbl. as
					select 
						b.*
                        ,%str(%')&state.%str(%') as submtg_state_cd                        
					from
					(select distinct submtg_state_orig as state_orig, submtg_state_prvdr_id from &temptable..&taskprefix._base_prov) a
					inner join &permview..&next_tbl._view b
					on a.submtg_state_prvdr_id = b.submtg_state_prvdr_id
					and a.state_orig = b.submtg_state_orig
					and %run_id_filter
					and (((&efctv_dt. <= &m_end. and &efctv_dt. is not null)
				   	and (&end_dt. >= &m_end. or &end_dt. is null))
			            %if "&next_tbl." = "tmsis_prvdr_txnmy_clsfctn" %then %do;
			             or (&efctv_dt. is null and &end_dt. is null)
			            %end;)
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._&next_tbl.);

			%end;

			%let tblList = tmsis_prvdr_attr_mn tmsis_prvdr_id tmsis_prvdr_mdcd_enrlmt;
            %let evrvarList = ever_provider ever_provider_id ever_enrolled_provider;

			%local i next_tbl;
			%do i=1 %to %sysfunc(countw(&tblList));
		   		%let next_tbl = %scan(&tblList,  &i);
				%let ever_var = %scan(&evrvarList,  &i);
		 
				*create ever provider view;

				%droptempviews(ever_&next_tbl.);
				execute(
					create or replace view &temptable..&taskprefix._ever_&next_tbl. as
					select *
                    ,%str(%')&state.%str(%') as submtg_state_cd  
 
                    ,case when (submtg_state_prvdr_id is not NULL and 
      							  not submtg_state_prvdr_id like repeat(8,30) and
	  							  not submtg_state_prvdr_id like repeat(9,30) and	  
	                              submtg_state_prvdr_id rlike '[a-zA-Z1-9]') 

                           then 1 else 0 end as &ever_var.

					from &permview..&next_tbl._view
					where %run_id_filter
					&limit.
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._ever_&next_tbl.);
	
			%end;

		%tmsis_disconnect;
	quit;
	%status_check;
			
	%timestamp_log;

%mend;

%macro create_mcplan_tables();

	proc sql;
		%tmsis_connect;

			sysecho "Creating MC plan tables";

			%let tblList = tmsis_mc_mn_data tmsis_mc_oprtg_authrty tmsis_natl_hc_ent_id_info;

			%let dtPrefix = mc_mn_rec mc_op_authrty natl_hlth_care_ent_id;

			%local i next_tbl efctv_dt end_dt;
			%do i=1 %to %sysfunc(countw(&tblList));
		   		%let next_tbl = %scan(&tblList,  &i);
				%let efctv_dt = %scan(&dtPrefix, &i)_efctv_dt;
				%let end_dt   = %scan(&dtPrefix, &i)_end_dt;
		 
				%droptempviews(&next_tbl.);
				execute(
					create or replace view &temptable..&taskprefix._&next_tbl. as
					select 
						*
                        ,%str(%')&state.%str(%') as submtg_state_cd						
					from &permview..&next_tbl._view
					where %run_id_filter
					and (((&efctv_dt. <= &m_end. and &efctv_dt. is not null)
    					and (&end_dt. >= &m_end. or &end_dt. is null))
    				or (&efctv_dt. is null and &end_dt. is null))
					&limit.
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._&next_tbl.);

			%end;
	
		%tmsis_disconnect;
	quit;
	%status_check;
			
	%timestamp_log;

%mend;

%macro create_tpl_tables();

	proc sql;

		%tmsis_connect;

			sysecho "Creating TPL tables";

			%let tblList = tmsis_tpl_mdcd_prsn_mn tmsis_tpl_mdcd_prsn_hi;

			%let dtPrefix = elgbl_prsn_mn insrnc_cvrg;

			%local i next_tbl efctv_dt end_dt;
			%do i=1 %to %sysfunc(countw(&tblList));
		   		%let next_tbl = %scan(&tblList,  &i);
				%let efctv_dt = %scan(&dtPrefix, &i)_efctv_dt;
				%let end_dt   = %scan(&dtPrefix, &i)_end_dt;
		 
				%droptempviews(&next_tbl.);
				execute(
					create or replace view &temptable..&taskprefix._&next_tbl. as
					select 
						 coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                        ,%str(%')&state.%str(%') as submtg_state_cd                         
						,b.*
					from
					(select distinct msis_ident_num from &temptable..&taskprefix._base_elig) a
					left join &permview..&next_tbl._view b
					on a.msis_ident_num = b.msis_id
					and %run_id_filter
					and (((&efctv_dt. <= &m_end. and &efctv_dt. is not null)
		    		and (&end_dt. >= &m_end. or &end_dt. is null))
		    		or (&efctv_dt. is null and &end_dt. is null))
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._&next_tbl.);

			%end;

			*create ever tpl;
			%droptempviews(ever_tpl);
			execute(
				create or replace view &temptable..&taskprefix._ever_tpl as
				select 
					 tmsis_run_id
					,%str(%')&state.%str(%') as submtg_state_cd
				 	,msis_id as msis_ident_num
					,elgbl_prsn_mn_efctv_dt
					,elgbl_prsn_mn_end_dt
					,tpl_insrnc_cvrg_ind
              		,tpl_othr_cvrg_ind
					,1 as ever_tpl
				from &permview..tmsis_tpl_mdcd_prsn_mn_view
				where %run_id_filter
				&limit.
			) by tmsis_passthrough;
			%countrecords(&temptable..&taskprefix._ever_tpl);
	
		%tmsis_disconnect;
	quit;
	%status_check;
			
	%timestamp_log;

%mend;

%macro create_claims_tables();

	%let clmList = ip lt ot rx;

	%local i clm_file;
	%do i=1 %to %sysfunc(countw(&clmList));

		%let clm_file = %scan(&clmList,  &i);

		proc sql;

			%tmsis_connect;

				sysecho "Creating &clm_file. claims tables";
			
				%droptemptables(dup_clh_&clm_file.);
				execute(
					create table &temptable..&taskprefix._dup_clh_&clm_file. as
					select *
                        ,%str(%')&state.%str(%') as submtg_state_cd 
						,count(1) over (partition by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind) as count_claims_key
					from &permview..prep_clh_&clm_file._view
					where %run_id_filter
					and tmsis_rptg_prd = &m_start.
					and denied_header = 0
					cluster by orgnl_clm_num
					&limit.
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._dup_clh_&clm_file.);


				%droptemptables(base_clh_&clm_file.);
				execute(
					create table &temptable..&taskprefix._base_clh_&clm_file. as
					select *                     
					from &temptable..&taskprefix._dup_clh_&clm_file.
					where count_claims_key = 1					
					cluster by orgnl_clm_num
				) by tmsis_passthrough;				
				%countrecords(&temptable..&taskprefix._base_clh_&clm_file.);


				%droptemptables(dup_cll_&clm_file._prep dup_cll_&clm_file.);
				execute(
					create table &temptable..&taskprefix._dup_cll_&clm_file._prep as
					select *
                            ,%str(%')&state.%str(%') as submtg_state_cd                    
							,count(1) over (partition by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                           			orgnl_line_num, adjstmt_line_num, line_adjstmt_ind) as count_cll_key
					from &permview..prep_cll_&clm_file._view
					where %run_id_filter
					and tmsis_rptg_prd = &m_start.
					and denied_line = 0
					cluster by orgnl_clm_num
					&limit.
				) by tmsis_passthrough;

				execute(
					create table &temptable..&taskprefix._dup_cll_&clm_file. as
					select a.*
						  ,b.denied_header
					from &temptable..&taskprefix._dup_cll_&clm_file._prep a
					left join
					(select distinct
						 orgnl_clm_num
						,adjstmt_clm_num
						,adjdctn_dt
						,adjstmt_ind
						,denied_header
						from &permview..prep_clh_&clm_file._view
						where %run_id_filter
						and tmsis_rptg_prd = &m_start.
					) b
					on a.orgnl_clm_num = b.orgnl_clm_num
					and a.adjstmt_clm_num = b.adjstmt_clm_num
					and a.adjdctn_dt = b.adjdctn_dt
					and a.line_adjstmt_ind = b.adjstmt_ind
					where (denied_header = 0 or denied_header is null)
					cluster by orgnl_clm_num
					&limit.
				) by tmsis_passthrough;
				%droptemptables(dup_cll_&clm_file._prep);
				%countrecords(&temptable..&taskprefix._dup_cll_&clm_file.);


				%droptemptables(base_cll_&clm_file.);
				execute(
					create table &temptable..&taskprefix._base_cll_&clm_file. as
					select
						/*keeping line-level variables where possible, but childless headers will
					      need to have values from header level.*/
					     coalesce(b.submtg_state_cd, a.submtg_state_cd) as submtg_state_cd
						,coalesce(b.submtg_state_orig, a.submtg_state_orig) as submtg_state_orig
			            ,coalesce(b.tmsis_run_id, a.tmsis_run_id) as tmsis_run_id
			            ,coalesce(b.tmsis_rptg_prd, a.tmsis_rptg_prd) as tmsis_rptg_prd
			            ,coalesce(b.msis_ident_num, a.msis_ident_num) as msis_ident_num
			        	,coalesce(b.orgnl_clm_num, a.orgnl_clm_num) as orgnl_clm_num
			        	,coalesce(b.adjstmt_clm_num, a.adjstmt_clm_num) as adjstmt_clm_num
			        	,coalesce(b.adjdctn_dt, a.adjdctn_dt) as adjdctn_dt
			            ,b.orgnl_line_num
			            ,b.adjstmt_line_num
			            ,b.line_adjstmt_ind
						,case when b.orgnl_line_num is null and b.line_adjstmt_ind is null 
								then 1 else 0 end as childless_header_flag

            			/* Need these variables that are not recoded for tabulating missingness measures **/
			            ,b.orgnl_line_num_orig
			            ,b.adjstmt_line_num_orig
			            ,b.line_adjstmt_ind_orig
						,b.line_adjdctn_dt_orig

						,b.xix_srvc_ctgry_cd 
			            ,b.xxi_srvc_ctgry_cd
						,b.cll_stus_cd
			            ,b.mdcd_ffs_equiv_amt
			            ,b.mdcd_pd_amt

		            %if "&clm_file." = "ip" %then %do;
						,b.cms_64_fed_reimbrsmt_ctgry_cd
		                ,b.srvc_endg_dt
		                ,b.stc_cd
		                ,b.rev_cd
		                ,b.prscrbng_prvdr_npi_num
		                ,b.bnft_type_cd
		                ,b.srvcng_prvdr_num
		                ,b.prvdr_fac_type_cd
		                ,b.rev_chrg_amt
		                ,b.srvcng_prvdr_spclty_cd
		                ,b.srvcng_prvdr_type_cd
		                ,b.srvc_bgnng_dt
					    ,b.alowd_amt
						,oprtg_prvdr_npi_num
		            %end;
		            %if "&clm_file." = "lt" %then %do;
		                ,b.stc_cd
		                ,b.bnft_type_cd
		                ,b.srvcng_prvdr_num
		                ,b.cms_64_fed_reimbrsmt_ctgry_cd
						,b.srvc_bgnng_dt
		                ,b.srvc_endg_dt
		                ,b.prvdr_fac_type_cd
		                ,b.rev_chrg_amt
		                ,b.rev_cd
		                ,b.prscrbng_prvdr_npi_num
		                ,b.srvcng_prvdr_spclty_cd
		                ,b.srvcng_prvdr_type_cd
					    ,b.alowd_amt
		            %end;
		            %if "&clm_file." = "ot" %then %do;
						,b.cms_64_fed_reimbrsmt_ctgry_cd               
		                ,b.othr_toc_rx_clm_actl_qty
						,b.srvc_bgnng_dt
		                ,b.srvc_endg_dt
		                ,b.prcdr_cd
		                ,b.prcdr_cd_ind
		                ,b.stc_cd
		                ,b.rev_cd
		                ,b.hcpcs_rate
		                ,b.srvcng_prvdr_num
		                ,b.srvcng_prvdr_spclty_cd
		                ,b.prscrbng_prvdr_npi_num
		                ,b.srvcng_prvdr_txnmy_cd
		                ,b.bill_amt
		                ,b.hcpcs_srvc_cd
		                ,b.hcpcs_txnmy_cd
		                ,b.bnft_type_cd
		                ,b.copay_amt
		                ,b.mdcr_pd_amt
		                ,b.othr_insrnc_amt
		                ,b.prcdr_1_mdfr_cd
		                ,b.prcdr_2_mdfr_cd
		                ,b.srvcng_prvdr_type_cd
		                ,b.tpl_amt
					    ,b.alowd_amt	
		            %end;
		            %if "&clm_file." = "rx" %then %do;
					    ,b.cms_64_fed_reimbrsmt_ctgry_cd
		                ,b.suply_days_cnt
		                ,b.othr_toc_rx_clm_actl_qty
		                ,b.ndc_cd
		                ,b.stc_cd
		                ,b.alowd_amt
		                ,b.bill_amt
		                ,b.brnd_gnrc_ind
		                ,b.copay_amt
		                ,b.dspns_fee_amt
		                ,b.mdcr_pd_amt
		                ,b.new_refl_ind
		                ,b.othr_insrnc_amt
		                ,b.rebt_elgbl_ind
		                ,b.tpl_amt
		            %end;
			            ,a.orgnl_clm_num_orig
			            ,a.adjstmt_clm_num_orig
			            ,a.adjdctn_dt_orig
			            ,a.adjstmt_ind_orig
			            ,a.clm_type_cd
			            ,a.adjstmt_ind
			            ,a.xovr_ind
			            ,a.tot_mdcd_pd_amt
			            ,a.tot_bill_amt
						,a.blg_prvdr_num
						,a.wvr_id
					    ,a.srvc_trkng_pymt_amt
					    ,a.srvc_trkng_type_cd
			            ,a.claim_cat_A
			            ,a.claim_cat_B
			            ,a.claim_cat_C
			            ,a.claim_cat_D
			            ,a.claim_cat_E
			            ,a.claim_cat_F
			            ,a.claim_cat_G
			            ,a.claim_cat_H
			            ,a.claim_cat_I
			            ,a.claim_cat_J
			            ,a.claim_cat_K
			            ,a.claim_cat_L
			            ,a.claim_cat_M
			            ,a.claim_cat_N
			            ,a.claim_cat_O
			            ,a.claim_cat_P
			            ,a.claim_cat_Q
			            ,a.claim_cat_R
			            ,a.claim_cat_S
			            ,a.claim_cat_T
			            ,a.claim_cat_U
			            ,a.claim_cat_V
			            ,a.claim_cat_W
			            ,a.claim_cat_X
			            ,a.claim_cat_Y
			            ,a.claim_cat_Z
			            ,a.claim_cat_AA
						,a.claim_cat_AB
						,a.claim_cat_AC
						,a.claim_cat_AD
			            ,a.claim_cat_AE
						,a.claim_cat_AF
						,a.claim_cat_AG
						,a.claim_cat_AH
						,a.claim_cat_AI
						,a.claim_cat_AJ
						,a.claim_cat_AK
						,a.claim_cat_AL
						,a.claim_cat_AM
						,a.claim_cat_AN
						,a.claim_cat_AO
						,a.claim_cat_AP
						,a.claim_cat_AQ
						,a.claim_cat_AR
						,a.claim_cat_AS
						,a.claim_cat_AT
						,a.claim_cat_AU
						,a.claim_cat_AV
						,a.claim_cat_AW

		            %if "&clm_file." = "ip" %then %do;
		                ,a.blg_prvdr_npi_num
		                ,a.prvdr_lctn_id
		                ,a.hosp_type_cd 
						,a.admsn_dt
		            %end;
		            %if "&clm_file." = "lt" %then %do;
		                ,a.nrsng_fac_days_cnt
		                ,a.mdcd_cvrd_ip_days_cnt
		                ,a.icf_iid_days_cnt
		                ,a.lve_days_cnt
		            %end;
		            %if "&clm_file." = "ot" %then %do;
		                ,a.srvc_plc_cd
		                ,a.dgns_1_cd
		                ,a.plan_id_num
		                ,a.blg_prvdr_npi_num
		                ,a.prvdr_lctn_id
		                ,a.othr_insrnc_ind
		                ,a.othr_tpl_clctn_cd
						,a.pgm_type_cd
						,a.bill_type_cd
		            %end;
		            %if "&clm_file." = "rx" %then %do;
		                ,a.prscrbng_prvdr_num
		                ,a.dspnsng_pd_prvdr_num
		            %end;

					from &temptable..&taskprefix._base_clh_&clm_file. a                    
					left join &temptable..&taskprefix._dup_cll_&clm_file. b
					on a.orgnl_clm_num = b.orgnl_clm_num
    				and a.adjstmt_clm_num = b.adjstmt_clm_num
    				and a.adjdctn_dt = b.adjdctn_dt
    				and a.adjstmt_ind = b.line_adjstmt_ind
					/* count_cll_key can be null for childless headers */
					where (b.count_cll_key = 1 or b.count_cll_key is null)
					cluster by orgnl_clm_num
				) by tmsis_passthrough;
				%countrecords(&temptable..&taskprefix._base_cll_&clm_file.);
				
			%tmsis_disconnect;

		quit;
		%status_check;

		%timestamp_log;

	%end;

%mend;

%macro create_msng_tbl(sgmt,el_flag);
      %droptemptables(msng_&sgmt.);
      execute(
            create table &temptable..&taskprefix._msng_&sgmt. as
            select *
                    ,%str(%')&state.%str(%') as submtg_state_cd            
			%if &el_flag. = 1 %then %do;
					,msis_id as msis_ident_num
			%end;
            from &permview..tmsis_&sgmt._view
            where %run_id_filter
			&limit.
      ) by tmsis_passthrough;
%mend;

%macro msis_id_not_missing();
    (msis_ident_num is not null
     and msis_ident_num not in ('88888888888888888888','99999999999999999999')
     and msis_ident_num rlike '[A-Za-z1-9]')
%mend;

%macro get_thresholds();

data thresholds (keep=Measure_ID Display_ID Measure_Type Claim_Category Active_Ind Display_Type Calculation_Source);
set thresh.measures;
rename
	'Measure ID'n=Measure_ID
	'Content Area Measure ID with Dis'n=Display_ID
	'Measure Type'n=Measure_Type
	'Claim Category'n=Claim_Category
	'Active Ind'n=Active_Ind
	'Display Type'n=Display_Type
	'Calculation Source'n=Calculation_Source
	;
run;

proc sort data=thresholds;
by measure_id;
run;

%mend;

%macro no_records(measure_id);
    y = cats('"',&measure_id.,'":[');
    put y;
    put '["No records meet the criteria"],';
    put '["No records meet the criteria"]';
    put '],';
%mend;

%macro droptemptables(tblList);
	
	%local i next_tbl;
	%do i=1 %to %sysfunc(countw(&tblList));
   		%let next_tbl = %scan(&tblList,  &i);
		/*execute(drop table if exists &temptable..&taskprefix._&next_tbl.) by tmsis_passthrough;*/

		title2 "Drop temp tables macro for &taskprefix._&next_tbl.";
		select count(1) as exists into :exists_&i. from connection to tmsis_passthrough
	   (show tables in &temptable. like %nrbquote('&taskprefix._&next_tbl.') );

	    %if &&exists_&i..=1 %then %do;
        
		   execute (
			   truncate table &temptable..&taskprefix._&next_tbl.
		   ) by tmsis_passthrough;
         
		   execute(
			   drop table &temptable..&taskprefix._&next_tbl.
		   ) by tmsis_passthrough;

		%end;

	%end;

%mend;

%macro dropwrktables(tblList);

	%local i next_tbl;
	%do i=1 %to %sysfunc(countw(&tblList));
   		%let next_tbl = %scan(&tblList,  &i);
		/*execute(drop table if exists &wrktable..&taskprefix._&next_tbl.) by tmsis_passthrough;*/

		title2 "Drop temp tables macro for &taskprefix._&next_tbl.";
				%let exists_&i=0;
        select count(1) as exists into :exists_&i. from connection to tmsis_passthrough
	    (show tables in &wrktable. like %nrbquote('&taskprefix._&next_tbl.') );

	    %if &&exists_&i..=1 %then %do;
        
		   execute (
			   truncate table &wrktable..&taskprefix._&next_tbl.
		   ) by tmsis_passthrough;
         
		   execute(
			   drop table &wrktable..&taskprefix._&next_tbl.
		   ) by tmsis_passthrough;

		%end;

	%end;

%mend;

%macro droptempviews(tblList);

	%local i next_tbl;
	%do i=1 %to %sysfunc(countw(&tblList));
   		%let next_tbl = %scan(&tblList,  &i);
		execute(drop view if exists &temptable..&taskprefix._&next_tbl.) by tmsis_passthrough;
	%end;

%mend;
%macro dropwrkviews(tblList);

	%local i next_tbl;
	%do i=1 %to %sysfunc(countw(&tblList));
   		%let next_tbl = %scan(&tblList,  &i);
		execute(drop view if exists &wrktable..&taskprefix._&next_tbl.) by tmsis_passthrough;
	%end;

%mend;

%macro countrecords(tbl);
	title2 "Count of records in &tbl.";
	select count_records format=comma18. from connection to tmsis_passthrough
	(select count(1) as count_records from &tbl.);
%mend;

%macro cache_claims_tables();

	sysecho "Caching claim lines";
	execute(cache table &temptable..&taskprefix._base_cll_ip) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_cll_lt) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_cll_ot) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_cll_rx) by tmsis_passthrough;

	sysecho "Caching claim headers";
	execute(cache table &temptable..&taskprefix._base_clh_ip) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_clh_lt) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_clh_ot) by tmsis_passthrough;
	execute(cache table &temptable..&taskprefix._base_clh_rx) by tmsis_passthrough;

%mend;

%macro schip_test(ds);
%if "&has_schip." = "NO" %then %do;
data &ds.;
    set &ds.;
    if substr(claim_category,1,6)='S-CHIP' and measure_type ne 'Frequency' then statistic = 'No S-CHIP Program';
run;
%end;
%mend schip_test;


%macro reshape(modulename,keyvar,keydesc);

    %if %upcase("&modulename.") = "TPL" or 
        /*%upcase("&modulename.") = "UTL" or */
        %upcase("&modulename.") = "FFS" %then %do;
        proc transpose  data=&modulename._msrs
                        out=&modulename._msrs_t (drop=_label_ rename=(col1=&m_col.))
                        name=msr_name;
        by submtg_state_cd;
        run;
    %end;
    %if %upcase("&modulename.") = "PRV" %then %do;
        data &modulename._msrs_t;
            set &modulename._msrs;
        run;
    %end;

    %let modulename2 = %upcase(&modulename.);
    /*%if %upcase("&modulename.") = "UTL" %then %do;
        %let modulename2 = ALL;
    %end;*/

    %if %upcase("&modulename.") = "MCRPLAN" or 
        %upcase("&modulename.") = "EXPPLAN" or 
        %upcase("&modulename.") = "ELGPLAN" %then %do;

        data &modulename.2;
           set plan_ids;
           length measure_id2 $9.;
           measure_id2=tranwrd(measure_id,'_','.');
           drop measure_id;
           rename measure_id2=measure_id;
        run;
        proc sort data=&modulename.2;
           by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") eq "MCR29" %then %do;
        data &modulename.2;
           set plan_ids_29;
           length measure_id2 $9.;
           measure_id2=tranwrd(measure_id,'_','.');
           drop measure_id;
           rename measure_id2=measure_id;
        run;
        proc sort data=&modulename.2;
           by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") ne "MCR" and 
        %upcase("&modulename.") ne "EXP" and
        %upcase("&modulename.") ne "ELG" and
        %upcase("&modulename.") ne "UTL" and       
        %upcase("&modulename.") ne "MIS" %then %do; /* elig71, FFS, prv, tpl*/
    data numer (rename=(&m_col.= numerator))
         denom (rename=(&m_col.= denominator))
         value (rename=(&m_col. = statistic));

        set &modulename._msrs_T;

        regex1 = prxparse('/(\D+)(\d+)_(\d+)_(\w+)/');
        regex2 = prxparse('/(\D+)_(\d+)_(\w+)/');
        if prxmatch(regex1,msr_name) then do;
            measure_id = cats(upcase(prxposn(regex1,1,msr_name)),prxposn(regex1,2,msr_name),
                        '.',prxposn(regex1,3,msr_name));
            if index(upcase(prxposn(regex1,4,msr_name)),'DEN') > 0 then do;
                if missing(&m_col.) then &m_col. = 0;
                output denom;
            end;
            else if index(upcase(prxposn(regex1,4,msr_name)),'NUM') > 0 then output numer;
        end;
        else if prxmatch(regex2,msr_name) then do;
            measure_id = cats(upcase(prxposn(regex2,1,msr_name)),'.',prxposn(regex2,2,msr_name));
            if index(upcase(prxposn(regex2,3,msr_name)),'DEN') > 0 then do;
                if missing(&m_col.) then &m_col. = 0;
                output denom;
            end;
            else if index(upcase(prxposn(regex2,3,msr_name)),'NUM') > 0 then output numer;
        end;
        else do;
            measure_id = upcase(tranwrd(msr_name,'_','.'));
            output value;
        end;
    run;
    proc sort data=numer (keep=measure_id &keyvar. numer:);
    by measure_id &keyvar.;
    run;

    proc sort data=denom (keep=measure_id &keyvar. denom:);
    by measure_id &keyvar.;
    run;

    proc sort data=value (drop=msr_name regex1 regex2);
    by measure_id &keyvar.;
    run;

    data &modulename.2;
        merge value numer denom;
        by measure_id &keyvar.;
        format numerator denominator 18.;
    run;
    %end;
    %else %if %upcase("&modulename.") = "MCR" %then %do;
        proc sort data=ffs_mcr_900 out=&modulename.2;
        by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") = "EXP" %then %do;
        proc sort data=exp_200 out=&modulename.2;
        by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") = "UTL" %then %do;
        proc sort data=utl_700 out=&modulename.2;
        by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") = "ELG" %then %do;
        proc sort data=elg_100 out=&modulename.2;
        by measure_id;
        run;
    %end;
    %else %if %upcase("&modulename.") = "MIS" %then %do;
        proc sort data=mis_800 out=&modulename.2;
        by measure_id;
        run;
    %end;

	%if %upcase("&modulename.") = "FFS" %then %do;
		data &modulename.2;
			length valid_value $10.;
			set &modulename.2
				FFS_Freqs (keep=measure_id submtg_state_cd statistic valid_value)
				;
		run;

		proc sort data=&modulename.2;
		by measure_id;
		run;
	%end;

    /*
	%if %upcase("&modulename") = ("UTL") %then %do;
		data &modulename.2;
			length valid_value $10.;
			set &modulename.2
				utl_msrs_freq (keep=measure_id submtg_state_cd statistic valid_value)
				;
            length ft $2;
            ft = upcase(scan(measure_id, 3, '.'));
            if ft in ('IP', 'LT', 'OT', 'RX') then
              measure_id = substr(measure_id, 1, index(measure_id, catx('.', ft))-2);

		run;

		proc sort data=&modulename.2;
		by measure_id;
		run;
	%end;*/

    %get_thresholds;

    data &modulename.thresh /*(drop=measure_type)*/;
        merge &modulename.2 (in=a)
              thresholds (in=b);
        by measure_id;

        in_measures = a;
		in_thresholds = b;
/*
        %if %upcase("&modulename.") = "EXPPLAN" or %upcase("&modulename.") = "MCRPLAN"
            or %upcase("&modulename.") = "MCR29" or %upcase("&modulename.") = "ELGPLAN" %then %do;
            if a and b;
        %end;
*/
        length cstat $18.;

        if upcase(measure_type) in ('SUM','COUNT') and
           missing(statistic) then cstat = '0';
        %if %upcase("&modulename.") = "PRV" or %upcase("&modulename.") = "TPL"
            or %upcase("&modulename.") = "UTL" or %upcase("&modulename.") ="FFS"
            %then %do;

		else if missing(statistic) and missing(numerator) and  denominator>0 then cstat = '0';

        %end;

        %if %upcase("&modulename.") = "MCR" or %upcase("&modulename.") = "EXP"
            or %upcase("&modulename.") = "MIS" /*or %upcase("&modulename.") = "UTL"*/ %then %do;

		else if missing(statistic) and missing(numer) and  denom>0 then cstat = '0';

        %end;
        %if %upcase("&modulename.") = "ELG"  %then %do;
		else if missing(statistic) and missing(numerator) and  denominator>0 then cstat = '0';
		else if measure_id in ('EL3.19','EL3.20','EL3.21',
                               'EL3.22','EL3.23','EL3.24') and missing(statistic) and  denominator=0 then cstat = 'N/A';

        %end;
        
        else if missing(statistic) then cstat = 'Div by 0';
        %if %upcase("&modulename.") ne "MCRPLAN" and %upcase("&modulename.") ne "MCR29"
        and %upcase("&modulename.") ne "ELGPLAN" %then %do;
        else if statistic > 999 or statistic < -999 then cstat = put(statistic,comma18.);
        else cstat = put(statistic,best18.);
        %end;
        %else %do;
        else cstat=statistic;
        %end;
        Report_State = "&stabbrev.";
        Month_Added = put(today(),yymmn6.);
        Statistic_Year_Month = input(&m_start.,yymmdd10.);
        drop statistic
             submtg_state_cd;
        format statistic_year_month yymmdd10.;

        rename  cstat=statistic
            %if %length(&keyvar.) > 0 %then %do;
                &keyvar. = valid_value
                &keydesc. = freq_descrpt
            %end;
            ;
    run;

	
	proc sql;
	create table measure_check as
	select 
		distinct 
			 measure_id
			,display_id
			,claim_category
			,measure_type
			,display_type
			,active_ind
			,calculation_source
			,in_measures
			,in_thresholds
	from &modulename.thresh
	where (upcase(substr(measure_id,1,2)) = "%substr(%upcase(&modulename.),1,2)" and in_measures = 1 and in_thresholds = 0)
	or (
		upcase(substr(measure_id,1,2)) = "%substr(%upcase(&modulename.),1,2)"
		and calculation_source = 'SAS' and (in_thresholds = 1 and upcase(active_ind) = 'Y' and in_measures = 0)
    	%if %upcase("&modulename.") = "MCRPLAN" or %upcase("&modulename.") = "EXPPLAN"
        	or %upcase("&modulename.") = "ELGPLAN"  %then %do;
				and upcase(display_type) not in ('DEFAULT','FREQUENCY','EL7.1','MCR29.1','MCR29.2')
		%end;
		%else %if %upcase("&modulename.") = "MCR29" %then %do;
			and upcase(display_type) in ('MCR29.1','MCR29.2')
		%end;
		%else %do;
			and upcase(display_type) in ('DEFAULT','FREQUENCY')
		%end;
		)
	%if %upcase("&modulename.") = "MCR" %then %do;
		or (upcase(substr(measure_id,1,3)) = "FFS" and in_measures = 1 and in_thresholds = 0)
	%end;
	;

	%let count_errs=0;
	title1 "Count of errors in measures check against thresholds for &modulename.";
	proc sql;
	select count(distinct measure_id)
	into :count_errs
	from measure_check
	;
	quit;
	run;
	title;

	%if &count_errs > 0 %then %do;
		%if &count_errs = 1 %then %put ERR%str(OR): There was &count_errs error in the measures check against the thresholds.;
		%else %put ERR%str(OR): There were &count_errs errors in the measures check against the thresholds.;
		title1 "print of measures check against thresholds doc for &modulename.";
		proc print data = measure_check;
		run;
		title;

	%end;

	data &modulename.2thresh;
		set &modulename.thresh;
		if upcase(active_ind) = 'Y' and in_thresholds=1 and in_measures=1 and calculation_source = 'SAS';
		drop measure_id;
		rename display_id=measure_id;
	run;

    %schip_test(&modulename.2thresh);
    %if %upcase("&modulename.") = "MCRPLAN" or %upcase("&modulename.") = "EXPPLAN"
        or %upcase("&modulename.") = "ELGPLAN"  %then %do;

    data
      %if &separate_entity.=1 or &separate_entity.=2 %then %do;
			dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id. (drop=valid_value freq_descrpt);
	  %end;
	  %else %do;
			dqout.&modulename._&stabbrev._&rpt_month._run&run_id. (drop=valid_value freq_descrpt);
	  %end;

    	set &modulename.2thresh;
    	drop claim_category;
     run;
		%if &separate_entity.=1 or &separate_entity.=2 %then %do;
    		proc sort data=dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id.;
    		by measure_id;
    		run;
    	%end;
		%else %do;
	 		proc sort data=dqout.&modulename._&stabbrev._&rpt_month._run&run_id.;
    		by measure_id;
    		run;
		%end;

    %end;
    %else %if %upcase("&modulename.") = "MCR29" %then %do;
        data 
           %if &separate_entity.=1 or &separate_entity.=2 %then %do;
				dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id. (drop=valid_value freq_descrpt);
		   %end;
		   %else %do;
				dqout.&modulename._&stabbrev._&rpt_month._run&run_id. (drop=valid_value freq_descrpt);
		   %end;
        set &modulename.2thresh;
        drop claim_category;
        run;

        proc sort data=
			%if &separate_entity.=1 or &separate_entity.=2 %then %do;
                 dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id.;
			%end;
			%else %do;
				 dqout.&modulename._&stabbrev._&rpt_month._run&run_id.;
			%end;
        by measure_id;
        run;
    %end;
    %else %do;
    data 
        %if &separate_entity.=1 or &separate_entity.=2 %then %do;
			dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id.;
		%end;
		%else %do;
			dqout.&modulename._&stabbrev._&rpt_month._run&run_id.;
		%end;
    retain Report_State Month_Added Measure_ID Statistic_Year_Month Statistic
           Numerator Denominator;
    set &modulename.2thresh;
    drop claim_category freq_descrpt;
    run;

    proc sort data=
        %if &separate_entity.=1 or &separate_entity.=2 %then %do;
			dqout.&modulename._&stabbrev._&typerun._&rpt_month._run&run_id.;
		%end;
		%else %do;
			dqout.&modulename._&stabbrev._&rpt_month._run&run_id.;
		%end;

    by measure_id;
    run;
    %end;

%mend reshape;

/*************************************
  Export to excel
 *************************************/
%macro exportexcel();
/*all regular measures*/
data dq_all;
  format Report_State $4. valid_value $10.;
  length Report_State $4. valid_value $10.;
  set 

  %if &separate_entity.=1 or &separate_entity.=2 %then %do;
      dqout.elg_&stabbrev._&typerun._&rpt_month._run&run_id. 
      dqout.exp_&stabbrev._&typerun._&rpt_month._run&run_id. (rename= (numer=numerator denom = denominator))
	  dqout.mcr_&stabbrev._&typerun._&rpt_month._run&run_id.
	  dqout.prv_&stabbrev._&typerun._&rpt_month._run&run_id. 
	  dqout.tpl_&stabbrev._&typerun._&rpt_month._run&run_id. 
	  dqout.utl_&stabbrev._&typerun._&rpt_month._run&run_id. 
	  %if &msng_dq_run_flag.=1 %then %do;
         dqout.mis_&stabbrev._&typerun._&rpt_month._run&run_id.
	  %end;
   %end;

   %else %do;

      dqout.elg_&stabbrev._&rpt_month._run&run_id. 
      dqout.exp_&stabbrev._&rpt_month._run&run_id. (rename= (numer=numerator denom = denominator))
	  dqout.mcr_&stabbrev._&rpt_month._run&run_id.
	  dqout.prv_&stabbrev._&rpt_month._run&run_id. 
	  dqout.tpl_&stabbrev._&rpt_month._run&run_id. 
	  dqout.utl_&stabbrev._&rpt_month._run&run_id. 
	  %if &msng_dq_run_flag.=1 %then %do;
         dqout.mis_&stabbrev._&rpt_month._run&run_id.
	  %end;
    
   %end;
;

	  length SpecVersion $6. RunID $5.;
	  SpecVersion="&specvrsn.";
	  RunID="&run_id.";

	  %if &separate_entity.=1 or &separate_entity.=2 %then %do;
	     Report_State="&stabbrev.-&typerun.";
      %end;


  format statistic_year_month mmddyy10.;
run;
data dq_all;
retain 
        Report_State Month_Added Measure_ID	
        Statistic_Year_Month Statistic Numerator
        Denominator valid_value	SpecVersion	RunID
		;
set dq_all;
run;
%if (&separate_entity.=. or &separate_entity.=0) and &msng_dq_run_flag.=1 %then %do;
proc export data= dq_all outfile="&txtout./MACBIS_DQ_&stabbrev._&rpt_fldr._run&run_id._&sysdate._all_IM" dbms=xlsx replace;
run;
%end;

%else %if (&separate_entity.=. or &separate_entity.=0) and &msng_dq_run_flag. ne 1 %then %do;
proc export data= dq_all outfile="&txtout./MACBIS_DQ_&stabbrev._&rpt_fldr._run&run_id._&sysdate._all_EM" dbms=xlsx replace;
run;
%end;

%else %if &separate_entity.=1 or &separate_entity.=2 and &msng_dq_run_flag.=1 %then %do;
proc export data= dq_all outfile="&txtout./MACBIS_DQ_&stabbrev.-&typerun._&rpt_fldr._run&run_id._&sysdate._all_IM" dbms=xlsx replace;
run;
%end;
%else %if &separate_entity.=1 or &separate_entity.=2 and &msng_dq_run_flag. ne 1 %then %do;
proc export data= dq_all outfile="&txtout./MACBIS_DQ_&stabbrev.-&typerun._&rpt_fldr._run&run_id._&sysdate._all_EM" dbms=xlsx replace;
run;
%end;

/*measure el71*/
data dq_elg71;
format Report_State $4.;
length Report_State $4;

 %if &separate_entity.=1 or &separate_entity.=2 %then %do;
    set dqout.elg71_&stabbrev._&typerun._&rpt_month._run&run_id. ;
  %end;
  %else %do;
      set dqout.elg71_&stabbrev._&rpt_month._run&run_id. ;
  %end;

  measure_id = "EL-7-001-1";
  drop measure;

   length SpecVersion $6. RunID $5.;
	  SpecVersion="&specvrsn.";
	  RunID="&run_id.";

	   %if &separate_entity.=1 or &separate_entity.=2 %then %do;
	     Report_State="&stabbrev.-&typerun.";
      %end;
  _statistic_year_month = input(statistic_year_month, monyy7.);
  format _statistic_year_month mmddyy10.;
  drop statistic_year_month;
  rename _statistic_year_month = statistic_year_month; 

run;

data dq_elg71;
retain 
      waiver_id	waiver_type	
      submtg_state_cd statistic_type measure_id
      statistic Report_State Month_Added 
      SpecVersion RunID statistic_year_month ;
set dq_elg71;
run;

%if &separate_entity.=. or &separate_entity.=0 %then %do;
proc export data= dq_elg71 outfile="&txtout./MACBIS_DQ_&stabbrev._&rpt_fldr._run&run_id._&sysdate._elg71" dbms=xlsx replace;
run;
%end;
%else %if &separate_entity.=1 or &separate_entity.=2 %then %do;
proc export data= dq_elg71 outfile="&txtout./MACBIS_DQ_&stabbrev.-&typerun._&rpt_fldr._run&run_id._&sysdate._elg71" dbms=xlsx replace;
run;
%end;



/*plan id measures*/
data dq_planid;
  format Report_State $4.;
  length Report_State $4;

   %if &separate_entity.=1 or &separate_entity.=2 %then %do;
       set dqout.elgplan_&stabbrev._&typerun._&rpt_month._run&run_id. (drop = measure);
    %end;
	%else %do;
        set dqout.elgplan_&stabbrev._&rpt_month._run&run_id. (drop = measure);
	%end;
 
  ;

   length SpecVersion $6. RunID $5.;
	  SpecVersion="&specvrsn.";
	  RunID="&run_id.";

	   %if &separate_entity.=1 or &separate_entity.=2 %then %do;
	     Report_State="&stabbrev.-&typerun.";
      %end;
  

  rename linked = In_MCR_File;

  format statistic_year_month mmddyy10.;
run;

data dq_planid;
retain 
      plan_id plan_type_el multipleplantypes_el plan_type_mc
      multipleplantypes_mc In_MCR_File statistic_type measure_id
      statistic Report_State Month_Added Statistic_Year_Month
      SpecVersion RunID  ;
set dq_planid;
run;

%if &separate_entity.=. or &separate_entity.=0 %then %do;
proc export data= dq_planid outfile="&txtout./MACBIS_DQ_&stabbrev._&rpt_fldr._run&run_id._&sysdate._planid" dbms=xlsx replace;
run;
%end;
%else %if &separate_entity.=1 or &separate_entity.=2 %then %do;
proc export data= dq_planid outfile="&txtout./MACBIS_DQ_&stabbrev.-&typerun._&rpt_fldr._run&run_id._&sysdate._planid" dbms=xlsx replace;
run;
%end;
%mend;
