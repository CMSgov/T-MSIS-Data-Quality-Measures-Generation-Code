
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro tpl_clm_sql();

%macro tpl_clm(clmtyp, clmlvl, clmcat, tblnum, msr1, msr2);

%dropwrktables(prep_clm_&clmcat. clm_&clmcat.);

execute(
create table &wrktable..&taskprefix._prep_clm_&clmcat. as
    select

        /*unique keys and other identifiers*/
         submtg_state_cd
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
        %if "&clmlvl." = "cll" %then %do;
    		,orgnl_line_num
            ,adjstmt_line_num
    		,line_adjstmt_ind
        %end;
        %else %do;
            ,adjstmt_ind
        %end;
        ,max(case when (othr_insrnc_ind = '1') then 1 else 0 end) as othr_ins
        ,max(case when (othr_tpl_clctn_cd in ('001', '002', '003','004', '005', '006','007'))
                                           then 1 else 0 end) as othr_tpl

    from &temptable..&taskprefix._base_&clmlvl._&clmtyp.
    where claim_cat_&clmcat. = 1
    %if "&clmlvl." = "cll" %then %do; 
		and childless_header_flag = 0
	%end;
	group by submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
        %if "&clmlvl." = "cll" %then %do; 
			 orgnl_line_num, adjstmt_line_num, line_adjstmt_ind
        %end;
        %else %do;
            adjstmt_ind
        %end;
	) by tmsis_passthrough;

    execute(
    create table &wrktable..&taskprefix._clm_&clmcat. as
    select
         submtg_state_cd
        ,sum(othr_ins) as tpl&tblnum._&msr1._numer
        ,count(submtg_state_cd) as tpl&tblnum._&msr1._denom
        ,round((sum(othr_ins) / count(submtg_state_cd)),4) as tpl&tblnum._&msr1.

        ,sum(othr_tpl) as tpl&tblnum._&msr2._numer
        ,count(submtg_state_cd) as tpl&tblnum._&msr2._denom
        ,round((sum(othr_tpl) / count(submtg_state_cd)),4) as tpl&tblnum._&msr2.


    from &wrktable..&taskprefix._prep_clm_&clmcat.
    group by submtg_state_cd
	) by tmsis_passthrough;

    %dropwrktables(prep_clm_&clmcat.);

%mend tpl_clm;

%macro combine_tpl_clm(clmtyp, msr1, msr2);

    %dropwrktables(tpl_&clmtyp.);
    
    execute(
    create table &wrktable..&taskprefix._tpl_&clmtyp. as 
    select
    	 u.submtg_state_cd
    	 %do i = 2 %to 5;
    		,tpl&i._&msr1._numer
    		,tpl&i._&msr1._denom
    		,tpl&i._&msr1.
    		,tpl&i._&msr2._numer
    		,tpl&i._&msr2._denom
    		,tpl&i._&msr2.
    	%end;
    from (select submtg_state_cd from &wrktable..&taskprefix._clm_a  
          union select submtg_state_cd from &wrktable..&taskprefix._clm_p  
		  union select submtg_state_cd from &wrktable..&taskprefix._clm_f
          union select submtg_state_cd from &wrktable..&taskprefix._clm_r) u 
    left join &wrktable..&taskprefix._clm_a a on u.submtg_state_cd = a.submtg_state_cd
    left join &wrktable..&taskprefix._clm_p p on u.submtg_state_cd = p.submtg_state_cd
	left join &wrktable..&taskprefix._clm_f f on u.submtg_state_cd = f.submtg_state_cd
    left join &wrktable..&taskprefix._clm_r r on u.submtg_state_cd = r.submtg_state_cd
    ) by tmsis_passthrough;
    
    %dropwrktables(clm_a clm_p clm_f clm_r);
    
%mend combine_tpl_clm;


%tpl_clm(ip, clh, a, 2, 1, 2);
%tpl_clm(ip, clh, p, 3, 1, 2);
%tpl_clm(ip, clh, f, 4, 1, 2);
%tpl_clm(ip, clh, r, 5, 1, 2);

%combine_tpl_clm(ip, 1, 2);

%tpl_clm(lt, clh, a, 2, 3, 4);
%tpl_clm(lt, clh, p, 3, 3, 4);
%tpl_clm(lt, clh, f, 4, 3, 4);
%tpl_clm(lt, clh, r, 5, 3, 4);

%combine_tpl_clm(lt, 3, 4);

%tpl_clm(ot, cll, a, 2, 5, 6);
%tpl_clm(ot, cll, p, 3, 5, 6);
%tpl_clm(ot, cll, f, 4, 5, 6);
%tpl_clm(ot, cll, r, 5, 5, 6);

%combine_tpl_clm(ot, 5, 6);

%tpl_clm(rx, clh, a, 2, 7, 8);
%tpl_clm(rx, clh, p, 3, 7, 8);
%tpl_clm(rx, clh, f, 4, 7, 8);
%tpl_clm(rx, clh, r, 5, 7, 8);

%combine_tpl_clm(rx, 7, 8);

%mend tpl_clm_sql;



