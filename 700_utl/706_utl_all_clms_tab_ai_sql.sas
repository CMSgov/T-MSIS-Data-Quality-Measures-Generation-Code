
/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%macro utl_all_clms_ai_sql;


%macro utl_ai_clm(clm, clmcat, n1, n2);

%if &clm.=ip %then %do;
   %let dtvar=admsn_dt;
%end;
%else %if &clm.=rx %then %do;
   %let dtvar=rx_fill_dt ;
%end;
%else %do;
   %let dtvar=srvc_bgnng_dt  ;
%end;

%let tblList = &clm._hdr_&clmcat._ever_elig &clm._hdr_&clmcat._ever_elig2 
               &clm._bene_&clmcat.;

         
 
	/*merge to ever eligible **/

  	execute(
	create or replace temporary view &taskprefix._&clm._hdr_&clmcat._ever_elig as
	select          
		 a.submtg_state_cd
		,a.msis_ident_num
		,a.tmsis_rptg_prd 
        ,orgnl_clm_num
        ,adjstmt_clm_num
        ,adjdctn_dt
		,adjstmt_ind
        ,&dtvar. 
	    ,case when b.ever_eligible=1 then 1 else 0 end as ever_elg
    from &temptable..&taskprefix._base_clh_&clm.  a
	left join
          (select distinct msis_ident_num, ever_eligible
		   from &temptable..&taskprefix._ever_elig) b
	on    a.msis_ident_num=b.msis_ident_num
	where a.claim_cat_&clmcat. = 1
	and a.&dtvar. is not null
	) by tmsis_passthrough;
	
	/**merge to el determinant file and apply restrictions */
    execute(
	create or replace temporary view &taskprefix._&clm._hdr_&clmcat._ever_elig2 as
	select
		    c.*
		   ,case when ever_elg =1 and ever_elg_dtrmnt =1 and non_dual_flag=1 then 1 else 0 end as all14_&n1._numer
		   ,case when ever_elg =1 and ever_elg_dtrmnt =1 and dual_premium_flag=1 then 1 else 0 end as all14_&n2._numer
	from 
		(select
			a.*
	       ,b.elgblty_dtrmnt_efctv_dt
		   ,b.elgblty_dtrmnt_end_dt
		   ,case when   (a.&dtvar.>=b.elgblty_dtrmnt_efctv_dt and a.&dtvar. is not null ) and 
						(a.&dtvar.<=b.elgblty_dtrmnt_end_dt or b.elgblty_dtrmnt_end_dt is null) then 1 else 0 end as ever_elg_dtrmnt      

	       ,case when b.dual_elgbl_cd not in ('01','02','04','08') or  
                      b.dual_elgbl_cd  is null then 1 else 0 end as non_dual_flag
	       ,case when b.dual_elgbl_cd in ('03','05','06') then 1 else 0 end as dual_premium_flag

	    from &taskprefix._&clm._hdr_&clmcat._ever_elig a
	    left join &temptable..&taskprefix._ever_elig_dtrmnt b
		on    a.submtg_state_cd=b.submtg_state_cd and
		      a.msis_ident_num=b.msis_ident_num) c

    ) by tmsis_passthrough;

	/*now rolling upto one record per msis id*/

execute(
    create or replace temporary view &taskprefix._&clm._bene_&clmcat. as
    select
         submtg_state_cd
        ,msis_ident_num
	    ,max(all14_&n1._numer) as all14_&n1._numer_max
	    ,max(all14_&n2._numer) as all14_&n2._numer_max
	
    from &taskprefix._&clm._hdr_&clmcat._ever_elig2
    group by submtg_state_cd,msis_ident_num
    ) by tmsis_passthrough;


execute(
    create or replace temporary view &taskprefix._utl_&clm._pct_&clmcat. as
    select
		 a.*
        ,case when all14_&n1._denom >0 then round((all14_&n1._numer /all14_&n1._denom),2) 
              else null end as all14_&n1.
		,case when all14_&n2._denom >0 then round((all14_&n2._numer /all14_&n2._denom),2) 
              else null end as all14_&n2.
	from
		(select	
		     submtg_state_cd
			,count(submtg_state_cd) as all14_&n1._denom
			,count(submtg_state_cd) as all14_&n2._denom
	        ,sum(all14_&n1._numer_max) as all14_&n1._numer
	        ,sum(all14_&n2._numer_max) as all14_&n2._numer
	    from &taskprefix._&clm._bene_&clmcat.
	    group by submtg_state_cd
		) a
	) by tmsis_passthrough;

/*	%local obs;

	select obs into :obs from connection to tmsis_passthrough
	(select count(1) as obs from #temp.&taskprefix._utl_&clm._pct_&clmcat.);

	%if &obs. = 0 %then %do;
		execute(
			insert into #temp.&taskprefix._utl_&clm._pct_&clmcat. 
				values(
					%str(%')&state.%str(%'),
					null,null,null,null,null,null
					);
		) by tmsis_passthrough;
	%end;*/


%mend ;


%utl_ai_clm(ip, ai, 1, 5);
%utl_ai_clm(lt, ai, 2, 6);
%utl_ai_clm(ot, ai, 3, 7);
%utl_ai_clm(rx, ai, 4, 8);


execute(

    insert into &utl_output

    select
    submtg_state_cd
    , 'all14_1'
    , '706'
    ,all14_1_numer
    ,all14_1_denom
    ,all14_1
    , null
    , null    
    from #temp.&taskprefix._utl_ip_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all14_5'
    , '706'
    ,all14_5_numer
    ,all14_5_denom
    ,all14_5
    , null
    , null    
    from #temp.&taskprefix._utl_ip_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
    , 'all14_2'
    , '706'
    ,all14_2_numer
    ,all14_2_denom
    ,all14_2
    , null
    , null    
    from #temp.&taskprefix._utl_lt_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all14_6'
    , '706'
    ,all14_6_numer
    ,all14_6_denom
    ,all14_6
    , null
    , null    
    from #temp.&taskprefix._utl_lt_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
    , 'all14_3'
    , '706'
    ,all14_3_numer
    ,all14_3_denom
    ,all14_3
    , null
    , null    
    from #temp.&taskprefix._utl_ot_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all14_7'
    , '706'
    ,all14_7_numer
    ,all14_7_denom
    ,all14_7
    , null
    , null    
    from #temp.&taskprefix._utl_ot_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 
    
    select
    submtg_state_cd
    , 'all14_4'
    , '706'
    ,all14_4_numer
    ,all14_4_denom
    ,all14_4
    , null
    , null    
    from #temp.&taskprefix._utl_rx_pct_ai

     ) by tmsis_passthrough; 

execute ( 
    insert into &utl_output 

    select
    submtg_state_cd
    , 'all14_8'
    , '706'
    ,all14_8_numer
    ,all14_8_denom
    ,all14_8
    , null
    , null    
    from #temp.&taskprefix._utl_rx_pct_ai

) by tmsis_passthrough;

    %insert_msr(msrid=all14_1);
    %insert_msr(msrid=all14_5);
    %insert_msr(msrid=all14_2);
    %insert_msr(msrid=all14_6);
    %insert_msr(msrid=all14_3);
    %insert_msr(msrid=all14_7);
    %insert_msr(msrid=all14_4);
    %insert_msr(msrid=all14_8);

%mend;
