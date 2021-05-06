/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/* macros that check missing values */

%macro miss_misslogic(var);      
     ( not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic;
 
 %macro miss_misslogic_c6(var);      
     ( &var. ='6' or
       not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_c6;

  %macro miss_misslogic_c017(var);      
     ( &var. ='017' or
       not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_c017;

 %macro miss_misslogic_cU(var);      
     ( &var. = 'U' or
       not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_cU;

%macro miss_misslogic_c9(var);      
     ( &var. ='9' or
       not &var. rlike '[a-zA-Z1-8]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_c9;

%macro miss_misslogic_c88_99(var);      
     ( &var. ='88' or
       &var. ='99' or
       not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_c88_99;

 %macro miss_misslogic_c88(var);      
     ( &var. ='88' or
       not &var. rlike '[a-zA-Z1-9]' or 
	   &var. is null
	  )     
 %mend miss_misslogic_c88;

 %macro miss_misslogic_ex000(var);      
     ( (&var. <> '000' and (not &var. rlike '[a-zA-Z1-9]'))
        or 
	   &var. is null
	  )     
 %mend miss_misslogic_ex000;

proc sql;
%tmsis_connect;

/******* 1. Connect to AREMAC database and create the below AREMAC tables  *******/

%create_msng_tbl(prmry_dmgrphc_elgblty,1);
%create_msng_tbl(var_dmgrphc_elgblty,1);
%create_msng_tbl(elgbl_cntct,1);
%create_msng_tbl(elgblty_dtrmnt,1);
%create_msng_tbl(hh_sntrn_prtcptn_info,1);
%create_msng_tbl(hh_sntrn_prvdr,1);
%create_msng_tbl(hh_chrnc_cond,1);
%create_msng_tbl(lckin_info,1);
%create_msng_tbl(mfp_info,1);
%create_msng_tbl(state_plan_prtcptn,1);
%create_msng_tbl(wvr_prtcptn_data,1);
%create_msng_tbl(ltss_prtcptn_data,1);
%create_msng_tbl(mc_prtcptn_data,1);
%create_msng_tbl(ethncty_info,1);
%create_msng_tbl(race_info,1);
%create_msng_tbl(dsblty_info,1);
%create_msng_tbl(sect_1115a_demo_info,1);
%create_msng_tbl(hcbs_chrnc_cond_non_hh,1);
%create_msng_tbl(enrlmt_time_sgmt_data,1);
%create_msng_tbl(mc_mn_data,0);
%create_msng_tbl(mc_lctn_cntct,0);
%create_msng_tbl(mc_sarea,0);
%create_msng_tbl(mc_oprtg_authrty,0);
%create_msng_tbl(mc_plan_pop_enrld,0);
%create_msng_tbl(mc_acrdtn_org,0);
%create_msng_tbl(chpid_shpid_rltnshp_data,0);
%create_msng_tbl(prvdr_attr_mn,0);
%create_msng_tbl(prvdr_lctn_cntct,0);
%create_msng_tbl(prvdr_lcnsg,0);
%create_msng_tbl(prvdr_id,0);
%create_msng_tbl(prvdr_txnmy_clsfctn,0);
%create_msng_tbl(prvdr_mdcd_enrlmt,0);
%create_msng_tbl(prvdr_afltd_grp,0);
%create_msng_tbl(prvdr_afltd_pgm,0);
%create_msng_tbl(prvdr_bed_type,0);
%create_msng_tbl(tpl_mdcd_prsn_mn,1);



sysecho "running 801";
%include "&progpath./&module./801_createMissingnessXlsx.sas";  %_801; %timestamp_log;

sysecho "running 802";
%include "&progpath./&module./802_miss_claims_pct.sas";        %_802; %timestamp_log;

sysecho "running 803";
%include "&progpath./&module./803_miss_non_claims_pct.sas";    %_803; %timestamp_log;


/******* 3. Diconnect from AREMAC once the measures are extracted *******/
/*----------------------------------------------------------------------------------------*
  End of AREMAC processing - all measures are now extracted at the measure level
  into SAS. Disconnect from tmsis.
 *----------------------------------------------------------------------------------------*/
	 %tmsis_disconnect;
quit;
%status_check;

/*****************************************************************************************
 * Join all_miss_claims and all_miss_non_claims
******************************************************************************************/

proc sql;
   create table all_miss_both_claims as
      select * from all_miss_claims
	  union all
	      select * from all_miss_non_claims
	  order by measure_id
;
quit;

data all_miss_both_claims;
    length measure_id $20;
    format measure_id $20.;
  	set 
	    all_miss_both_claims ;
	mvalue = round(mvalue,.001);
run;

data MIS_800;
   
	set all_miss_both_claims(drop=miss_position miss_varname)  ;

    submtg_state_cd = "&state.";

    measure_id = tranwrd(measure_id,'_','.');

    rename numer=numerator
  	       denom=denominator
		   mvalue=statistic
		 ;
run;

%reshape(MIS,);

%timestamp_log;

/*this forces the timestamp to be printed*/
data _null_;
run;




