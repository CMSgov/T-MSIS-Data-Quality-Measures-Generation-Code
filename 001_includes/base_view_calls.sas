/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

%include "/sasdata/users/&sysuserid./tmsisshare/prod/01_AREMAC/global/task12_databricks_connection.sas" /source2;
%include "/sasdata/users/&sysuserid./tmsisshare/prod/01_AREMAC/Task_12/DQ_SAS/001_includes/base_view_defs.sas" /source2;

%let permview = macbis_t12_perm;

%macro msis_id_not_missing();
    (msis_ident_num is not null
     and msis_ident_num not in ('88888888888888888888','99999999999999999999')
     and msis_ident_num rlike '[A-Za-z1-9]')
%mend;


proc sql;

	%tmsis_connect;

 		%create_base_elig_views;

		%create_base_cll_view(ip,ip);
		%create_base_cll_view(lt,lt);
		%create_base_cll_view(ot,othr_toc);
		%create_base_cll_view(rx,rx);

		%create_base_clh_view(ip,ip);
		%create_base_clh_view(lt,lt);
		%create_base_clh_view(ot,othr_toc);
		%create_base_clh_view(rx,rx);

		%create_base_elig_info_view(tmsis_sect_1115a_demo_info);
	    %create_base_elig_info_view(tmsis_dsblty_info);
	    %create_base_elig_info_view(tmsis_elgblty_dtrmnt);
	    %create_base_elig_info_view(tmsis_elgbl_cntct); 
		%create_base_elig_info_view(tmsis_ethncty_info);
	    %create_base_elig_info_view(tmsis_hcbs_chrnc_cond_non_hh);
	    %create_base_elig_info_view(tmsis_hh_chrnc_cond);
	    %create_base_elig_info_view(tmsis_hh_sntrn_prtcptn_info);     
		%create_base_elig_info_view(tmsis_hh_sntrn_prvdr);     
	    %create_base_elig_info_view(tmsis_lckin_info);
	    %create_base_elig_info_view(tmsis_ltss_prtcptn_data);
		%create_base_elig_info_view(tmsis_state_plan_prtcptn);
	    %create_base_elig_info_view(tmsis_mc_prtcptn_data);
	    %create_base_elig_info_view(tmsis_mfp_info);
	    %create_base_elig_info_view(tmsis_prmry_dmgrphc_elgblty);
	    %create_base_elig_info_view(tmsis_race_info);
	    %create_base_elig_info_view(tmsis_var_dmgrphc_elgblty);
	    %create_base_elig_info_view(tmsis_wvr_prtcptn_data); 
		%create_base_elig_info_view(tmsis_enrlmt_time_sgmt_data);

		%create_base_tpl_view(tmsis_tpl_mdcd_prsn_mn);
	 	%create_base_tpl_view(tmsis_tpl_mdcd_prsn_hi);
			
		%create_base_prov_view();

		%create_base_prov_info_view(tmsis_prvdr_attr_mn);
		%create_base_prov_info_view(tmsis_prvdr_lctn_cntct);
		%create_base_prov_info_view(tmsis_prvdr_id);
		%create_base_prov_info_view(tmsis_prvdr_lcnsg);
		%create_base_prov_info_view(tmsis_prvdr_txnmy_clsfctn);
		%create_base_prov_info_view(tmsis_prvdr_mdcd_enrlmt);
		%create_base_prov_info_view(tmsis_prvdr_afltd_grp);
		%create_base_prov_info_view(tmsis_prvdr_afltd_pgm);		
		%create_base_prov_info_view(tmsis_prvdr_bed_type);

		%create_base_mc_view(tmsis_mc_mn_data);
		%create_base_mc_view(tmsis_mc_lctn_cntct);
		%create_base_mc_view(tmsis_mc_sarea);
		%create_base_mc_view(tmsis_mc_oprtg_authrty);
		%create_base_mc_view(tmsis_mc_plan_pop_enrld);
		%create_base_mc_view(tmsis_mc_acrdtn_org);
		%create_base_mc_view(tmsis_natl_hc_ent_id_info);
		%create_base_mc_view(tmsis_chpid_shpid_rltnshp_data);


	%tmsis_disconnect;

quit;
run;
