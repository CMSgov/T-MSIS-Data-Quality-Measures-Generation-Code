/*****************************************************************************
* Copyright © Mathematica, Inc. 
* This code cannot be copied, distributed or used without the express written permission
* of Mathematica, Inc. 
*******************************************************************************/

/*****************************************************************
Project:     50139 MACBIS Data Analytics Task 12
Program:     view_defs.sas
Programmer:  Kerianne Hourihan

Measures: None
Inputs:   None
Outputs:  None

Modifications:
*****************************************************************/


%macro create_base_elig_views();

    execute(
	    create or replace view &permview..base_elig_view as
	    select
	         tmsis_run_id
			,submtg_state_cd as submtg_state_orig
	        ,msis_ident_num
	        ,enrlmt_type_cd
			,enrlmt_efctv_dt
			,enrlmt_end_dt
	        ,1 as is_eligible
	    from tmsis.tmsis_enrlmt_time_sgmt_data
	    where tmsis_actv_ind = 1
			and %msis_id_not_missing
    ) by tmsis_passthrough;
 
	execute(
	    create or replace view &permview..base_dtrmnt_view as
	    select
	         tmsis_run_id
	        ,submtg_state_cd as submtg_state_orig
	        ,msis_ident_num
	        ,elgblty_dtrmnt_efctv_dt
	        ,elgblty_dtrmnt_end_dt
			,elgblty_grp_cd
	        ,elgblty_mdcd_basis_cd
	        ,prmry_elgblty_grp_ind
	        ,dual_elgbl_cd
	        ,rstrctd_bnfts_cd
	        ,1 as ever_eligible_det
	    from tmsis.tmsis_elgblty_dtrmnt
	    where tmsis_actv_ind = 1
			and %msis_id_not_missing
			and prmry_elgblty_grp_ind = '1'
    ) by tmsis_passthrough;

%mend;

%macro claim_cat_sql();

          case when (clm_type_cd = '1' and adjstmt_ind = '0' and 
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_A
         ,case when (clm_type_cd = '1' and adjstmt_ind = '0' and xovr_ind = '1') then 1 else 0 end as claim_cat_B
         ,case when (clm_type_cd = '1') then 1 else 0 end as claim_cat_C
         ,case when (clm_type_cd = '2' and adjstmt_ind = '0' ) then 1 else 0 end as claim_cat_D
         ,case when (clm_type_cd = '2' ) then 1 else 0 end as claim_cat_E
         ,case when (clm_type_cd = 'A' and adjstmt_ind = '0' and 
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_F
         ,case when (clm_type_cd = 'A' and adjstmt_ind = '0' and xovr_ind = '1') then 1 else 0 end as claim_cat_G
         ,case when (clm_type_cd = 'A' and adjstmt_ind = '0') then 1 else 0 end as claim_cat_H
         ,case when (clm_type_cd = 'A') then 1 else 0 end as claim_cat_I
         ,case when (clm_type_cd = 'B' and adjstmt_ind = '0' ) then 1 else 0 end as claim_cat_J
         ,case when (clm_type_cd = 'B') then 1 else 0 end as claim_cat_K
         ,case when (clm_type_cd in ('1','3') and adjstmt_ind = '0' and
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_L
         ,case when (clm_type_cd = '1' and adjstmt_ind = '0') then 1 else 0 end as claim_cat_M
         ,case when (clm_type_cd in ('1','3','A','C') and adjstmt_ind = '0' and 
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_N
         ,case when (clm_type_cd = '3') then 1 else 0 end as claim_cat_O
         ,case when (clm_type_cd = '3' and adjstmt_ind = '0' and 
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_P
         ,case when (clm_type_cd = '3' and adjstmt_ind = '0') then 1 else 0 end as claim_cat_Q
         ,case when (clm_type_cd = 'C' and adjstmt_ind = '0' and 
                    (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_R
         ,case when (clm_type_cd = 'C' and adjstmt_ind = '0') then 1 else 0 end as claim_cat_S
         ,case when (clm_type_cd = '3' and adjstmt_ind = '0' and xovr_ind = '1') then 1 else 0 end as claim_cat_T
         ,case when (clm_type_cd = 'C') then 1 else 0 end as claim_cat_U
         ,case when (clm_type_cd = 'C' and adjstmt_ind = '0' and xovr_ind = '1') then 1 else 0 end as claim_cat_V
         ,case when (1=1) then 1 else 0 end as claim_cat_W /** Include all claims **/
         ,case when (clm_type_cd in ('2','B') and adjstmt_ind = '0') then 1 else 0 end as claim_cat_X
         ,case when (clm_type_cd = '2') then 1 else 0 end as claim_cat_Y
         ,case when (clm_type_cd = 'B') then 1 else 0 end as claim_cat_Z
         ,case when (clm_type_cd in ('1','3', 'A','C') and adjstmt_ind in ('0','4') ) then 1 else 0 end as claim_cat_AA
		 ,case when (clm_type_cd in ('1','3') and adjstmt_ind in ('0') ) then 1 else 0 end as claim_cat_AB
	     ,case when (clm_type_cd in ('A','C') and adjstmt_ind in ('0') ) then 1 else 0 end as claim_cat_AC
         ,case when (clm_type_cd in ('1') and xovr_ind = '1' ) then 1 else 0 end as claim_cat_AD
         ,case when (clm_type_cd in ('1','A') and adjstmt_ind in ('0') ) then 1 else 0 end as claim_cat_AE
		 ,case when (clm_type_cd in ('3','C') and adjstmt_ind in ('0') ) then 1 else 0 end as claim_cat_AF
		 ,case when (clm_type_cd in ('2','B') and adjstmt_ind in ('0','4') ) then 1 else 0 end as claim_cat_AG
         ,case when (clm_type_cd in ('1','A') and adjstmt_ind in ('0','4') ) then 1 else 0 end as claim_cat_AH
		 ,case when (clm_type_cd in ('1','3') and xovr_ind = '1' ) then 1 else 0 end as claim_cat_AI
		 ,case when (clm_type_cd in ('1','3','A','C') ) then 1 else 0 end as claim_cat_AJ
		 ,case when (clm_type_cd in ('1','A')) then 1 else 0 end as claim_cat_AK
		 ,case when (clm_type_cd in ('3','C')) then 1 else 0 end as claim_cat_AL
		 ,case when (clm_type_cd in ('1') and adjstmt_ind in ('0','4') ) then 1 else 0 end as claim_cat_AM
		 ,case when (clm_type_cd in ('A') and adjstmt_ind in ('0','4') ) then 1 else 0 end as claim_cat_AN
         ,case when (clm_type_cd in ('1','A') and 
                     (xovr_ind = '0' or  xovr_ind is null)) then 1 else 0 end as claim_cat_AO
         ,case when (clm_type_cd in ('3','C') and 
                     (xovr_ind = '0' or  xovr_ind is null) ) then 1 else 0 end as claim_cat_AP
         ,case when (clm_type_cd in ('1', 'A') and xovr_ind = '1' ) then 1 else 0 end as claim_cat_AQ
         ,case when (clm_type_cd in ('3', 'C') and xovr_ind = '1' ) then 1 else 0 end as claim_cat_AR
         ,case when (clm_type_cd in ('4','D') and adjstmt_ind <> '1' ) then 1 else 0 end as claim_cat_AS
         ,case when (clm_type_cd in ('5','E') ) then 1 else 0 end as claim_cat_AT
         ,case when (clm_type_cd in ('4','D') ) then 1 else 0 end as claim_cat_AU
         ,case when (clm_type_cd in ('4') and adjstmt_ind <> '1' ) then 1 else 0 end as claim_cat_AV
         ,case when (clm_type_cd in ('D') and adjstmt_ind <> '1' ) then 1 else 0 end as claim_cat_AW
%mend;


%macro create_base_cll_view(ftype,fname);

	execute(
	    create or replace view &permview..prep_cll_&ftype._view as
	    select
             submtg_state_cd as submtg_state_orig
            ,tmsis_run_id
            ,tmsis_rptg_prd
            ,msis_ident_num
        	,coalesce(orgnl_clm_num,'0') as orgnl_clm_num
        	,coalesce(adjstmt_clm_num,'0') as adjstmt_clm_num
        	,coalesce(adjdctn_dt,'01JAN1960') as adjdctn_dt
            ,coalesce(orgnl_line_num,'0') as orgnl_line_num
            ,coalesce(adjstmt_line_num,'0') as adjstmt_line_num
            ,coalesce(line_adjstmt_ind,'X') as line_adjstmt_ind

            /* Need these variables that are not recoded for tabulating missingness measures **/
            ,orgnl_line_num as orgnl_line_num_orig
            ,adjstmt_line_num as adjstmt_line_num_orig
            ,line_adjstmt_ind as line_adjstmt_ind_orig
			,adjdctn_dt as line_adjdctn_dt_orig
			,xix_srvc_ctgry_cd 
            ,xxi_srvc_ctgry_cd
			,cll_stus_cd
            ,mdcd_ffs_equiv_amt
            ,mdcd_pd_amt

            %if "&ftype." = "ip" %then %do;
				,cms_64_fed_reimbrsmt_ctgry_cd
                ,srvc_endg_dt
                ,stc_cd
                ,rev_cd
                ,prscrbng_prvdr_npi_num
                ,bnft_type_cd
                ,srvcng_prvdr_num
                ,prvdr_fac_type_cd
                ,rev_chrg_amt
                ,srvcng_prvdr_spclty_cd
                ,srvcng_prvdr_type_cd
                ,srvc_bgnng_dt
			    ,alowd_amt
				,oprtg_prvdr_npi_num
            %end;
            %if "&ftype." = "lt" %then %do;
                ,stc_cd
                ,bnft_type_cd
                ,srvcng_prvdr_num
                ,cms_64_fed_reimbrsmt_ctgry_cd
				,srvc_bgnng_dt
                ,srvc_endg_dt
                ,prvdr_fac_type_cd
                ,rev_chrg_amt
                ,rev_cd
                ,prscrbng_prvdr_npi_num
                ,srvcng_prvdr_spclty_cd
                ,srvcng_prvdr_type_cd
			    ,alowd_amt
            %end;
            %if "&ftype." = "ot" %then %do;
				,cms_64_fed_reimbrsmt_ctgry_cd               
                ,othr_toc_rx_clm_actl_qty
				,srvc_bgnng_dt
                ,srvc_endg_dt
                ,prcdr_cd
                ,prcdr_cd_ind
                ,stc_cd
                ,rev_cd
                ,hcpcs_rate
                ,srvcng_prvdr_num
                ,srvcng_prvdr_spclty_cd
                ,prscrbng_prvdr_npi_num
                ,srvcng_prvdr_txnmy_cd
                ,bill_amt
                ,hcpcs_srvc_cd
                ,hcpcs_txnmy_cd
                ,bnft_type_cd
                ,copay_amt
                ,mdcr_pd_amt
                ,othr_insrnc_amt
                ,prcdr_1_mdfr_cd
                ,prcdr_2_mdfr_cd
                ,srvcng_prvdr_type_cd
                ,tpl_amt
			    ,alowd_amt	
            %end;
            %if "&ftype." = "rx" %then %do;
			    ,cms_64_fed_reimbrsmt_ctgry_cd
                ,suply_days_cnt
                ,othr_toc_rx_clm_actl_qty
                ,ndc_cd
                ,stc_cd
                ,alowd_amt
                ,bill_amt
                ,brnd_gnrc_ind
                ,copay_amt
                ,dspns_fee_amt
                ,mdcr_pd_amt
                ,new_refl_ind
                ,othr_insrnc_amt
                ,rebt_elgbl_ind
                ,tpl_amt
            %end;
			,case when cll_stus_cd in ('26','026','87','087','542','585','654') then 1 else 0 end as denied_line
	    from tmsis.tmsis_cll_rec_&fname.
	    where tmsis_actv_ind = 1
    ) by tmsis_passthrough;
 

%mend;

%macro create_base_clh_view(ftype,fname);

/* Modified to flag denied claims. The denied claims are deleted in next steps.  */
/* We had to create a header file with a denied claim flag because for tabulating*/
/* duplicate claim line (MSRS ALL 5.4 -ALL 5.8) we need to delete claim lines    */
/* that have denied claim hdr record but still want to retain orphan claim lines. */

    execute(
    create or replace view &permview..prep_clh_&ftype._view as
    select
		 tmsis_run_id
        ,submtg_state_cd as submtg_state_orig
        ,tmsis_rptg_prd
        ,coalesce(orgnl_clm_num,'0') as orgnl_clm_num
        ,coalesce(adjstmt_clm_num,'0') as adjstmt_clm_num
        ,coalesce(adjdctn_dt,'01JAN1960') as adjdctn_dt
        ,coalesce(adjstmt_ind,'X') as adjstmt_ind

        /* Need these variables that are not recoded for tabulating missingness measures **/
        ,orgnl_clm_num as orgnl_clm_num_orig
        ,adjstmt_clm_num as adjstmt_clm_num_orig
        ,adjdctn_dt as adjdctn_dt_orig
        ,adjstmt_ind as adjstmt_ind_orig

		,mdcd_pd_dt
        ,clm_type_cd
        ,xovr_ind
        ,clm_stus_ctgry_cd
        ,clm_dnd_ind
        ,othr_insrnc_ind
        ,othr_tpl_clctn_cd
        ,tot_bill_amt
        ,tot_mdcd_pd_amt
        ,plan_id_num
        ,msis_ident_num
        ,blg_prvdr_num
		,blg_prvdr_txnmy_cd
        ,bene_coinsrnc_amt
        ,bene_copmt_amt
        ,bene_ddctbl_amt
        ,cll_cnt
		,wvr_id
     	,srvc_trkng_pymt_amt
		,srvc_trkng_type_cd

	    %if "&ftype." = "ip" %then %do;
	        ,admsn_dt
	        ,admsn_type_cd
	        ,blg_prvdr_type_cd
	        ,dgns_poa_1_cd_ind
	        ,dschrg_dt
	        ,fixd_pymt_ind
	        ,hlth_care_acqrd_cond_cd
	        ,mdcd_dsh_pd_amt
	        ,mdcd_cvrd_ip_days_cnt
	        ,mdcr_pd_amt
	        ,mdcr_reimbrsmt_type_cd
	        ,ncvrd_chrgs_amt
	        ,prcdr_1_cd_dt
	        ,prcdr_2_cd_dt
	        ,prcdr_1_cd_ind
	        ,prcdr_2_cd_ind
	        ,pgm_type_cd
	        ,tot_alowd_amt
	        ,tot_copay_amt
	        ,tot_othr_insrnc_amt
	        ,tot_tpl_amt
	        ,bill_type_cd
	        ,ptnt_stus_cd
	        ,drg_cd
			,drg_cd_ind
	        ,dgns_1_cd
	        ,dgns_2_cd
	        ,dgns_3_cd
	        ,dgns_4_cd
	        ,dgns_5_cd
	        ,dgns_6_cd
	        ,dgns_7_cd
	        ,dgns_8_cd
	        ,dgns_9_cd
	        ,dgns_10_cd
	        ,dgns_11_cd
	        ,dgns_12_cd
	        ,prcdr_1_cd
	        ,prcdr_2_cd
	        ,prcdr_3_cd
	        ,prcdr_4_cd
	        ,prcdr_5_cd
	        ,prcdr_6_cd
	        ,prvdr_lctn_id
	        ,blg_prvdr_npi_num
	        ,hosp_type_cd
		    ,tot_mdcr_coinsrnc_amt
	        ,tot_mdcr_ddctbl_amt
	        ,pymt_lvl_ind
			,admtg_prvdr_npi_num
			,admtg_prvdr_num
			,rfrg_prvdr_npi_num
			,rfrg_prvdr_num

	    %end;
	    %if "&ftype." = "lt" %then %do;
	        ,nrsng_fac_days_cnt
	        ,mdcd_cvrd_ip_days_cnt
	        ,icf_iid_days_cnt
	        ,lve_days_cnt
	        ,ptnt_stus_cd
	        ,srvc_endg_dt
	        ,ltc_rcp_lblty_amt
	        ,dgns_1_cd
	        ,dgns_2_cd
	        ,dgns_3_cd
	        ,dgns_4_cd
	        ,dgns_5_cd
	        ,prvdr_lctn_id
	        ,blg_prvdr_npi_num
	        ,srvc_bgnng_dt
	        ,blg_prvdr_type_cd
	        ,dgns_1_cd_ind
	        ,dgns_2_cd_ind
	        ,dgns_poa_1_cd_ind
	        ,fixd_pymt_ind
	        ,hlth_care_acqrd_cond_cd
	        ,mdcr_pd_amt
	        ,mdcr_reimbrsmt_type_cd
	        ,pgm_type_cd
	        ,tot_alowd_amt
	        ,tot_mdcr_coinsrnc_amt
	        ,tot_mdcr_ddctbl_amt
	        ,tot_othr_insrnc_amt
	        ,tot_tpl_amt
	        ,bill_type_cd
	        ,pymt_lvl_ind
			,admtg_prvdr_npi_num
			,admtg_prvdr_num
			,rfrg_prvdr_npi_num
			,rfrg_prvdr_num
	    %end;
	    %if "&ftype." = "ot" %then %do;
	        ,dgns_1_cd
	        ,dgns_2_cd
	        ,srvc_plc_cd
	        ,prvdr_lctn_id
	        ,blg_prvdr_npi_num
	        ,srvc_bgnng_dt
	        ,blg_prvdr_type_cd
	        ,dgns_1_cd_ind
	        ,dgns_2_cd_ind
	        ,dgns_poa_1_cd_ind
	        ,srvc_endg_dt
	        ,fixd_pymt_ind
	        ,hh_prvdr_ind
	        ,pgm_type_cd
	        ,tot_mdcr_coinsrnc_amt
	        ,tot_mdcr_ddctbl_amt
	        ,tot_othr_insrnc_amt
	        ,tot_tpl_amt
	        ,bill_type_cd
		    ,tot_alowd_amt
	        ,pymt_lvl_ind
			,rfrg_prvdr_npi_num
			,rfrg_prvdr_num
	    %end;
	    %if "&ftype." = "rx" %then %do;
	        ,prscrbd_dt
	        ,rx_fill_dt
	        ,prvdr_lctn_id
	        ,blg_prvdr_npi_num
	        ,dspnsng_pd_prvdr_npi_num
	        ,dspnsng_pd_prvdr_num
	        ,prscrbng_prvdr_num
	        ,tot_mdcr_coinsrnc_amt
	        ,tot_mdcr_ddctbl_amt
	        ,tot_othr_insrnc_amt
	        ,tot_tpl_amt
	        ,cmpnd_drug_ind
	        ,fixd_pymt_ind
	        ,pymt_lvl_ind
	        ,srvcng_prvdr_npi_num
	        ,pgm_type_cd
	        ,tot_alowd_amt
	        ,tot_copay_amt
	    %end;
        ,%claim_cat_sql   
        ,case when clm_stus_ctgry_cd = 'F2' then 1
              when clm_dnd_ind = '0' then 1
              when clm_type_cd = 'Z' then 1
              when clm_stus_cd in ('26','026','87','087','542','585','654') then 1
              else 0 end as denied_header
    from tmsis.tmsis_clh_rec_&fname.
    where tmsis_actv_ind = 1
    	and (orgnl_clm_num is not null or adjstmt_clm_num is not null)
    ) by tmsis_passthrough;

%mend;


%macro create_base_elig_info_view(ftype);

    %if "&ftype." = "tmsis_prmry_dmgrphc_elgblty" %then %do;
        %let saseff_dt = prmry_dmgrphc_ele_efctv_dt;
        %let sasend_dt = prmry_dmgrphc_ele_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_var_dmgrphc_elgblty" %then %do;
        %let saseff_dt = var_dmgrphc_ele_efctv_dt;
        %let sasend_dt = var_dmgrphc_ele_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_elgbl_cntct" %then %do;
        %let saseff_dt = elgbl_adr_efctv_dt;
        %let sasend_dt = elgbl_adr_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_elgblty_dtrmnt" %then %do;
        %let saseff_dt = elgblty_dtrmnt_efctv_dt;
        %let sasend_dt = elgblty_dtrmnt_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_hh_sntrn_prtcptn_info" %then %do;
        %let saseff_dt = hh_sntrn_prtcptn_efctv_dt;
        %let sasend_dt = hh_sntrn_prtcptn_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_hh_sntrn_prvdr" %then %do;
        %let saseff_dt = hh_sntrn_prvdr_efctv_dt;
        %let sasend_dt = hh_sntrn_prvdr_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_hh_chrnc_cond" %then %do;
        %let saseff_dt = hh_chrnc_efctv_dt;
        %let sasend_dt = hh_chrnc_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_lckin_info" %then %do;
        %let saseff_dt = lckin_efctv_dt;
        %let sasend_dt = lckin_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_mfp_info" %then %do;
        %let saseff_dt = mfp_enrlmt_efctv_dt;
        %let sasend_dt = mfp_enrlmt_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_ltss_prtcptn_data" %then %do;
        %let saseff_dt = ltss_elgblty_efctv_dt;
        %let sasend_dt = ltss_elgblty_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_state_plan_prtcptn" %then %do;
        %let saseff_dt = state_plan_optn_efctv_dt;
        %let sasend_dt = state_plan_optn_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_wvr_prtcptn_data" %then %do;
        %let saseff_dt = wvr_enrlmt_efctv_dt;
        %let sasend_dt = wvr_enrlmt_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_mc_prtcptn_data" %then %do;
        %let saseff_dt = mc_plan_enrlmt_efctv_dt;
        %let sasend_dt = mc_plan_enrlmt_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_ethncty_info" %then %do;
        %let saseff_dt = ethncty_dclrtn_efctv_dt;
        %let sasend_dt = ethncty_dclrtn_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_race_info" %then %do;
        %let saseff_dt = race_dclrtn_efctv_dt;
        %let sasend_dt = race_dclrtn_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_dsblty_info" %then %do;
        %let saseff_dt = dsblty_type_efctv_dt;
        %let sasend_dt = dsblty_type_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_sect_1115a_demo_info" %then %do;
        %let saseff_dt = sect_1115a_demo_efctv_dt;
        %let sasend_dt = sect_1115a_demo_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_hcbs_chrnc_cond_non_hh" %then %do;
        %let saseff_dt = ndc_uom_chrnc_non_hh_efctv_dt;
        %let sasend_dt = ndc_uom_chrnc_non_hh_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_enrlmt_time_sgmt_data" %then %do;
    	%let saseff_dt = enrlmt_efctv_dt;
    	%let sasend_dt = enrlmt_end_dt;
    %end;
    
    execute(
        create or replace view &permview..&ftype._view as
        select
             tmsis_run_id
            ,submtg_state_cd as submtg_state_orig
            ,msis_ident_num as msis_id /*Note: alias used to avoid conflict with base_elig column of same name */
            ,&saseff_dt.
            ,&sasend_dt.
        %if "&ftype." = "tmsis_prmry_dmgrphc_elgblty" %then
            ,gndr_cd
            ,death_dt
            ,birth_dt;
        %else %if "&ftype." = "tmsis_var_dmgrphc_elgblty" %then
            ,ssn_num
            ,ssn_vrfctn_ind
            ,ctznshp_ind
            ,ctznshp_vrfctn_ind
            ,imgrtn_vrfctn_ind
            ,imgrtn_stus_cd
    		,hsehld_size_cd
    		,incm_cd
    		,mrtl_stus_cd
    		,vet_ind
            ,chip_cd;
        %else %if "&ftype." = "tmsis_elgbl_cntct" %then
		    ,elgbl_state_cd
            ,elgbl_cnty_cd
            ,elgbl_zip_cd
            ,elgbl_adr_type_cd;
        %else %if "&ftype." = "tmsis_elgblty_dtrmnt" %then
            ,msis_case_num
            ,elgblty_grp_cd
            ,elgblty_mdcd_basis_cd
            ,prmry_elgblty_grp_ind
            ,dual_elgbl_cd
            ,rstrctd_bnfts_cd
            ,mas_cd
            ,ssdi_ind as ssdi_ind
            ,ssi_ind as ssi_ind
            ,ssi_state_splmt_stus_cd
            ,tanf_cash_cd;
    	%else %if "&ftype." = "tmsis_hh_sntrn_prtcptn_info" %then
    		,hh_ent_name
            ,hh_sntrn_name;
    	%else %if "&ftype." = "tmsis_hh_sntrn_prvdr" %then
            ,hh_ent_name
            ,hh_prvdr_num
            ,hh_sntrn_name;
        %else %if "&ftype." = "tmsis_hh_chrnc_cond" %then
            ,hh_chrnc_cd
			,hh_chrnc_othr_explntn_txt;
    	%else %if "&ftype." = "tmsis_lckin_info" %then
    		,lckin_prvdr_type_cd
    		,lckin_prvdr_num;
    	%else %if "&ftype." = "tmsis_mfp_info" %then ;
    		/*no variables except dates*/
        %else %if "&ftype." = "tmsis_state_plan_prtcptn" %then
            ,state_plan_optn_type_cd;
        %else %if "&ftype." = "tmsis_wvr_prtcptn_data" %then
            ,wvr_type_cd
            ,wvr_id;
    	%else %if "&ftype." = "tmsis_ltss_prtcptn_data" %then
    		,ltss_lvl_care_cd
            ,ltss_prvdr_num;
        %else %if "&ftype." = "tmsis_mc_prtcptn_data" %then
            ,enrld_mc_plan_type_cd
            ,mc_plan_id;
        %else %if "&ftype." = "tmsis_ethncty_info" %then
            ,ethncty_cd;
        %else %if "&ftype." = "tmsis_race_info" %then
            ,race_cd
    		,race_othr_txt
            ,crtfd_amrcn_indn_alskn_ntv_ind;
        %else %if "&ftype." = "tmsis_dsblty_info" %then
            ,dsblty_type_cd;
        %else %if "&ftype." = "tmsis_sect_1115a_demo_info" %then
            ,sect_1115a_demo_ind;
    	%else %if "&ftype." = "tmsis_hcbs_chrnc_cond_non_hh" %then
            ,ndc_uom_chrnc_non_hh_cd;
    	%else %if "&ftype." = "tmsis_enrlmt_time_sgmt_data" %then
    		,enrlmt_type_cd;
      from tmsis.&ftype.
      where tmsis_actv_ind = 1
   ) by tmsis_passthrough;

%mend create_base_elig_info_view;

%macro create_base_prov_view();

    execute(
         create or replace view &permview..base_prov_view as
         select
             tmsis_run_id
    		,submtg_state_cd as submtg_state_orig
    		,submtg_state_prvdr_id
            ,prvdr_mdcd_efctv_dt
    		,prvdr_mdcd_end_dt
        from tmsis.tmsis_prvdr_mdcd_enrlmt
        where tmsis_actv_ind = 1
        	and submtg_state_prvdr_id is not null
        	and submtg_state_prvdr_id not rlike '[89]{30}'
            and submtg_state_prvdr_id rlike '[A-Za-z1-9]'
    ) by tmsis_passthrough;

%mend create_base_prov_view;

%macro create_base_prov_info_view(ftype);

    
    %if "&ftype." = "tmsis_prvdr_attr_mn" %then %do;
        %let saseff_dt = prvdr_attr_efctv_dt;
        %let sasend_dt = prvdr_attr_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_lctn_cntct" %then %do;
        %let saseff_dt = prvdr_lctn_cntct_efctv_dt;
        %let sasend_dt = prvdr_lctn_cntct_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_lcnsg" %then %do;
        %let saseff_dt = prvdr_lcns_efctv_dt;
        %let sasend_dt = prvdr_lcns_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_id" %then %do;
        %let saseff_dt = prvdr_id_efctv_dt;
        %let sasend_dt = prvdr_id_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_txnmy_clsfctn" %then %do;
        %let saseff_dt = prvdr_txnmy_clsfctn_efctv_dt;
        %let sasend_dt = prvdr_txnmy_clsfctn_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_mdcd_enrlmt" %then %do;
        %let saseff_dt = prvdr_mdcd_efctv_dt;
        %let sasend_dt = prvdr_mdcd_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_afltd_grp" %then %do;
        %let saseff_dt = prvdr_afltd_grp_efctv_dt;
        %let sasend_dt = prvdr_afltd_grp_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_afltd_pgm" %then %do;
        %let saseff_dt = prvdr_afltd_pgm_efctv_dt;
        %let sasend_dt = prvdr_afltd_pgm_end_dt;
    %end;
    %else %if "&ftype." = "tmsis_prvdr_bed_type" %then %do;
        %let saseff_dt = bed_type_efctv_dt;
        %let sasend_dt = bed_type_end_dt;
    %end;
    
    execute(
         create or replace view &permview..&ftype._view as
         select
             tmsis_run_id
            ,submtg_state_cd as submtg_state_orig
            ,submtg_state_prvdr_id
            ,&saseff_dt.
            ,&sasend_dt.
         %if "&ftype." = "tmsis_prvdr_attr_mn" %then
            ,fac_grp_indvdl_cd
            ,birth_dt
            ,death_dt
            ,prvdr_dba_name
            ,prvdr_1st_name
            ,prvdr_last_name
            ,prvdr_lgl_name
            ,prvdr_org_name;                
         %else %if "&ftype." = "tmsis_prvdr_lctn_cntct" %then
             ,adr_city_name
             ,adr_cnty_cd
             ,email_adr
             ,adr_line_1_txt
             ,adr_state_cd
             ,prvdr_adr_type_cd
             ,adr_zip_cd
             ,prvdr_lctn_id
             ,rec_num;
         %else %if "&ftype." = "tmsis_prvdr_lcnsg" %then
            ,lcns_issg_ent_id_txt
            ,lcns_or_acrdtn_num
            ,lcns_type_cd
            ,prvdr_lctn_id;
         %else %if "&ftype." = "tmsis_prvdr_id" %then
            ,prvdr_id
            ,prvdr_id_issg_ent_id_txt
            ,prvdr_id_type_cd
            ,prvdr_lctn_id;
         %else %if "&ftype." = "tmsis_prvdr_txnmy_clsfctn" %then
            ,prvdr_clsfctn_cd
            ,prvdr_clsfctn_type_cd;
         %else %if "&ftype." = "tmsis_prvdr_mdcd_enrlmt" %then
            ,prvdr_mdcd_enrlmt_stus_cd
            ,state_plan_enrlmt_cd;
         %else %if "&ftype." = "tmsis_prvdr_afltd_grp" %then
            ,submtg_state_afltd_prvdr_id;
    	 %else %if "&ftype." = "tmsis_prvdr_afltd_pgm" %then
    	 	,afltd_pgm_id
    		,afltd_pgm_type_cd;
         %else %if "&ftype." = "tmsis_prvdr_bed_type" %then
            ,bed_type_cd
            ,prvdr_lctn_id
            ,rec_num;
                
            from tmsis.&ftype.
    		where tmsis_actv_ind = 1
    ) by tmsis_passthrough;

%mend create_base_prov_info_view;

%macro create_base_tpl_view(ftype);

      %if "&ftype." = "tmsis_tpl_mdcd_prsn_mn" %then %do;
          %let saseff_dt = elgbl_prsn_mn_efctv_dt;
          %let sasend_dt = elgbl_prsn_mn_end_dt;
      %end;
      %if "&ftype." = "tmsis_tpl_mdcd_prsn_hi" %then %do;
          %let saseff_dt = insrnc_cvrg_efctv_dt;
          %let sasend_dt = insrnc_cvrg_end_dt;
      %end;
      
      execute(
          create or replace view  &permview..&ftype._view as
          select
               tmsis_run_id
              ,submtg_state_cd as submtg_state_orig
              ,msis_ident_num as msis_id /*Note: alias used to avoid conflict with base_elig column of same name */
              ,&saseff_dt.
              ,&sasend_dt.
          %if "&ftype." = "tmsis_tpl_mdcd_prsn_mn" %then %do;
              ,tpl_insrnc_cvrg_ind
              ,tpl_othr_cvrg_ind
          %end;
          %if "&ftype." = "tmsis_tpl_mdcd_prsn_hi" %then %do;
              ,insrnc_carr_id_num
              ,insrnc_plan_id
              ,insrnc_plan_type_cd
              ,cvrg_type_cd
          %end;
      
        from tmsis.&ftype.
        where tmsis_actv_ind = 1
      ) by tmsis_passthrough;

%mend create_base_tpl_view;

%macro create_base_mc_view(ftype);
      
      %if "&ftype." = "tmsis_mc_mn_data" %then %do;
          %let saseff_dt = mc_mn_rec_efctv_dt;
          %let sasend_dt = mc_mn_rec_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_mc_lctn_cntct" %then %do;
          %let saseff_dt = mc_lctn_cntct_efctv_dt;
          %let sasend_dt = mc_lctn_cntct_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_mc_sarea" %then %do;
          %let saseff_dt = mc_sarea_efctv_dt;
          %let sasend_dt = mc_sarea_end_dt;
      %end; 
      %else %if "&ftype." = "tmsis_mc_oprtg_authrty" %then %do;
          %let saseff_dt = mc_op_authrty_efctv_dt;
          %let sasend_dt = mc_op_authrty_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_mc_plan_pop_enrld" %then %do;
          %let saseff_dt = mc_plan_pop_efctv_dt;
          %let sasend_dt = mc_plan_pop_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_mc_acrdtn_org" %then %do;
          %let saseff_dt = acrdtn_achvmt_dt;
          %let sasend_dt = acrdtn_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_natl_hc_ent_id_info" %then %do;
          %let saseff_dt = natl_hlth_care_ent_id_efctv_dt;
          %let sasend_dt = natl_hlth_care_ent_id_end_dt;
      %end;
      %else %if "&ftype." = "tmsis_chpid_shpid_rltnshp_data" %then %do;
	  	  %let saseff_dt = chpid_shpid_rltnshp_efctv_dt;
		  %let sasend_dt = chpid_shpid_rltnshp_end_dt;
	  %end;

      execute(
      	create or replace view &permview..&ftype._view as
      	select
      	     tmsis_run_id
      	    ,submtg_state_cd as submtg_state_orig
      	    ,state_plan_id_num
      	    ,&saseff_dt.
      	    ,&sasend_dt.
      	    %if "&ftype." = "tmsis_mc_mn_data" %then
                  ,mc_plan_type_cd
                  ,mc_pgm_cd
                  ,reimbrsmt_arngmt_cd;    
              %else %if "&ftype." = "tmsis_mc_lctn_cntct" %then
                  ,mc_adr_type_cd
                  ,mc_lctn_id
                  ,rec_num;         
              %else %if "&ftype." = "tmsis_mc_sarea" %then
                  ,mc_sarea_name;         
      	    %else %if "&ftype." = "tmsis_mc_oprtg_authrty" %then
      	        ,oprtg_authrty_cd
                  ,wvr_id;
              %else %if "&ftype." = "tmsis_mc_plan_pop_enrld" %then
                  ,mc_plan_pop_cnt;                   
              %else %if "&ftype." = "tmsis_mc_acrdtn_org" %then
                  ,acrdtn_org_cd;       
      	    %else %if "&ftype." = "tmsis_natl_hc_ent_id_info" %then
      	        ,natl_hlth_care_ent_id
      	        ,natl_hlth_care_ent_id_type_cd;
      	from tmsis.&ftype.
      	where tmsis_actv_ind = 1
      ) by tmsis_passthrough;

%mend create_base_mc_view;
