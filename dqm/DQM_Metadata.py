
# -------------------------------------------------------------------------------------------------
#
#
#   Data-Definition Metadata
#
#
# -------------------------------------------------------------------------------------------------
from numpy import float64, int64
from decimal import Decimal


class DQM_Metadata:

    # -------------------------------------------------------------------------------------------------
    #   Eligibility Tables
    # -------------------------------------------------------------------------------------------------
    class elig_tables():

        # -------------------------------------------------------------------------------------------------
        #   Eligibility - Current
        # -------------------------------------------------------------------------------------------------
        class current():

            tblList = ('tmsis_prmry_dmgrphc_elgblty', 'tmsis_var_dmgrphc_elgblty', 'tmsis_elgbl_cntct', 'tmsis_elgblty_dtrmnt',
                       'tmsis_hh_sntrn_prtcptn_info', 'tmsis_hh_chrnc_cond', 'tmsis_lckin_info', 'tmsis_mfp_info', 'tmsis_ltss_prtcptn_data',
                       'tmsis_state_plan_prtcptn', 'tmsis_wvr_prtcptn_data', 'tmsis_mc_prtcptn_data', 'tmsis_ethncty_info', 'tmsis_race_info',
                       'tmsis_dsblty_info', 'tmsis_sect_1115a_demo_info', 'tmsis_hcbs_chrnc_cond_non_hh', 'tmsis_enrlmt_time_sgmt_data')

            dtPrefix = ('prmry_dmgrphc_ele', 'var_dmgrphc_ele', 'elgbl_adr', 'elgblty_dtrmnt',
                        'hh_sntrn_prtcptn', 'hh_chrnc', 'lckin', 'mfp_enrlmt', 'ltss_elgblty',
                        'state_plan_optn', 'wvr_enrlmt', 'mc_plan_enrlmt', 'ethncty_dclrtn', 'race_dclrtn',
                        'dsblty_type', 'sect_1115a_demo', 'ndc_uom_chrnc_non_hh', 'enrlmt')

        # -------------------------------------------------------------------------------------------------
        #   Eligibility - Prior
        # -------------------------------------------------------------------------------------------------
        class prior():

            tblList = ('tmsis_prmry_dmgrphc_elgblty', 'tmsis_var_dmgrphc_elgblty', 'tmsis_elgbl_cntct', 'tmsis_mc_prtcptn_data', 'tmsis_ethncty_info', 'tmsis_race_info')

            dtPrefix = ('prmry_dmgrphc_ele', 'var_dmgrphc_ele', 'elgbl_adr', 'mc_plan_enrlmt', 'ethncty_dclrtn', 'race_dclrtn')

    # -------------------------------------------------------------------------------------------------
    #   Provider Tables
    # -------------------------------------------------------------------------------------------------
    class prov_tables():

        tblList = ('tmsis_prvdr_attr_mn', 'tmsis_prvdr_lctn_cntct', 'tmsis_prvdr_id', 'tmsis_prvdr_txnmy_clsfctn', 'tmsis_prvdr_mdcd_enrlmt', 'tmsis_prvdr_afltd_pgm')

        dtPrefix = ('prvdr_attr', 'prvdr_lctn_cntct', 'prvdr_id', 'prvdr_txnmy_clsfctn', 'prvdr_mdcd', 'prvdr_afltd_pgm')

        # -------------------------------------------------------------------------------------------------
        #   Provider - Ever
        # -------------------------------------------------------------------------------------------------
        class ever():

            tblList = ('tmsis_prvdr_attr_mn', 'tmsis_prvdr_id', 'tmsis_prvdr_mdcd_enrlmt')

            evrvarList = ('ever_provider', 'ever_provider_id', 'ever_enrolled_provider')

        class prvdr_pct_sql():

            tblList = ('prvdr_prep', 'all_clms_prvdrs', 'uniq_clms_prvdrs_file', 'uniq_clms_prvdrs', 'prv_clm',
                       'clm_prv_tab', 'clm_prv_ip', 'clm_prv_tab_ip', 'clm_prv_lt', 'clm_prv_tab_lt',
                       'clm_prv_ot', 'clm_prv_tab_ot', 'clm_prv_rx', 'clm_prv_tab_rx', 'prv_addtyp_prep',
                       'prv_addtyp_rollup', 'prv_addtyp', 'prv_idtyp_prep', 'prv_idtyp', 'prv_mdcd_prep', 'prv_mdcd',
                       'prv_id_npi', 'prvdr_npi_txnmy', 'prvdr_npi_txnmy2',
                       'prv2_10_denom', 'prv2_10_numer', 'prv2_10_msr')

        class prvdr_freq_sql():

            tblList = ('prvdr_txnmy', 'prvdr_freq_t', 'prvdr_freq_t2')

    # -------------------------------------------------------------------------------------------------
    #   TPL Tables
    # -------------------------------------------------------------------------------------------------
    class tpl_tables():

        tblList = ('tmsis_tpl_mdcd_prsn_mn', 'tmsis_tpl_mdcd_prsn_hi')

        dtPrefix = ('elgbl_prsn_mn', 'insrnc_cvrg')

        class tpl_prsn_hi_sql():

            tpl_cvrg_typ = ('01','02','03','04','05','06','07','08','09','10',
                            '11','12','13','14','15','16','17','18','19','20',
                            '21','22','23','98')

            tpl_insrnc_typ = ('01','02','03','04','05','06','07','08','09',
                              '10','11','12','13','14','15','16')

    # -------------------------------------------------------------------------------------------------
    #   MCPlan Tables
    # -------------------------------------------------------------------------------------------------
    class mcplan_tables():

        #tblList = ('tmsis_mc_mn_data', 'tmsis_mc_oprtg_authrty', 'tmsis_natl_hc_ent_id_info')

        #dtPrefix = ('mc_mn_rec', 'mc_op_authrty', 'natl_hlth_care_ent_id')

        tblList = ('tmsis_mc_mn_data', 'tmsis_mc_oprtg_authrty')

        dtPrefix = ('mc_mn_rec', 'mc_op_authrty')

        class base_mc_view_columns():
            select = {
                'tmsis_mc_mn_data'         : ',mc_plan_type_cd, mc_pgm_cd, reimbrsmt_arngmt_cd',
                'tmsis_mc_lctn_cntct'      : ',mc_adr_type_cd, mc_lctn_id, rec_num',
                'tmsis_mc_sarea'           : ',mc_sarea_name',
                'tmsis_mc_oprtg_authrty'   : ',oprtg_authrty_cd, wvr_id',
                'tmsis_mc_plan_pop_enrld'  : ',mc_plan_pop_cnt',
                'tmsis_mc_acrdtn_org'      : ',acrdtn_org_cd',
                'tmsis_natl_hc_ent_id_info': ',natl_hlth_care_ent_id, natl_hlth_care_ent_id_type_cd',
                'tmsis_chpid_shpid_rltnshp_data': ''
            }

    # -------------------------------------------------------------------------------------------------
    #   FTX Tables
    # -------------------------------------------------------------------------------------------------
    class ftx_tables():

        tblList = ('tmsis_indvdl_cptatn_pmpm','tmsis_indvdl_hi_prm_pymt','tmsis_grp_insrnc_prm_pymt','tmsis_cst_shrng_ofst','tmsis_val_bsd_pymt','tmsis_sdp_seprt_pymt_term','tmsis_cst_stlmt_pymt','tmsis_fqhc_wrp_pymt','tmsis_misc_pymt')

        class ftx_view_columns():

            select = {
                'tmsis_indvdl_cptatn_pmpm':
                    """ ,pyr_mcr_plan_type
                        ,pyee_mcr_plan_type
                        ,pyee_tax_id_type
                        ,msis_ident_num
                        ,cptatn_prd_strt_dt
                        ,cptatn_prd_end_dt
                        ,sdp_ind
                        ,subcptatn_ind """,

                'tmsis_indvdl_hi_prm_pymt':
                    """ ,msis_ident_num
                        ,prm_prd_strt_dt
                        ,prm_prd_end_dt
                        ,insrnc_plan_id """,
                            
                'tmsis_grp_insrnc_prm_pymt':
                    """ ,coalesce(msis_ident_num, 'X') as msis_ident_num
                        ,coalesce(ssn,'X') as ssn
                        ,msis_ident_num as orig_msis_ident_num
                        ,ssn as orig_ssn
                        ,prm_prd_strt_dt
                        ,prm_prd_end_dt
                        ,insrnc_plan_id
                        ,plcy_ownr_cd""",

                'tmsis_cst_shrng_ofst':
                    """ ,pyee_mcr_plan_type
                        ,msis_ident_num
                        ,cvrg_prd_strt_dt
                        ,cvrg_prd_end_dt
                        ,insrnc_plan_id
                        ,ofst_trans_type""",

                'tmsis_val_bsd_pymt':	
                    """ ,pyee_mcr_plan_type
                        ,msis_ident_num
                        ,prfmnc_prd_strt_dt
                        ,prfmnc_prd_end_dt
                        ,sdp_ind
                        ,vb_pymt_model_type""",

                'tmsis_sdp_seprt_pymt_term':
                    """ ,pyee_mcr_plan_type
                        ,pymt_prd_strt_dt
                        ,pymt_prd_end_dt
                        ,pymt_prd_type""",   

                'tmsis_cst_stlmt_pymt':	
                    """,pyee_mcr_plan_type
                       ,cst_stlmt_prd_strt_dt		
                       ,cst_stlmt_prd_end_dt""",

                'tmsis_fqhc_wrp_pymt':	
                    """,pyee_mcr_plan_type
                       ,wrp_prd_strt_dt		
                       ,wrp_prd_end_dt""",	

                'tmsis_misc_pymt':
                    """ ,pyee_mcr_plan_type
                        ,msis_ident_num
                        ,pymt_prd_strt_dt
                        ,pymt_prd_end_dt
                        ,pymt_prd_type
                        ,trans_type_cd
                        ,sdp_ind"""
                }
            
            #FTX claim categories. Used for all tables except cost sharing offset table
            #For cost sharing offset table use ftx_cst_shrng_claim_cat
            # ex. ftx_tables.ftx_views_column.ftx_claim_cat['D']          

            ftx_claim_cat = {

                 'D': "(adjstmt_ind = '0' and mbescbes_form_grp in ('1','2'))"
                ,'E': "(mbescbes_form_grp in ('1','2'))"
                ,'J': "(adjstmt_ind = '0' and mbescbes_form_grp in ('3'))"
                ,'K': "(mbescbes_form_grp in ('3'))"
                ,'X': "(adjstmt_ind = '0' and mbescbes_form_grp is not null)"
                ,'Z': "(mbescbes_form_grp in ('3'))"
                ,'AT': "(trans_type_cd in ('76','77'))"
                #,'AG': "(adjstmt_ind in ('0','4') and mbescbes_form_grp is not null)"
                #,'BA': "(adjstmt_ind in ('0','4') and mbescbes_form_grp in ('1','2'))"
                #,'BB': "(adjstmt_ind in ('0','4') and mbescbes_form_grp in ('3'))"
                #,'BF': "(subcptatn_ind in ('2') and adjstmt_ind in ('0') and mbescbes_form_grp in ('1','2'))"
                #,'BG': "(subcptatn_ind in ('2') and adjstmt_ind in ('0') and mbescbes_form_grp in ('3'))"
                
                }
            
            #Create claim cat dictionary for cost sharing offset table
            # ex. ftx_tables.ftx_views_column.ftx_cst_shrng_claim_cat['D']          

            ftx_cst_shrng_claim_cat={}

            for key in ftx_claim_cat.keys():
                if key in ('D', 'E', 'J', 'K', 'X', 'Z', 'AG', 'BA','BB'):
                    ftx_cst_shrng_claim_cat[key] = "(" + ftx_claim_cat[key] + " and (ofst_trans_type != '3') )"
                else:
                    ftx_cst_shrng_claim_cat[key] = ftx_claim_cat[key] 
            
            # Nested dictionary for claim category
            # For cost sharing offset table use ftx_cst_shrng_claim_cat
            # Else use ftx_claim_cat
            # ex. ftx_tables.ftx_views_column.ftx_claim_cat_fnl['ftx_cst_shrng']['D'] or 
            #     ftx_tables.ftx_views_column.ftx_claim_cat_fnl['ftx_othr']['D']          

            ftx_claim_cat_fnl ={
                               'ftx_cst_shrng': 
                                    ftx_cst_shrng_claim_cat,
                                'ftx_othr':
                                      ftx_claim_cat
                               }
                               

    class create_base_elig_info_view():
        select = {
            'tmsis_prmry_dmgrphc_elgblty':
                """,sex_cd
                    ,death_dt
                    ,birth_dt""",
            'tmsis_var_dmgrphc_elgblty':
                """,ssn_num
                    ,ssn_vrfctn_ind
                    ,ctznshp_ind
                    ,ctznshp_vrfctn_ind
                    ,prefrd_lang_cd
                    ,imgrtn_vrfctn_ind
                    ,imgrtn_stus_cd
                    ,hsehld_size_cd
                    ,incm_cd
                    ,mrtl_stus_cd
                    ,vet_ind
                    ,chip_cd
                    ,mdcr_bene_id
                    ,prgnt_ind""",
            'tmsis_elgbl_cntct':
                """,elgbl_state_cd
                    ,elgbl_cnty_cd
                    ,elgbl_zip_cd
                    ,elgbl_adr_type_cd""",
            'tmsis_elgblty_dtrmnt':
                """,msis_case_num
                    ,elgblty_trmntn_rsn
                    ,elgblty_grp_cd
                    ,prmry_elgblty_grp_ind
                    ,dual_elgbl_cd
                    ,rstrctd_bnfts_cd
                    ,ssdi_ind as ssdi_ind
                    ,ssi_ind as ssi_ind
                    ,ssi_state_splmt_stus_cd
                    ,tanf_cash_cd""",
            'tmsis_hh_sntrn_prtcptn_info':
                """,hh_ent_name
                    ,hh_sntrn_name""",
            'tmsis_hh_sntrn_prvdr':
                """,hh_ent_name
                    ,hh_prvdr_num
                    ,hh_sntrn_name""",
            'tmsis_hh_chrnc_cond':
                """,hh_chrnc_cd
                    ,hh_chrnc_othr_explntn_txt""",
            'tmsis_lckin_info':
                """,lckin_prvdr_type_cd
                    ,lckin_prvdr_num""",
            'tmsis_mfp_info':
                """""",
            'tmsis_state_plan_prtcptn':
                """,state_plan_optn_type_cd""",
            'tmsis_wvr_prtcptn_data':
                """,wvr_type_cd
                ,wvr_id""",
            'tmsis_ltss_prtcptn_data':
                """,ltss_lvl_care_cd
                    ,ltss_prvdr_num""",
            'tmsis_mc_prtcptn_data':
                """,mc_plan_type_cd
                    ,mc_plan_id""",
            'tmsis_ethncty_info':
                """,ethncty_cd""",
            'tmsis_race_info':
                """,race_cd
                    ,race_othr_txt
                    ,crtfd_amrcn_indn_alskn_ntv_ind""",
            'tmsis_dsblty_info':
                """,dsblty_type_cd""",
            'tmsis_sect_1115a_demo_info':
                """,sect_1115a_demo_ind""",
            'tmsis_hcbs_chrnc_cond_non_hh':
                """,ndc_uom_chrnc_non_hh_cd""",
            'tmsis_enrlmt_time_sgmt_data':
                """,enrlmt_type_cd"""
        }
    
    class create_base_prov_info_view():
        select = {
            'tmsis_prvdr_attr_mn':
                """,fac_grp_indvdl_cd
                    ,birth_dt
                    ,death_dt
                    ,prvdr_dba_name
                    ,prvdr_1st_name
                    ,prvdr_last_name
                    ,prvdr_lgl_name
                    ,prvdr_org_name""",
            'tmsis_prvdr_lctn_cntct':
                """,adr_city_name
                    ,adr_cnty_cd
                    ,email_adr
                    ,adr_line_1_txt
                    ,adr_state_cd
                    ,prvdr_adr_type_cd
                    ,adr_zip_cd
                    ,prvdr_lctn_id
                    ,rec_num""",
            'tmsis_prvdr_lcnsg':
                """,lcns_issg_ent_id_txt
                    ,lcns_or_acrdtn_num
                    ,lcns_type_cd
                    ,prvdr_lctn_id""",
            'tmsis_prvdr_id':
                """,prvdr_id
                    ,prvdr_id_issg_ent_id_txt
                    ,prvdr_id_type_cd
                    ,prvdr_lctn_id""",
            'tmsis_prvdr_txnmy_clsfctn':
                """,prvdr_clsfctn_cd
                    ,prvdr_clsfctn_type_cd""",
            'tmsis_prvdr_mdcd_enrlmt':
                """,prvdr_mdcd_enrlmt_stus_cd
                    ,state_plan_enrlmt_cd""",
            'tmsis_prvdr_afltd_grp':
                """,submtg_state_afltd_prvdr_id""",
            'tmsis_prvdr_afltd_pgm':
                """,afltd_pgm_id
                    ,afltd_pgm_type_cd""",
            'tmsis_prvdr_bed_type':
                """,bed_type_cd
                    ,prvdr_lctn_id
                    ,rec_num"""
        }

        class create_base_prov_info_view():
            class b():

                select = {
                    'ip':
                        """
                            ,b.fed_reimbrsmt_ctgry_cd
                            ,b.srvc_endg_dt
                            ,b.stc_cd
                            ,b.rev_cd
                            ,b.prscrbng_prvdr_npi_num                          
                            ,b.srvcng_prvdr_num
                            ,b.prvdr_fac_type_cd
                            ,b.rev_chrg_amt
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.srvc_bgnng_dt
                            ,b.alowd_amt
                            ,oprtg_prvdr_npi_num""",
                    'lt':
                        """,b.stc_cd
                            ,b.srvcng_prvdr_num
                            ,b.fed_reimbrsmt_ctgry_cd
                            ,b.srvc_bgnng_dt
                            ,b.srvc_endg_dt
                            ,b.prvdr_fac_type_cd
                            ,b.rev_chrg_amt
                            ,b.rev_cd
                            ,b.prscrbng_prvdr_npi_num
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.alowd_amt""",
                    'ot':
                        """,b.fed_reimbrsmt_ctgry_cd
                            ,b.svc_qty_actl
                            ,b.srvc_bgnng_dt
                            ,b.srvc_endg_dt
                            ,b.prcdr_cd
                            ,b.prcdr_cd_ind
                            ,b.stc_cd
                            ,b.rev_cd
                            ,b.srvcng_prvdr_num
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.prscrbng_prvdr_npi_num
                            ,b.srvcng_prvdr_txnmy_cd
                            ,b.bill_amt
                            ,b.hcpcs_srvc_cd
                            ,b.hcpcs_txnmy_cd
                            ,b.bene_copmt_pd_amt
                            ,b.mdcr_pd_amt
                            ,b.othr_insrnc_amt
                            ,b.prcdr_1_mdfr_cd
                            ,b.prcdr_2_mdfr_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.tpl_amt
                            ,b.tooth_num
                            ,b.alowd_amt""",
                    'rx':
                        """,b.fed_reimbrsmt_ctgry_cd 
                            ,b.suply_days_cnt
                            ,b.rx_qty_actl
                            ,b.ndc_cd
                            ,b.stc_cd
                            ,b.alowd_amt
                            ,b.bill_amt
                            ,b.brnd_gnrc_ind
                            ,b.bene_copmt_pd_amt
                            ,b.dspns_fee_sbmtd
                            ,b.mdcr_pd_amt
                            ,b.new_refl_ind
                            ,b.othr_insrnc_amt
                            ,b.rebt_elgbl_ind
                            ,b.tpl_amt"""
                }

            class a():

                select = {
                    'ip':
                        """,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.hosp_type_cd
                            ,a.admsn_dt""",
                    'lt':
                        """,a.nrsng_fac_days_cnt
                            ,a.mdcd_cvrd_ip_days_cnt
                            ,a.icf_iid_days_cnt
                            ,a.lve_days_cnt""",
                    'ot':
                        """,a.srvc_plc_cd
                            ,a.plan_id_num
                            ,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.othr_insrnc_ind
                            ,a.othr_tpl_clctn_cd
                            ,a.pgm_type_cd
                            ,a.bill_type_cd """,
                    'rx':
                        """,a.prscrbng_prvdr_num
                            ,a.dspnsng_pd_prvdr_num
                            ,a.rx_fill_dt"""
                }

    class create_base_cll_view():

        select = {
            'ip':
                """,fed_reimbrsmt_ctgry_cd
                    ,srvc_endg_dt
                    ,stc_cd
                    ,rev_cd
                    ,prscrbng_prvdr_npi_num
                    ,srvcng_prvdr_num
                    ,prvdr_fac_type_cd
                    ,rev_chrg_amt
                    ,srvcng_prvdr_spclty_cd
                    ,srvcng_prvdr_type_cd
                    ,srvc_bgnng_dt
                    ,alowd_amt
                    ,oprtg_prvdr_npi_num""",
            'lt':
                """,stc_cd
                    ,srvcng_prvdr_num
                    ,fed_reimbrsmt_ctgry_cd
                    ,srvc_bgnng_dt
                    ,srvc_endg_dt
                    ,prvdr_fac_type_cd
                    ,rev_chrg_amt
                    ,rev_cd
                    ,prscrbng_prvdr_npi_num
                    ,srvcng_prvdr_spclty_cd
                    ,srvcng_prvdr_type_cd
                    ,alowd_amt""",
            'ot':
                """,fed_reimbrsmt_ctgry_cd
                    ,svc_qty_actl
                    ,srvc_bgnng_dt
                    ,srvc_endg_dt
                    ,prcdr_cd
                    ,prcdr_cd_ind
                    ,stc_cd
                    ,rev_cd
                    ,srvcng_prvdr_num
                    ,srvcng_prvdr_spclty_cd
                    ,prscrbng_prvdr_npi_num
                    ,srvcng_prvdr_txnmy_cd
                    ,bill_amt
                    ,hcpcs_srvc_cd
                    ,hcpcs_txnmy_cd
                    ,bene_copmt_pd_amt
                    ,mdcr_pd_amt
                    ,othr_insrnc_amt
                    ,prcdr_1_mdfr_cd
                    ,prcdr_2_mdfr_cd
                    ,srvcng_prvdr_type_cd
                    ,tpl_amt
                    ,tooth_num
                    ,alowd_amt""",
            'rx':
                """,fed_reimbrsmt_ctgry_cd
                    ,suply_days_cnt
                    ,rx_qty_actl
                    ,ndc_cd
                    ,stc_cd
                    ,alowd_amt
                    ,bill_amt
                    ,brnd_gnrc_ind
                    ,bene_copmt_pd_amt
                    ,dspns_fee_sbmtd
                    ,mdcr_pd_amt
                    ,new_refl_ind
                    ,othr_insrnc_amt
                    ,rebt_elgbl_ind
                    ,tpl_amt"""
        }
    class create_base_clh_view():

        select = {
            'ip':
                """,admsn_dt
                    ,admsn_type_cd
                    ,blg_prvdr_type_cd
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
                    ,rfrg_prvdr_num""",
            'lt':
                """,nrsng_fac_days_cnt
                    ,mdcd_cvrd_ip_days_cnt
                    ,icf_iid_days_cnt
                    ,lve_days_cnt
                    ,ptnt_stus_cd
                    ,srvc_endg_dt
                    ,ltc_rcp_lblty_amt
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,srvc_bgnng_dt
                    ,blg_prvdr_type_cd
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
                    ,rfrg_prvdr_num""",
            'ot':
                """
                    ,srvc_plc_cd
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,srvc_bgnng_dt
                    ,blg_prvdr_type_cd
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
                   """,
            'rx':
                """,prscrbd_dt
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
                    ,tot_copay_amt"""
        }

        # since we realize claim_cat when measures are run
        # the base view replaces null adjstmt_ind values with X
        # therefore we modified the logic to test for X to reflect how
        # a null value would have been handled
        # (adjstmt_ind not in ('1', 'X'))
        claim_cat = {
             'A': "(clm_type_cd = '1' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'B': "(clm_type_cd = '1' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'C': "(clm_type_cd = '1')"
            #,'D': "(clm_type_cd = '2' and adjstmt_ind = '0')"
            #,'E': "(clm_type_cd = '2')"
            ,'F': "(clm_type_cd = 'A' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'G': "(clm_type_cd = 'A' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'H': "(clm_type_cd = 'A' and adjstmt_ind = '0')"
            ,'I': "(clm_type_cd = 'A')"
            #,'J': "(clm_type_cd = 'B' and adjstmt_ind = '0')"
            #,'K': "(clm_type_cd = 'B')"
            ,'L': "(clm_type_cd in ('1','3') and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'M': "(clm_type_cd = '1' and adjstmt_ind = '0')"
            ,'N': "(clm_type_cd in ('1','3','A','C') and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'O': "(clm_type_cd = '3')"
            ,'P': "(clm_type_cd = '3' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'Q': "(clm_type_cd = '3' and adjstmt_ind = '0')"
            ,'R': "(clm_type_cd = 'C' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'S': "(clm_type_cd = 'C' and adjstmt_ind = '0')"
            ,'T': "(clm_type_cd = '3' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'U': "(clm_type_cd = 'C')"
            ,'V': "(clm_type_cd = 'C' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'W': "(1=1)"
            #,'X': "(clm_type_cd in ('2','B') and adjstmt_ind = '0')"
            #,'Y': "(clm_type_cd = '2')"
            #,'Z': "(clm_type_cd = 'B')"
            ,'AA': "(clm_type_cd in ('1','3', 'A','C') and adjstmt_ind in ('0','4') )"
            ,'AB': "(clm_type_cd in ('1','3') and adjstmt_ind in ('0') )"
            ,'AC': "(clm_type_cd in ('A','C') and adjstmt_ind in ('0') )"
            ,'AD': "(clm_type_cd in ('1') and xovr_ind = '1' )"
            ,'AE': "(clm_type_cd in ('1','A') and adjstmt_ind in ('0') )"
            ,'AF': "(clm_type_cd in ('3','C') and adjstmt_ind in ('0') )"
            #,'AG': "(clm_type_cd in ('2','B') and adjstmt_ind in ('0','4') )"
            ,'AH': "(clm_type_cd in ('1','A') and adjstmt_ind in ('0','4') )"
            ,'AI': "(clm_type_cd in ('1','3') and xovr_ind = '1' )"
            ,'AJ': "(clm_type_cd in ('1','3','A','C') )"
            ,'AK': "(clm_type_cd in ('1','A'))"
            ,'AL': "(clm_type_cd in ('3','C'))"
            ,'AM': "(clm_type_cd in ('1') and adjstmt_ind in ('0','4') )"
            ,'AN': "(clm_type_cd in ('A') and adjstmt_ind in ('0','4') )"
            ,'AO': "(clm_type_cd in ('1','A') and (xovr_ind = '0' or xovr_ind is null))"
            ,'AP': "(clm_type_cd in ('3','C') and (xovr_ind = '0' or xovr_ind is null))"
            ,'AQ': "(clm_type_cd in ('1','A') and xovr_ind = '1' )"
            ,'AR': "(clm_type_cd in ('3','C') and xovr_ind = '1' )"
            ,'AS': "(clm_type_cd in ('4','D') and (adjstmt_ind not in ('1', 'X')) )"
            #,'AT': "(clm_type_cd in ('5','E') )"
            ,'AU': "(clm_type_cd in ('4','D') )"
            ,'AV': "(clm_type_cd in ('4') and (adjstmt_ind not in ('1', 'X')) )"
            ,'AW': "(clm_type_cd in ('D') and (adjstmt_ind not in ('1', 'X')) )"
            ,'AX': "(clm_type_cd in ('1','3') and (adjstmt_ind in ('0','4')) )" # Medicaid FFS and Encounter: Original and Replacement, Paid Claims
            ,'AY': "(clm_type_cd in ('A','C') and (adjstmt_ind in ('0','4')) )" #   S-CHIP FFS and Encounter: Original and Replacement, Paid Claims
            ,'AZ': "(clm_type_cd in ('3','C') and (adjstmt_ind in ('0','4')) )" # Medicaid and S-CHIP Encounters: Original and Replacement, Paid Claims
            #,'BA': "(clm_type_cd in ('2') and (adjstmt_ind in ('0','4')) )"     # Medicaid Capitation Payment: Original and Replacement, Paid Claims
            #,'BB': "(clm_type_cd in ('B') and (adjstmt_ind in ('0','4')) )"     #   S-CHIP Capitation Payment: Original and Replacement, Paid Claims
            ,'BC': "(clm_type_cd in ('1','A') and (adjstmt_ind not in ('1')) and xovr_ind = '1' )"
            ,'BD': "(clm_type_cd in ('3','C') and (adjstmt_ind not in ('1')) and xovr_ind = '1' )"
            # BE claim category not used in code as of v3.9, so not coded
            #,'BF': "(clm_type_cd in ('6') and adjstmt_ind in ('0') )"
            #,'BG': "(clm_type_cd in ('F') and adjstmt_ind in ('0') )"
        }

    class create_claims_tables():

        class b():
            select = {
                'ip':
                    """,b.fed_reimbrsmt_ctgry_cd
                        ,b.srvc_endg_dt
                        ,b.stc_cd
                        ,b.rev_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.srvcng_prvdr_num
                        ,b.prvdr_fac_type_cd
                        ,b.rev_chrg_amt
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.srvc_bgnng_dt
                        ,b.alowd_amt
                        ,oprtg_prvdr_npi_num""",
                'lt':
                    """,b.stc_cd
                        ,b.srvcng_prvdr_num
                        ,b.fed_reimbrsmt_ctgry_cd
                        ,b.srvc_bgnng_dt
                        ,b.srvc_endg_dt
                        ,b.prvdr_fac_type_cd
                        ,b.rev_chrg_amt
                        ,b.rev_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.alowd_amt""",
                'ot':
                    """,b.fed_reimbrsmt_ctgry_cd
                        ,b.svc_qty_actl
                        ,b.srvc_bgnng_dt
                        ,b.srvc_endg_dt
                        ,b.prcdr_cd
                        ,b.prcdr_cd_ind
                        ,b.stc_cd
                        ,b.rev_cd
                        ,b.srvcng_prvdr_num
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.srvcng_prvdr_txnmy_cd
                        ,b.bill_amt
                        ,b.hcpcs_srvc_cd
                        ,b.hcpcs_txnmy_cd
                        ,b.bene_copmt_pd_amt
                        ,b.mdcr_pd_amt
                        ,b.othr_insrnc_amt
                        ,b.prcdr_1_mdfr_cd
                        ,b.prcdr_2_mdfr_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.tpl_amt
                        ,b.tooth_num
                        ,b.alowd_amt""",
                'rx':
                    """,b.fed_reimbrsmt_ctgry_cd
                        ,b.suply_days_cnt
                        ,b.rx_qty_actl
                        ,b.ndc_cd
                        ,b.stc_cd
                        ,b.alowd_amt
                        ,b.bill_amt
                        ,b.brnd_gnrc_ind
                        ,b.bene_copmt_pd_amt
                        ,b.dspns_fee_sbmtd
                        ,b.mdcr_pd_amt
                        ,b.new_refl_ind
                        ,b.othr_insrnc_amt
                        ,b.rebt_elgbl_ind
                        ,b.tpl_amt"""
            }
        class a():

                select = {
                    'ip':
                        """,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.hosp_type_cd
                            ,a.admsn_dt""",
                    'lt':
                        """,a.nrsng_fac_days_cnt
                            ,a.mdcd_cvrd_ip_days_cnt
                            ,a.icf_iid_days_cnt
                            ,a.lve_days_cnt""",
                    'ot':
                        """,a.srvc_plc_cd
                            ,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.othr_insrnc_ind
                            ,a.othr_tpl_clctn_cd
                            ,a.pgm_type_cd
                            ,a.bill_type_cd
                            ,a.pymt_lvl_ind""",
                    'rx':
                        """,a.prscrbng_prvdr_num
                            ,a.dspnsng_pd_prvdr_num
                            ,a.rx_fill_dt"""
                }

    # -------------------------------------------------------------------------------------------------
    #   Missingness - non claims pct
    # -------------------------------------------------------------------------------------------------
    class Missingness():
        class non_claims_pct():

            group_by = {
                'ELG': "group by msis_ident_num",
                'MCR': "group by state_plan_id_num",
                'PRV': "group by submtg_state_prvdr_id",
                'TPL': "group by msis_ident_num"
            }

    # -------------------------------------------------------------------------------------------------
    #   Reports
    # -------------------------------------------------------------------------------------------------
    class Reports():

        rpt_to_fn = {
            'summary': 'all_IM',
            'plan': 'planid',
            'waiver': 'elg71'
        }
        class waiver():

            columns = [
                'waiver_id',
                'waiver_type',
                'submtg_state_cd',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'SpecVersion',
                'RunID',
                'Statistic_Year_Month']

            types = {
                'waiver_id': str,
                'waiver_type': str,
                'submtg_state_cd': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'SpecVersion': str,
                'RunID': str,
                'Statistic_Year_Month': str
            }
        class plan8_2():

            columns = [
                'plan_id',
                'plan_type_el',
                'MultiplePlanTypes_el',
                'plan_type_mc',
                'MultiplePlanTypes_mc',
                'In_MCR_File',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'Statistic_Year_Month',
                'SpecVersion',
                'RunID',
                'Measure_Type',
                'Active_Ind',
                'Display_Type',
                'Calculation_Source',
                'in_measures',
                'in_thresholds']

            id_vars = ['plan_id','plan_type_el','MultiplePlanTypes_el','plan_type_mc','MultiplePlanTypes_mc','linked','Measure_ID','Report_State','Month_Added','Statistic_Year_Month','SpecVersion','RunID']

            value_vars = ['capitation_type', 'encounter_type', 'enrollment', 'cap_hmo','cap_php','cap_pccm','cap_phi','cap_oth','cap_tot','cap_ratio','enc_ip','enc_lt','enc_ot','enc_rx','ip_ratio','lt_ratio','ot_ratio','rx_ratio']

            statistic_type_formats = {
                'capitation_type': 'Capitation Type',
                'encounter_type': 'Encounter Type',
                'enrollment': 'Enrollment',
                'cap_hmo': 'HMO capitation',
                'cap_php': 'PHP capitation',
                'cap_pccm': 'PCCM capitation',
                'cap_phi': 'PHI capitation',
                'cap_oth': 'Other capitation',
                'cap_tot': 'Total capitation',
                'cap_ratio': 'Capitation Ratio',
                'enc_ip': 'IP encounters',
                'enc_lt': 'LT encounters',
                'enc_ot': 'OT encounters',
                'enc_rx': 'RX encounters',
                'ip_ratio': 'IP ratio',
                'lt_ratio': 'LT ratio',
                'ot_ratio': 'OT ratio',
                'rx_ratio': 'RX ratio'
            }

            types = {
                'plan_id': str,
                'plan_type_el': str,
                'MultiplePlanTypes_el': str,
                'plan_type_mc': str,
                'MultiplePlanTypes_mc': str,
                'In_MCR_File': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'Statistic_Year_Month': str,
                'SpecVersion': str,
                'RunID': str,
                'Measure_Type': str,
                'Active_Ind': str,
                'Display_Type': str,
                'Calculation_Source': str,
                'in_measures': int64,
                'in_thresholds': int64
            }

        class plan9_1():

            columns = [
                'plan_id',
                'plan_type_el',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'Statistic_Year_Month',
                'SpecVersion',
                'RunID',
                'Measure_Type',
                'Active_Ind',
                'Display_Type',
                'Calculation_Source',
                'in_measures',
                'in_thresholds']

            types = {
                'plan_id': str,
                'plan_type_el': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'Statistic_Year_Month': str,
                'SpecVersion': str,
                'RunID': str,
                'Measure_Type': str,
                'Active_Ind': str,
                'Display_Type': str,
                'Calculation_Source': str,
                'in_measures': int64,
                'in_thresholds': int64
            }

        class summary():

            columns = [
                'Report_State',
                'Month_Added',
                'Measure_ID',
                'Statistic_Year_Month',
                'Statistic',
                'Numerator',
                'Denominator',
                'valid_value',
                'SpecVersion',
                'RunID',
                'Measure_Type',
                'Active_Ind',
                'Display_Type',
                'Calculation_Source',
                'in_measures',
                'in_thresholds',
                'numer',
                'denom',
                'claim_type',
                'plan_id']

            types = {
                'Report_State': str,
                'Month_Added': str,
                'Measure_ID': str,
                'Statistic_Year_Month': str,
                'Statistic': str,
                'Numerator': str,
                'Denominator': str,
                'valid_value': str,
                'SpecVersion': str,
                'RunID': str,
                'Measure_Type': str,
                'Active_Ind': str,
                'Display_Type': str,
                'Calculation_Source': str,
                'in_measures': int64,
                'in_thresholds': int64,
                'numer': float64,
                'denom': float64,
                'claim_type': str,
                'plan_id': str
            }

        class spark():

            types = {
                'submtg_state_cd': str,
                'measure_id': str,
                'submodule': str,
                'numer': Decimal,
                'denom': Decimal,
                'mvalue': Decimal,
                'valid_value': str,
                'claim_type': str
            }

        class one_value():

            measure_ids = ['ALL18_1', 'ALL18_2', 'ALL18_3', 'ALL18_4']

    # -------------------------------------------------------------------------------------------------
    #   Results
    # -------------------------------------------------------------------------------------------------
    class Results():

        columns = [
            'submtg_state_cd',
            'measure_id',
            'submodule',
            'numer',
            'denom',
            'mvalue',
            'valid_value',
            'claim_type',
            'plan_id',
            'plan_type_el',
            'MultiplePlanTypes_el',
            'plan_type_mc',
            'MultiplePlanTypes_mc',
            'linked',
            'enrollment',
            'cap_hmo',
            'cap_php',
            'cap_pccm',
            'cap_phi',
            'cap_oth',
            'cap_tot',
            'capitation_type',
            'plan_type',
            'enc_ip',
            'enc_lt',
            'enc_ot',
            'enc_rx',
            'enc_tot',
            'ip_ratio',
            'lt_ratio',
            'ot_ratio',
            'rx_ratio',
            'cap_ratio',
            'encounter_type',
            'waiver_id',
            'waiver_type']

        types = {
            'submtg_state_cd': str,
            'measure_id': str,
            'submodule': str,
            'numer': float64,
            'denom': float64,
            'mvalue': float64,
            'valid_value': str,
            'claim_type': str,
            'plan_id': str,
            'plan_type_el': str,
            'MultiplePlanTypes_el': float64,
            'plan_type_mc': str,
            'MultiplePlanTypes_mc': float64,
            'linked': str,
            'enrollment': float64,
            'cap_hmo': float64,
            'cap_php': float64,
            'cap_pccm': float64,
            'cap_phi': float64,
            'cap_oth': float64,
            'cap_tot': float64,
            'capitation_type': str,
            'plan_type': str,
            'enc_ip': float64,
            'enc_lt': float64,
            'enc_ot': float64,
            'enc_rx': float64,
            'enc_tot': float64,
            'ip_ratio': float64,
            'lt_ratio': float64,
            'ot_ratio': float64,
            'rx_ratio': float64,
            'cap_ratio': float64,
            'encounter_type': str,
            'waiver_id': str,
            'waiver_type': str}

    # -------------------------------------------------------------------------------------------------
    #   Rounding Adjustments
    # -------------------------------------------------------------------------------------------------
    class Rounding():

        round_noop = ['EXP28_2', 
                      'EL10_1', 'EL3_14', 'EL3_20', 'EL3_23', 'EL6_26', 'EL6_27']

        round0 = ['EXP1_4']

        round1 = ['EXP22_7', 'EXP28_1', 'EXP23_1']

        round2 = ['EL13_1',
                  'EL5_1',
                  'EL5_3',
                  'EXP14_4', 'EXP45_4', 'EXP45_5', 'EXP45_6',
                  'EL10_3', 'EL10_4',
                  'EL1_5',
                  'MCR28_1', 'MCR56_1', 'MCR57_1', 'FFS47_1', 'FFS48_1',
                  'MCR56P_1',
                  'EL3_37','EL3_38']

        round3 = ['EL1_20',
                  'EL3_21',
                  'FFS19_1',
                  'FFS11_9',
                  'MCR9_18',
                  'MCR14_9', 'MCR32_1', 'MCR32_2', 'MCR32_4', 'MCR32_5',
                  'MCR32_6', 'MCR32_7', 'MCR32_8', 'MCR32_9', 'MCR32_10', 'MCR32_11', 'MCR32_12',
                  'MCR32_13', 'MCR32_14', 'MCR32_16', 'MCR32_18', 'MCR32_20',
                  'MCR13_18', 'MCR62_4',
                  'MCR9_20', 'MCR13_20', 'MCR9_21', 'MCR13_21',
                  'FFS26_1', 'FFS26_2', 'FFS26_3', 'FFS26_4', 'FFS26_5', 'FFS26_6', 'FFS26_7',
                  'FFS26_8', 'FFS26_9', 'FFS26_10', 'FFS26_11', 'FFS26_12', 'FFS26_13', 'FFS26_14', 'FFS26_15',
                  'FFS26_16', 'FFS52_4', 'FFS19_1', 'MCR32_15', 'MCR32_17', 'MCR32_19',
                  'FFS53_1', 'MCR63_1', 'FFS53_2', 'MCR63_2',
                  'FFS53_3', 'MCR63_3', 'FFS53_4', 'MCR63_4',
                  'EXP16_1', 'EXP2_1',
                  'MIS5_13',
                  'EL3_22',
                  'ALL34_1', 'ALL34_2', 'ALL35_1', 'ALL35_2', 'ALL35_3', 'ALL35_4', 'ALL36_1',
                  'EL6_38', 'EL6_39', 'EL6_42', 'EL6_43', 'EL6_44'
                  ]

        round4 = ['FFS10_3',
                  'FFS10_84',
                  'MCR10_19',
                  'FFS1_17',
                  'FFS1_18',
                  'FFS1_2',
                  'FFS1_4',
                  'FFS18_2',
                  'FFS3_2',
                  'FFS3_6',
                  'FFS7_11',
                  'SUMFFS_13',
                  'SUMFFS_19',
                  'SUMFFS_16',
                  'SUMFFS_21',
                  'FFS5_11',
                  'FFS5_12',
                  'FFS5_24',
                  'FFS5_25',
                  'FFS5_9',
                  'FFS9_2',
                  'FFS9_9',
                  'FFS9_98',
                  'SUMFFS_10',
                  'SUMFFS_2',
                  'SUMFFS_5',
                  'SUMFFS_8',
                  'MCR10_19',
                  'MCR10_2',
                  'MCR10_9',
                  'MCR1_2',
                  'MCR1_4',
                  'MCR1_5',
                  'MCR1_6',
                  'MCR21_2',
                  'MCR5_11',
                  'MCR5_9',
                  'SUMMCR_10',
                  'SUMMCR_13',
                  'SUMMCR_2',
                  'SUMMCR_5',
                  'SUMMCR_8',
                  'MCR5_12',
                  'MCR3_2',
                  'MCR3_6',
                  'MCR7_9',
                  'MCR7_11',
                  'MCR7_12',
                  'SUMMCR_16',
                  'SUMMCR_19',
                  'SUMMCR_21',
                  'FFS7_9',
                  'FFS7_12'
                  ]

    # -------------------------------------------------------------------------------------------------
    #   US Federal Information Processing Standards
    # -------------------------------------------------------------------------------------------------
    class FIPS():
        # note this does not include
        # outlying areas under us sovereignty
        # minor outlying island territories
        # freely associated states
        stfips = {
            'AK': '02',
            'AL': '01',
            'AR': '05',
            'AS': '60',
            'AZ': '04',
            'CA': '06',
            'CO': '08',
            'CT': '09',
            'DC': '11',
            'DE': '10',
            'FL': '12',
            'GA': '13',
            'GU': '66',
            'HI': '15',
            'IA': '19',
            'ID': '16',
            'IL': '17',
            'IN': '18',
            'KS': '20',
            'KY': '21',
            'LA': '22',
            'MA': '25',
            'MD': '24',
            'ME': '23',
            'MI': '26',
            'MN': '27',
            'MO': '29',
            'MS': '28',
            'MP': '69',
            'MT': '30',
            'NC': '37',
            'ND': '38',
            'NE': '31',
            'NH': '33',
            'NJ': '34',
            'NM': '35',
            'NV': '32',
            'NY': '36',
            'OH': '39',
            'OK': '40',
            'OR': '41',
            'PA': '42',
            'PR': '72',
            'RI': '44',
            'SC': '45',
            'SD': '46',
            'TN': '47',
            'TX': '48',
            'UT': '49',
            'VA': '51',
            'VI': '78',
            'VT': '50',
            'WA': '53',
            'WI': '55',
            'WV': '54',
            'WY': '56',
        }

# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work;

#   ii. moral rights retained by the original author(s) and/or performer(s);

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work;

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below;

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work;

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive); and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>