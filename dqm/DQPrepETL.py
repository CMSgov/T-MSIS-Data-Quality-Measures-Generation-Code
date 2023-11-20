from dqm.DQClosure import DQClosure
from dqm.DQM_Metadata import DQM_Metadata

from pyspark.sql import SparkSession
from dqm.DQMeasures import DQMeasures


# -------------------------------------------------------------------------
#
#   Data Preparation and View Chains
#
# -------------------------------------------------------------------------
class DQPrepETL:

    # -------------------------------------------------------------------------
    #
    #   Assertion for missing MSIS ID
    #
    # -------------------------------------------------------------------------
    msis_id_not_missing = """(msis_ident_num is not null
        and msis_ident_num not in ('88888888888888888888','99999999999999999999')
        and msis_ident_num rlike '[A-Za-z1-9]')"""

    # -------------------------------------------------------------------------
    #
    #   Age calculation logic
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def select_calculateAge(next_tbl, end_date):
        if next_tbl == 'tmsis_prmry_dmgrphc_elgblty':
            return f"""
                   ,case when ( death_dt is not null and death_dt <= '{end_date}')
                    then floor( (datediff(death_dt, birth_dt) / 365.25 ) )
                    else floor( (datediff('{end_date}', birth_dt) / 365.25) ) end as age"""
        else:
            return ""

    # -------------------------------------------------------------------------
    #
    #   Constraint for Primary Eligibigily Group Indicator
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def where_primaryEligibilityGroupIndicator(next_tbl: str):
        if next_tbl == 'tmsis_elgblty_dtrmnt':
            return "and prmry_elgblty_grp_ind = '1'"
        else:
            return ""

    # -------------------------------------------------------------------------
    #
    #   Constraint for Primary Taxonomy Classication
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def where_primaryTaxonomyClassification(next_tbl: str, efctv_dt: str, end_dt: str):
        if next_tbl == 'tmsis_prvdr_txnmy_clsfctn':
            return f"or ({efctv_dt} is null and {end_dt} is null)"
        else:
            return ""

    # -------------------------------------------------------------------------
    #
    #   Selection clause for third-party liability
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def select_TplMedicaidPerson(ftype: str):
        if ftype == "tmsis_tpl_mdcd_prsn_mn":
            return """,tpl_insrnc_cvrg_ind
                      ,tpl_othr_cvrg_ind
                    """
        elif ftype == "tmsis_tpl_mdcd_prsn_hi":
            return """,insrnc_carr_id_num
                      ,insrnc_plan_id
                      ,insrnc_plan_type_cd
                      ,cvrg_type_cd
                    """

    # -------------------------------------------------------------------------
    #
    #   Remove extraneous whitespace from string
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def compress(string):
        return ' '.join(string.split())

    # -------------------------------------------------------------------------
    #
    #   log the generated formated SQL
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def log(dqm: DQMeasures, viewname: str, sql=''):
        dqm.logger.info('\t' + viewname)
        if sql != '':
            dqm.logger.debug(DQPrepETL.compress(sql.replace('\n', '')))
            dqm.sql[viewname] = '\n'.join(sql.split('\n')[2:])

    # -------------------------------------------------------------------------
    #
    #   base_view_calls.sas -> proc sql
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_all_base_views(dqm: DQMeasures):

        dqm.logger.info('Creating Base Views...')

        dqm.logger.info('Creating Base Eligibility Views...')

        DQPrepETL.create_base_elig_views(dqm)

        dqm.logger.info('Creating Base Claim Line Views...')

        DQPrepETL.create_base_cll_view(dqm, 'ip', 'ip')
        DQPrepETL.create_base_cll_view(dqm, 'lt', 'lt')
        DQPrepETL.create_base_cll_view(dqm, 'ot', 'othr_toc')
        DQPrepETL.create_base_cll_view(dqm, 'rx', 'rx')

        dqm.logger.info('Creating Base Claim Header Views...')

        DQPrepETL.create_base_clh_view(dqm, 'ip', 'ip')
        DQPrepETL.create_base_clh_view(dqm, 'lt', 'lt')
        DQPrepETL.create_base_clh_view(dqm, 'ot', 'othr_toc')
        DQPrepETL.create_base_clh_view(dqm, 'rx', 'rx')

        dqm.logger.info('Creating Base Eligibility Info Views...')

        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_sect_1115a_demo_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_dsblty_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_elgblty_dtrmnt')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_elgbl_cntct')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_ethncty_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_hcbs_chrnc_cond_non_hh')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_hh_chrnc_cond')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_hh_sntrn_prtcptn_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_hh_sntrn_prvdr')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_lckin_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_ltss_prtcptn_data')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_state_plan_prtcptn')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_mc_prtcptn_data')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_mfp_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_prmry_dmgrphc_elgblty')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_race_info')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_var_dmgrphc_elgblty')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_wvr_prtcptn_data')
        DQPrepETL.create_base_elig_info_view(dqm, 'tmsis_enrlmt_time_sgmt_data')

        dqm.logger.info('Creating Base TPL Views...')

        DQPrepETL.create_base_tpl_view(dqm, 'tmsis_tpl_mdcd_prsn_mn')
        DQPrepETL.create_base_tpl_view(dqm, 'tmsis_tpl_mdcd_prsn_hi')

        dqm.logger.info('Creating Base Provider Info Views...')

        DQPrepETL.create_base_prov_view(dqm)

        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_attr_mn')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_lctn_cntct')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_id')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_lcnsg')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_txnmy_clsfctn')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_mdcd_enrlmt')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_afltd_grp')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_afltd_pgm')
        DQPrepETL.create_base_prov_info_view(dqm, 'tmsis_prvdr_bed_type')

        dqm.logger.info('Creating Base Managed Care Views...')

        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_mn_data')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_lctn_cntct')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_sarea')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_oprtg_authrty')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_plan_pop_enrld')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_mc_acrdtn_org')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_natl_hc_ent_id_info')
        DQPrepETL.create_base_mc_view(dqm, 'tmsis_chpid_shpid_rltnshp_data')

    # -------------------------------------------------------------------------
    #
    #   Missingness Views
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_all_missingness_views(dqm: DQMeasures):

        dqm.logger.info('Creating Base Missingness Views...')

        DQPrepETL.create_msng_tbl(dqm, 'prmry_dmgrphc_elgblty', 1)
        DQPrepETL.create_msng_tbl(dqm, 'var_dmgrphc_elgblty', 1)
        DQPrepETL.create_msng_tbl(dqm, 'elgbl_cntct', 1)
        DQPrepETL.create_msng_tbl(dqm, 'elgblty_dtrmnt', 1)
        DQPrepETL.create_msng_tbl(dqm, 'hh_sntrn_prtcptn_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'hh_sntrn_prvdr', 1)
        DQPrepETL.create_msng_tbl(dqm, 'hh_chrnc_cond', 1)
        DQPrepETL.create_msng_tbl(dqm, 'lckin_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'mfp_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'state_plan_prtcptn', 1)
        DQPrepETL.create_msng_tbl(dqm, 'wvr_prtcptn_data', 1)
        DQPrepETL.create_msng_tbl(dqm, 'ltss_prtcptn_data', 1)
        DQPrepETL.create_msng_tbl(dqm, 'mc_prtcptn_data', 1)
        DQPrepETL.create_msng_tbl(dqm, 'ethncty_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'race_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'dsblty_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'sect_1115a_demo_info', 1)
        DQPrepETL.create_msng_tbl(dqm, 'hcbs_chrnc_cond_non_hh', 1)
        DQPrepETL.create_msng_tbl(dqm, 'enrlmt_time_sgmt_data', 1)
        DQPrepETL.create_msng_tbl(dqm, 'mc_mn_data', 0)
        DQPrepETL.create_msng_tbl(dqm, 'mc_lctn_cntct', 0)
        DQPrepETL.create_msng_tbl(dqm, 'mc_sarea', 0)
        DQPrepETL.create_msng_tbl(dqm, 'mc_oprtg_authrty', 0)
        DQPrepETL.create_msng_tbl(dqm, 'mc_plan_pop_enrld', 0)
        DQPrepETL.create_msng_tbl(dqm, 'mc_acrdtn_org', 0)
        DQPrepETL.create_msng_tbl(dqm, 'chpid_shpid_rltnshp_data', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_attr_mn', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_lctn_cntct', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_lcnsg', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_id', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_txnmy_clsfctn', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_mdcd_enrlmt', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_afltd_grp', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_afltd_pgm', 0)
        DQPrepETL.create_msng_tbl(dqm, 'prvdr_bed_type', 0)
        DQPrepETL.create_msng_tbl(dqm, 'tpl_mdcd_prsn_mn', 1)

    # -------------------------------------------------------------------------
    #
    #   %macro create_tables;
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_tables(dqm: DQMeasures):

        dqm.logger.info('Creating Base Tables...')

        dqm.logger.info('Creating Base Eligibility Tables...')

        DQPrepETL.create_elig_tables(dqm, 'current')
        DQPrepETL.create_elig_tables(dqm, 'prior')

        dqm.logger.info('Creating Base Provider Tables...')

        DQPrepETL.create_prov_tables(dqm)

        dqm.logger.info('Creating Base Managed Care Tables...')

        DQPrepETL.create_mcplan_tables(dqm)

        dqm.logger.info('Creating Base TPL Tables...')

        DQPrepETL.create_tpl_tables(dqm)

        dqm.logger.info('Creating Base Claims Tables...')

        DQPrepETL.create_claims_tables(dqm, 'ip')
        DQPrepETL.create_claims_tables(dqm, 'lt')
        DQPrepETL.create_claims_tables(dqm, 'ot')
        DQPrepETL.create_claims_tables(dqm, 'rx')

    # -------------------------------------------------------------------------
    #
    #   View specific to individual measures/rules
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_peripheral_tables(dqm: DQMeasures):

        dqm.logger.info('Creating Peripheral Tables...')

        # plan IDs
        DQPrepETL.all_plan_ids(dqm)

        # 900 series
        DQPrepETL.run_912_other_measures_PCCM(dqm)

        # 100 series
        DQPrepETL.run_104_el401a(dqm)
        DQPrepETL.preprocess_el12x(dqm)

        # 700 series
        DQPrepETL.prgncy_codes(dqm)
        DQPrepETL.ot_prep_clm_l(dqm)
        DQPrepETL.utl_state_plan(dqm)
        DQPrepETL.utl_wvr(dqm)
        DQPrepETL.utl_el_sql(dqm)
        DQPrepETL.get_prov_ever_enrld_sql(dqm)
        DQPrepETL.ot_hdr_clm_ab(dqm)
        DQPrepETL.ot_prep_clm2_ab(dqm)
        DQPrepETL.utl_1915_wvr(dqm)
        DQPrepETL.get_prov_id_sql(dqm)

        DQPrepETL.msng_cll_recs(dqm, 'ip')
        DQPrepETL.msng_cll_recs(dqm, 'lt')
        DQPrepETL.msng_cll_recs(dqm, 'ot')
        DQPrepETL.msng_cll_recs(dqm, 'rx')
        DQPrepETL.msng_clh_recs(dqm, 'ip')
        DQPrepETL.msng_clh_recs(dqm, 'lt')
        DQPrepETL.msng_clh_recs(dqm, 'ot')
        DQPrepETL.msng_clh_recs(dqm, 'rx')

        # 600 series
        DQPrepETL.prep_clm(dqm, 'ip', 'clh', 'A')
        DQPrepETL.prep_clm(dqm, 'ip', 'clh', 'P')
        DQPrepETL.prep_clm(dqm, 'ip', 'clh', 'F')
        DQPrepETL.prep_clm(dqm, 'ip', 'clh', 'R')

        DQPrepETL.prep_clm(dqm, 'lt', 'clh', 'A')
        DQPrepETL.prep_clm(dqm, 'lt', 'clh', 'P')
        DQPrepETL.prep_clm(dqm, 'lt', 'clh', 'F')
        DQPrepETL.prep_clm(dqm, 'lt', 'clh', 'R')

        DQPrepETL.prep_clm(dqm, 'ot', 'cll', 'A')
        DQPrepETL.prep_clm(dqm, 'ot', 'cll', 'P')
        DQPrepETL.prep_clm(dqm, 'ot', 'cll', 'F')
        DQPrepETL.prep_clm(dqm, 'ot', 'cll', 'R')

        DQPrepETL.prep_clm(dqm, 'rx', 'clh', 'A')
        DQPrepETL.prep_clm(dqm, 'rx', 'clh', 'P')
        DQPrepETL.prep_clm(dqm, 'rx', 'clh', 'F')
        DQPrepETL.prep_clm(dqm, 'rx', 'clh', 'R')

        # 500 series
        DQPrepETL.prvdr_txnmy(dqm)
        DQPrepETL.prvdr_freq_t(dqm)
        DQPrepETL.prvdr_freq_t2(dqm)

    # -------------------------------------------------------------------------
    #
    #   Turbo Mode - Materialized Views
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def materialize_views(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLL_ip using DELTA partitioned by (clm_type_cd) as select * from ' + dqm.taskprefix + '_base_cll_ip')
        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLL_lt using DELTA partitioned by (clm_type_cd) as select * from ' + dqm.taskprefix + '_base_cll_lt')

        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLL_ot using DELTA partitioned by (clm_type_cd) as select * from ' + dqm.taskprefix + '_base_cll_ot')
        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLL_rx using DELTA partitioned by (clm_type_cd) as select * from ' + dqm.taskprefix + '_base_cll_rx')

        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLH_ip using DELTA partitioned by (ADJSTMT_IND) as select * from ' + dqm.taskprefix + '_base_clh_ip')
        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLH_lt using DELTA partitioned by (ADJSTMT_IND) as select * from ' + dqm.taskprefix + '_base_clh_lt')

        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLH_ot using DELTA partitioned by (ADJSTMT_IND) as select * from ' + dqm.taskprefix + '_base_clh_ot')
        spark.sql('create table if not exists ' + dqm.turboDB + '.' + dqm.taskprep + '_' + dqm.z_run_id + '_prepop_CLH_rx using DELTA partitioned by (ADJSTMT_IND) as select * from ' + dqm.taskprefix + '_base_clh_rx')

    # -------------------------------------------------------------------------
    #
    #   Turbo Mode - Materialized Views
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def shared_metadata_views(dqm: DQMeasures):
        import pandas as pd

        spark = SparkSession.getActiveSession()

        df = pd.DataFrame(['A_', 'M_', 'N_', 'T_'] + dqm.stc_cd['z_tos'].tolist(), columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.freq_msr_tos")

        df = pd.DataFrame(['A_', 'M_', 'N_', 'T_'] +
                  ['011', '012', '013', '014', '015', '016', '018', '021', '022',
                   '023', '024', '025', '026', '028', '031', '032', '033', '034',
                   '035', '036', '038', '041', '042', '043', '044', '045', '046',
                   '048', '061', '062', '063', '064', '065', '066', '068', '071',
                   '072', '073', '074', '075', '076', '077', '078', '079', '081',
                   '082', '083', '084', '085', '086', '087', '089'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.freq_msr_billtype")

        df = pd.DataFrame(['A_', 'M_', 'N_', 'T_'] +
                ['1932', '1934', '261Q', '282N', '283Q', '313M', '3140', '3902', '4053',
                '10XX', '11XX', '12XX', '13XX', '14XX', '15XX', '16XX', '17XX', '18XX',
                '20XX', '21XX', '22XX', '23XX', '24XX', '25XX', '26XX', '27XX', '28XX', '29XX',
                '30XX', '31XX', '32XX', '33XX', '34XX', '36XX', '37XX', '38XX'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.freq_msr_tax")

        df = pd.DataFrame(['A_', 'N_', 'T_'] +
                ['1','2','3','4','5','6','A','B','C','D','E','F','U','V','W','X','Y','Z'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.clm_type_cd")

        df = pd.DataFrame(['A_', 'N_', 'T_'] +
                ['01','02','03','04','05','06','07','08','09','10','20','22','23'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.src_lctn_cd")
        
        df = pd.DataFrame([['1','TAXONOMY CODE'],['2','PROVIDER SPECIALTY CODE'],['3','PROVIDER TYPE CODE'],
                        ['4','AUTHORIZED CATEGORY OF SERVICE CODE'],['A','ANY VALID VALUE'],['N','NO VALID VALUE'],
                        ['T','TOTAL']], columns=['valid_value','label'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.prvdr_clsfctn_type_cd")

        df = pd.DataFrame(['A', 'N', 'T'] +
                ['1','2','9'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.enrlmt_type_cd")

        apdxc_dict = {'elgblty_grp_cd': [2, ['A', 'N', 'T']], 'race_cd': [3, ['A_', 'N_', 'T_']], 'imgrtn_stus_cd': [0, ['A', 'N', 'T']]}
        for var in apdxc_dict.keys():
            var_df = dqm.apdxc.loc[dqm.apdxc['Variable'] == f"{var}"]
            var_df['z_code'] = var_df['Code'].apply(lambda x: x.zfill(apdxc_dict[var][0]))
            df = pd.DataFrame(apdxc_dict[var][1] + var_df['z_code'].tolist(), columns=['valid_value'])
            df['mvalue'] = 0
            spark.createDataFrame(df).write.mode("ignore").saveAsTable(f"dqm_conv.{var}")

        df = pd.DataFrame(['A_', 'N_', 'T_'] +
                ['0','1','2','3','4','5','6','7','A','B','C','D','E','F','G'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.rstrctd_bnfts_cd")

        df = pd.DataFrame(['A_','N_','T_'] +
                ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20', \
		        '21','22','23','24','25','26','27','28','29','30','31'], columns=['valid_value'])
        df['mvalue'] = 0
        spark.createDataFrame(df).write.mode("ignore").saveAsTable("dqm_conv.elgblty_chg_rsn_cd")

        spark.createDataFrame(dqm.zipstate_lookup).write.mode("ignore").saveAsTable("dqm_conv.zipstate_crosswalk")
        spark.createDataFrame(dqm.countystate_lookup).write.mode("ignore").saveAsTable("dqm_conv.countystate_lookup")
        spark.createDataFrame(dqm.prgncy).write.mode("ignore").saveAsTable("dqm_conv.prgncy")

        dqm.provider_classification_lookup = dqm.provider_classification_lookup.rename(columns={"Source List": "source_list", "PROV-CLASSIFICATION-TYPE": "prov_class_type"})
        spark.createDataFrame(dqm.provider_classification_lookup).write.mode("ignore").saveAsTable("dqm_conv.provider_classification_lookup")  

        dqm.atypical_provider_table = dqm.atypical_provider_table.rename(columns={"Provider_classification_type": "prov_class_type", "Provider_classification_code": "prov_class_cd","NPI_Required": "NPI_req"})
        spark.createDataFrame(dqm.atypical_provider_table).write.mode("ignore").saveAsTable("dqm_conv.atypical_provider_table")

        dqm.prvtxnmy = dqm.prvtxnmy.rename(columns={"Individual or Groups (of Individuals)": "Group"})
        spark.createDataFrame(dqm.prvtxnmy).write.mode("ignore").saveAsTable("dqm_conv.prvtxnmy")

    # -------------------------------------------------------------------------
    #
    #   Turbo Mode - Drop Materialized Views
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def drop_views(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_clh_ip')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_clh_lt')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_clh_ot')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_clh_rx')

        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_cll_ip')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_cll_lt')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_cll_ot')
        spark.sql('drop table ' + dqm.turboDB + '.' + dqm.taskprep + '_prepop_cll_rx')

    # -------------------------------------------------------------------------
    #
    #   %macro create_base_clh_view(ftype,fname)
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_base_clh_view(dqm: DQMeasures, ftype: str, fname: str):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view prep_clh_{ftype}_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,tmsis_rptg_prd
                    ,coalesce(orgnl_clm_num,'0') as orgnl_clm_num
                    ,coalesce(adjstmt_clm_num,'0') as adjstmt_clm_num
                    ,coalesce(adjdctn_dt,'01JAN1960') as adjdctn_dt
                    ,coalesce(adjstmt_ind,'X') as adjstmt_ind

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
                    ,tot_bene_coinsrnc_pd_amt
                    ,tot_bene_copmt_pd_amt
                    ,tot_bene_ddctbl_pd_amt
                    ,cll_cnt
                    ,wvr_id
                    ,srvc_trkng_pymt_amt
                    ,srvc_trkng_type_cd
                    ,src_lctn_cd

                    {DQM_Metadata.create_base_clh_view.select[ftype]}

                    ,case when clm_stus_ctgry_cd = 'F2' then 1
                            when clm_dnd_ind = '0' then 1
                            when clm_type_cd = 'Z' then 1
                            when clm_stus_cd in ('26','026','87','087','542','585','654') then 1
                            else 0 end as denied_header
                from
                    tmsis.tmsis_clh_rec_{fname}
                where
                    tmsis_actv_ind = 1
                    and (orgnl_clm_num is not null or adjstmt_clm_num is not null)
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'prep_clh_' + ftype + '_view', z)

    # -------------------------------------------------------------------------
    #
    #   %macro create_claims_tables()
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_claims_tables(dqm: DQMeasures, clm_file: str):

        spark = SparkSession.getActiveSession()

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view {dqm.taskprefix}_dup_clh_{clm_file} as
                select *
                    ,'{dqm.state}' as submtg_state_cd
                    ,count(1) over (partition by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt, adjstmt_ind) as count_claims_key
                from
                    prep_clh_{clm_file}_view
                where
                    {dqm.run_id_filter()}
                    and tmsis_rptg_prd = '{dqm.m_start}'
                    and denied_header = 0
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_dup_clh_' + clm_file, z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view {dqm.taskprefix}_base_clh_{clm_file} as
                select
                    *
                from
                    {dqm.taskprefix}_dup_clh_{clm_file}
                where
                    count_claims_key = 1

            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_base_clh_' + clm_file, z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view {dqm.taskprefix}_dup_cll_{clm_file}_prep  as
                select
                    *
                    ,'{dqm.state}' as submtg_state_cd
                    ,count(1) over (partition by orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                        orgnl_line_num, adjstmt_line_num, line_adjstmt_ind) as count_cll_key
                from
                   prep_cll_{clm_file}_view
                where
                        {dqm.run_id_filter()}
                    and tmsis_rptg_prd = '{dqm.m_start}'
                    and denied_line = 0
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_dup_cll_' + clm_file + '_prep', z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view {dqm.taskprefix}_dup_cll_{clm_file} as
                select
                     a.*
                    ,b.denied_header
                from
                    {dqm.taskprefix}_dup_cll_{clm_file}_prep a
                left join
                    (select distinct
                         orgnl_clm_num
                        ,adjstmt_clm_num
                        ,adjdctn_dt
                        ,adjstmt_ind
                        ,denied_header
                    from
                        prep_clh_{clm_file}_view
                    where
                        tmsis_run_id = '{dqm.run_id}'
                        and tmsis_rptg_prd = '{dqm.m_start}'
                    ) b on
                            a.orgnl_clm_num = b.orgnl_clm_num
                        and a.adjstmt_clm_num = b.adjstmt_clm_num
                        and a.adjdctn_dt = b.adjdctn_dt
                        and a.line_adjstmt_ind = b.adjstmt_ind
                where
                    (denied_header = 0 or denied_header is null)
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_dup_cll_' + clm_file, z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view {dqm.taskprefix}_base_cll_{clm_file} as
                select
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

                    ,b.orgnl_line_num_orig
                    ,b.adjstmt_line_num_orig
                    ,b.line_adjstmt_ind_orig
                    ,b.line_adjdctn_dt_orig

                    ,b.xix_srvc_ctgry_cd
                    ,b.xxi_srvc_ctgry_cd
                    ,b.cll_stus_cd
                    ,b.mdcd_ffs_equiv_amt
                    ,b.mdcd_pd_amt

                    {DQM_Metadata.create_claims_tables.b.select[clm_file]}

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
                    ,a.src_lctn_cd
                    ,a.plan_id_num

                    {DQM_Metadata.create_claims_tables.a.select[clm_file]}

                from
                    {dqm.taskprefix}_base_clh_{clm_file} a
                left join
                    {dqm.taskprefix}_dup_cll_{clm_file} b on
                            a.orgnl_clm_num = b.orgnl_clm_num
                        and a.adjstmt_clm_num = b.adjstmt_clm_num
                        and a.adjdctn_dt = b.adjdctn_dt
                        and a.adjstmt_ind = b.line_adjstmt_ind
                where
                    (b.count_cll_key = 1 or b.count_cll_key is null)

            """

        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_base_cll_' + clm_file, z)

    # -------------------------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_base_cll_view(dqm: DQMeasures, ftype: str, fname: str):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view prep_cll_{ftype}_view as
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

                    ,orgnl_line_num as orgnl_line_num_orig
                    ,adjstmt_line_num as adjstmt_line_num_orig
                    ,line_adjstmt_ind as line_adjstmt_ind_orig
                    ,adjdctn_dt as line_adjdctn_dt_orig
                    ,xix_srvc_ctgry_cd
                    ,xxi_srvc_ctgry_cd
                    ,cll_stus_cd
                    ,mdcd_ffs_equiv_amt
                    ,mdcd_pd_amt

                    {DQM_Metadata.create_base_cll_view.select[ftype]}

                    ,case when cll_stus_cd in
                        ('26','026','87','087','542','585','654') then 1 else 0 end as denied_line

                from
                    tmsis.tmsis_cll_rec_{fname}
                where
                    tmsis_actv_ind = 1
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'prep_cll_' + ftype + '_view', z)

    # -------------------------------------------------------------------------
    #
    #   %macro create_base_elig_views()
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_base_elig_views(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view base_elig_view as
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
                    and {DQPrepETL.msis_id_not_missing}
                    and (is_archived = 'false' or is_archived is null)
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_elig_view', z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view base_elig_all_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,msis_ident_num
                    ,enrlmt_type_cd
                    ,enrlmt_efctv_dt
                    ,enrlmt_end_dt
                    ,1 as is_eligible_all
                from tmsis.tmsis_enrlmt_time_sgmt_data
                where tmsis_actv_ind = 1
                    and {DQPrepETL.msis_id_not_missing}            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_elig_all_view', z)

        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view base_dtrmnt_view as
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
                    and {DQPrepETL.msis_id_not_missing}
                    and prmry_elgblty_grp_ind = '1'
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_dtrmnt_view', z)
        
        
        # -------------------------------------------------------------------------
        z = f"""
                create or replace temporary view base_prmry_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,msis_ident_num
                    ,prmry_dmgrphc_ele_efctv_dt
                    ,prmry_dmgrphc_ele_end_dt
                    ,gndr_cd
                    ,1 as ever_eligible_prm
                from tmsis.tmsis_prmry_dmgrphc_elgblty
                where tmsis_actv_ind = 1
                    and {DQPrepETL.msis_id_not_missing}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_prmry_view', z)

    # ------------------------------------------------------
    #
    #   %macro create_base_prov_view()
    #
    # ------------------------------------------------------
    @staticmethod
    def create_base_prov_view(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view base_prov_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,submtg_state_prvdr_id
                    ,prvdr_mdcd_efctv_dt
                    ,prvdr_mdcd_end_dt
                    ,1 as is_enrolled_provider
                from tmsis.tmsis_prvdr_mdcd_enrlmt
                where tmsis_actv_ind = 1
                    and submtg_state_prvdr_id is not null
                    and submtg_state_prvdr_id not rlike '[89]{{30}}'
                    and submtg_state_prvdr_id rlike '[A-Za-z1-9]'
                    and (is_archived = 'false' or is_archived is null)
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_prov_view', z)

        z = f"""
                create or replace temporary view base_prov_all_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,submtg_state_prvdr_id
                    ,prvdr_mdcd_efctv_dt
                    ,prvdr_mdcd_end_dt
                    ,1 as is_enrolled_provider_all
                from tmsis.tmsis_prvdr_mdcd_enrlmt
                where tmsis_actv_ind = 1
                    and submtg_state_prvdr_id is not null
                    and submtg_state_prvdr_id not rlike '[89]{{30}}'
                    and submtg_state_prvdr_id rlike '[A-Za-z1-9]'
            """
        spark.sql(z)
        DQPrepETL.log(dqm, 'base_prov_all_view', z)


    # -------------------------------------------------------------------------
    #
    #  %macro create_elig_tables(monthind=)
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_elig_tables(dqm: DQMeasures, monthind: str):

        spark = SparkSession.getActiveSession()

        suffix = ''

        if monthind == 'current':
            suffix = ''
            start_date = dqm.m_start
            end_date = dqm.m_end

        elif monthind == 'prior':
            suffix = '_prior'
            start_date = dqm.prior_m_start
            end_date = dqm.prior_m_end

        # base elig
        z = f"""
                create or replace temporary view {dqm.taskprefix}_base_elig{suffix} as
                select *
                    ,'{dqm.state}' as submtg_state_cd
                from base_elig_view
                where {dqm.run_id_filter()}
                and ((enrlmt_efctv_dt <= '{end_date}' and enrlmt_efctv_dt is not null)
                and (enrlmt_end_dt >= '{end_date}' or enrlmt_end_dt is null))
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm,  dqm.taskprefix + '_base_elig' + suffix, z)

        # ---------------------------------------------------------------------

        if monthind == 'current':
            # ever elig
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_ever_elig as
                    select *
                        ,'{dqm.state}' as submtg_state_cd
                        ,1 as ever_eligible
                    from
                        base_elig_all_view
                    where
                        {dqm.run_id_filter()}
                    {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm,  dqm.taskprefix + '_ever_elig', z)

            # ---------------------------------------------------------------------
            # ever elig determinant
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_ever_elig_dtrmnt as
                    select *
                        ,'{dqm.state}' as submtg_state_cd
                    from
                        base_dtrmnt_view
                    where
                        {dqm.run_id_filter()}
                    {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm,  dqm.taskprefix + '_ever_elig_dtrmnt', z)

            # ---------------------------------------------------------------------
            # ever elig prm
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_ever_elig_prmry  as
                    select *
                        ,'{dqm.state}' as submtg_state_cd
                    from
                        base_prmry_view
                    where
                        {dqm.run_id_filter()}
                    {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm,  dqm.taskprefix + '_ever_elig_prmry', z)
            # ---------------------------------------------------------------------
            # elig in month primary demographics (for EL1.15)
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_elig_in_month_prmry as
                    select *
                        ,'{dqm.state}' as submtg_state_cd
                    from
                        (select distinct msis_ident_num from {dqm.taskprefix}_ever_elig
                            where ((enrlmt_efctv_dt <= '{end_date}' and enrlmt_efctv_dt is not null)
                            and (enrlmt_end_dt >= '{start_date}' or enrlmt_end_dt is null))) a
                    left join tmsis_prmry_dmgrphc_elgblty_view b
                        on {dqm.run_id_filter()}
                        and a.msis_ident_num = b.msis_id
                        and (((prmry_dmgrphc_ele_efctv_dt <= '{end_date}' and prmry_dmgrphc_ele_efctv_dt is not null)
                            and (prmry_dmgrphc_ele_end_dt >= '{end_date}' or prmry_dmgrphc_ele_end_dt is null))
                            or (prmry_dmgrphc_ele_efctv_dt is null and prmry_dmgrphc_ele_end_dt is null))
                    {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm,  dqm.taskprefix + '_elig_in_month_prmry', z)
            # ---------------------------------------------------------------------

        # ---------------------------------------------------------------------

        dqm.logger.info('Creating Secondary Eligibilty Tables...')

        if monthind == 'current':
            tblList = DQM_Metadata.elig_tables.current.tblList
            dtPrefix = DQM_Metadata.elig_tables.current.dtPrefix
        elif monthind == 'prior':
            tblList = DQM_Metadata.elig_tables.prior.tblList
            dtPrefix = DQM_Metadata.elig_tables.prior.dtPrefix

        pos = 0

        for next_tbl in tblList:
            efctv_dt = dtPrefix[pos] + '_efctv_dt'
            end_dt = dtPrefix[pos] + '_end_dt'

            z = f"""
                    create or replace temporary view {dqm.taskprefix}_{next_tbl}{suffix} as
                    select
                        coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                        ,'{dqm.state}' as submtg_state_cd
                        ,b.*
                        {DQPrepETL.select_calculateAge(next_tbl, end_date)}
                    from
                        (select distinct msis_ident_num from {dqm.taskprefix}_base_elig{suffix}) a
                    left join
                        {next_tbl}_view b
                            on a.msis_ident_num = b.msis_id
                            and {dqm.run_id_filter()}

                            {DQPrepETL.where_primaryEligibilityGroupIndicator(next_tbl)}

                            and ((({efctv_dt} <= '{end_date}' and {efctv_dt} is not null)
                            and ({end_dt} >= '{end_date}' or {end_dt} is null))
                            or ({efctv_dt} is null and {end_dt} is null))
                """
            spark.sql(z)
            DQPrepETL.log(dqm,  dqm.taskprefix + '_' + next_tbl + suffix, z)
            pos += 1

    # -------------------------------------------------------------------------
    #
    #  %macro create_base_elig_info_view(ftype)
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_base_elig_info_view(dqm: DQMeasures, ftype: str):

        spark = SparkSession.getActiveSession()

        if ftype == "tmsis_prmry_dmgrphc_elgblty":
            saseff_dt = 'prmry_dmgrphc_ele_efctv_dt'
            sasend_dt = 'prmry_dmgrphc_ele_end_dt'

        elif ftype == "tmsis_var_dmgrphc_elgblty":
            saseff_dt = 'var_dmgrphc_ele_efctv_dt'
            sasend_dt = 'var_dmgrphc_ele_end_dt'

        elif ftype == "tmsis_elgbl_cntct":
            saseff_dt = 'elgbl_adr_efctv_dt'
            sasend_dt = 'elgbl_adr_end_dt'

        elif ftype == "tmsis_elgblty_dtrmnt":
            saseff_dt = 'elgblty_dtrmnt_efctv_dt'
            sasend_dt = 'elgblty_dtrmnt_end_dt'

        elif ftype == "tmsis_hh_sntrn_prtcptn_info":
            saseff_dt = 'hh_sntrn_prtcptn_efctv_dt'
            sasend_dt = 'hh_sntrn_prtcptn_end_dt'

        elif ftype == "tmsis_hh_sntrn_prvdr":
            saseff_dt = 'hh_sntrn_prvdr_efctv_dt'
            sasend_dt = 'hh_sntrn_prvdr_end_dt'

        elif ftype == "tmsis_hh_chrnc_cond":
            saseff_dt = 'hh_chrnc_efctv_dt'
            sasend_dt = 'hh_chrnc_end_dt'

        elif ftype == "tmsis_lckin_info":
            saseff_dt = 'lckin_efctv_dt'
            sasend_dt = 'lckin_end_dt'

        elif ftype == "tmsis_mfp_info":
            saseff_dt = 'mfp_enrlmt_efctv_dt'
            sasend_dt = 'mfp_enrlmt_end_dt'

        elif ftype == "tmsis_ltss_prtcptn_data":
            saseff_dt = 'ltss_elgblty_efctv_dt'
            sasend_dt = 'ltss_elgblty_end_dt'

        elif ftype == "tmsis_state_plan_prtcptn":
            saseff_dt = 'state_plan_optn_efctv_dt'
            sasend_dt = 'state_plan_optn_end_dt'

        elif ftype == "tmsis_wvr_prtcptn_data":
            saseff_dt = 'wvr_enrlmt_efctv_dt'
            sasend_dt = 'wvr_enrlmt_end_dt'

        elif ftype == "tmsis_mc_prtcptn_data":
            saseff_dt = 'mc_plan_enrlmt_efctv_dt'
            sasend_dt = 'mc_plan_enrlmt_end_dt'

        elif ftype == "tmsis_ethncty_info":
            saseff_dt = 'ethncty_dclrtn_efctv_dt'
            sasend_dt = 'ethncty_dclrtn_end_dt'

        elif ftype == "tmsis_race_info":
            saseff_dt = 'race_dclrtn_efctv_dt'
            sasend_dt = 'race_dclrtn_end_dt'

        elif ftype == "tmsis_dsblty_info":
            saseff_dt = 'dsblty_type_efctv_dt'
            sasend_dt = 'dsblty_type_end_dt'

        elif ftype == "tmsis_sect_1115a_demo_info":
            saseff_dt = 'sect_1115a_demo_efctv_dt'
            sasend_dt = 'sect_1115a_demo_end_dt'

        elif ftype == "tmsis_hcbs_chrnc_cond_non_hh":
            saseff_dt = 'ndc_uom_chrnc_non_hh_efctv_dt'
            sasend_dt = 'ndc_uom_chrnc_non_hh_end_dt'

        elif ftype == "tmsis_enrlmt_time_sgmt_data":
            saseff_dt = 'enrlmt_efctv_dt'
            sasend_dt = 'enrlmt_end_dt'

        z = f"""
                create or replace temporary view {ftype}_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,msis_ident_num as msis_id
                    ,{saseff_dt}
                    ,{sasend_dt}
                    {DQM_Metadata.create_base_elig_info_view.select[ftype]}
                from
                    tmsis.{ftype}
                where
                    tmsis_actv_ind = 1
                    and (is_archived = 'false' or is_archived is null)
            """
        spark.sql(z)
        DQPrepETL.log(dqm, ftype + '_view', z)


    # ------------------------------------------------------
    #
    #   %macro create_prov_tables()
    #
    # ------------------------------------------------------
    @staticmethod
    def create_prov_tables(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        DQPrepETL.log(dqm, "Creating prov tables")

        # -----------------------------------------------------------------
        # base_prov_table
        # %droptemptables(base_prov)
        z = f"""
                create or replace temporary view {dqm.taskprefix}_base_prov as
                select *
                    ,'{dqm.state}' as submtg_state_cd
                from
                    base_prov_view
                where {dqm.run_id_filter()}
                    and ((prvdr_mdcd_efctv_dt <= '{dqm.m_end}' and prvdr_mdcd_efctv_dt is not null)
                    and  (prvdr_mdcd_end_dt >= '{dqm.m_end}' or prvdr_mdcd_end_dt is null))
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_base_prov', z)


        # -----------------------------------------------------------------
        pos = 0

        for next_tbl in DQM_Metadata.prov_tables.tblList:
            efctv_dt = DQM_Metadata.prov_tables.dtPrefix[pos] + '_efctv_dt'
            end_dt   = DQM_Metadata.prov_tables.dtPrefix[pos] + '_end_dt'

            # %droptempviews({next_tbl})
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_{next_tbl} as
                    select
                        b.*
                        ,'{dqm.state}' as submtg_state_cd
                    from
                        (select distinct submtg_state_orig as state_orig, submtg_state_prvdr_id from {dqm.taskprefix}_base_prov) a
                    inner join {next_tbl}_view b
                        on a.submtg_state_prvdr_id = b.submtg_state_prvdr_id
                        and a.state_orig = b.submtg_state_orig
                        and {dqm.run_id_filter()}
                        and ((({efctv_dt} <= '{dqm.m_end}' and {efctv_dt} is not null)
                        and ({end_dt} >= '{dqm.m_end}' or {end_dt} is null))
                        {DQPrepETL.where_primaryTaxonomyClassification(next_tbl, efctv_dt, end_dt)})
                """
            spark.sql(z)
            DQPrepETL.log(dqm, dqm.taskprefix + '_' + next_tbl, z)
            pos += 1

        # -----------------------------------------------------------------
        pos = 0

        for next_tbl in DQM_Metadata.prov_tables.ever.tblList:
            ever_var = DQM_Metadata.prov_tables.ever.evrvarList[pos]

            # create ever provider view

            # %droptempviews(ever_{next_tbl})
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_ever_{next_tbl} as
                    select *
                    ,'{dqm.state}' as submtg_state_cd

                    ,case when (submtg_state_prvdr_id is not NULL and
                                not submtg_state_prvdr_id like repeat(8,30) and
                                not submtg_state_prvdr_id like repeat(9,30) and
                                submtg_state_prvdr_id rlike '[a-zA-Z1-9]')

                        then 1 else 0 end as {ever_var}

                    from {next_tbl}_view
                    where
                        {dqm.run_id_filter()}
                    {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm, dqm.taskprefix + '_ever_' + next_tbl, z)
            pos += 1

        # -----------------------------------------------------------------

    # ------------------------------------------------------
    #
    #   %macro create_base_prov_info_view()
    #
    # ------------------------------------------------------
    @staticmethod
    def create_base_prov_info_view(dqm: DQMeasures, ftype: str):

        spark = SparkSession.getActiveSession()

        if ftype == "tmsis_prvdr_attr_mn":
            saseff_dt = 'prvdr_attr_efctv_dt'
            sasend_dt = 'prvdr_attr_end_dt'

        elif ftype == "tmsis_prvdr_lctn_cntct":
            saseff_dt = 'prvdr_lctn_cntct_efctv_dt'
            sasend_dt = 'prvdr_lctn_cntct_end_dt'

        elif ftype == "tmsis_prvdr_lcnsg":
            saseff_dt = 'prvdr_lcns_efctv_dt'
            sasend_dt = 'prvdr_lcns_end_dt'

        elif ftype == "tmsis_prvdr_id":
            saseff_dt = 'prvdr_id_efctv_dt'
            sasend_dt = 'prvdr_id_end_dt'

        elif ftype == "tmsis_prvdr_txnmy_clsfctn":
            saseff_dt = 'prvdr_txnmy_clsfctn_efctv_dt'
            sasend_dt = 'prvdr_txnmy_clsfctn_end_dt'

        elif ftype == "tmsis_prvdr_mdcd_enrlmt":
            saseff_dt = 'prvdr_mdcd_efctv_dt'
            sasend_dt = 'prvdr_mdcd_end_dt'

        elif ftype == "tmsis_prvdr_afltd_grp":
            saseff_dt = 'prvdr_afltd_grp_efctv_dt'
            sasend_dt = 'prvdr_afltd_grp_end_dt'

        elif ftype == "tmsis_prvdr_afltd_pgm":
            saseff_dt = 'prvdr_afltd_pgm_efctv_dt'
            sasend_dt = 'prvdr_afltd_pgm_end_dt'

        elif ftype == "tmsis_prvdr_bed_type":
            saseff_dt = 'bed_type_efctv_dt'
            sasend_dt = 'bed_type_end_dt'

        z = f"""
                create or replace temporary view {ftype}_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,submtg_state_prvdr_id
                    ,{saseff_dt}
                    ,{sasend_dt}
                    {DQM_Metadata.create_base_prov_info_view.select[ftype]}
                from
                    tmsis.{ftype}
                where
                    tmsis_actv_ind = 1
                    and (is_archived = 'false' or is_archived is null)
            """
        spark.sql(z)
        DQPrepETL.log(dqm, ftype + '_view', z)


    # -------------------------------------------------------------------------
    #
    #   %macro create_tpl_tables()
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_tpl_tables(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        DQPrepETL.log(dqm, "Creating TPL tables")

        # -------------------------------------------------------------------------
        pos = 0

        for next_tbl in DQM_Metadata.tpl_tables.tblList:
            efctv_dt = DQM_Metadata.tpl_tables.dtPrefix[pos] + '_efctv_dt'
            end_dt = DQM_Metadata.tpl_tables.dtPrefix[pos] + '_end_dt'

            # %droptempviews({next_tbl})
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_{next_tbl} as
                    select
                        coalesce(a.msis_ident_num, b.msis_id) as msis_ident_num
                        ,'{dqm.state}' as submtg_state_cd
                        ,b.*
                    from
                        (select distinct msis_ident_num from {dqm.taskprefix}_base_elig) a
                    left join {next_tbl}_view b
                        on a.msis_ident_num = b.msis_id
                        and {dqm.run_id_filter()}
                        and ((({efctv_dt} <= '{dqm.m_end}' and {efctv_dt} is not null)
                        and ({end_dt} >= '{dqm.m_end}' or {end_dt} is null))
                        or ({efctv_dt} is null and {end_dt} is null))
                """
            spark.sql(z)
            DQPrepETL.log(dqm, dqm.taskprefix + '_' + next_tbl, z)
            pos += 1

        # -------------------------------------------------------------------------
        # create ever tpl
        # %droptempviews(ever_tpl)
        z = f"""
                create or replace temporary view {dqm.taskprefix}_ever_tpl as
                select
                    tmsis_run_id
                    ,'{dqm.state}' as submtg_state_cd
                    ,msis_id as msis_ident_num
                    ,elgbl_prsn_mn_efctv_dt
                    ,elgbl_prsn_mn_end_dt
                    ,tpl_insrnc_cvrg_ind
                    ,tpl_othr_cvrg_ind
                    ,1 as ever_tpl
                from
                    tmsis_tpl_mdcd_prsn_mn_view
                where
                    {dqm.run_id_filter()}
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_ever_tpl', z)

    # -------------------------------------------------------------------------
    #
    #   %macro create_base_tpl_view()
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def create_base_tpl_view(dqm: DQMeasures, ftype: str):

        spark = SparkSession.getActiveSession()

        if ftype == "tmsis_tpl_mdcd_prsn_mn":
            saseff_dt = 'elgbl_prsn_mn_efctv_dt'
            sasend_dt = 'elgbl_prsn_mn_end_dt'

        if ftype == "tmsis_tpl_mdcd_prsn_hi":
            saseff_dt = 'insrnc_cvrg_efctv_dt'
            sasend_dt = 'insrnc_cvrg_end_dt'

        z = f"""
                create or replace temporary view {ftype}_view as
                select
                    tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,msis_ident_num as msis_id
                    ,{saseff_dt}
                    ,{sasend_dt}
                    {DQPrepETL.select_TplMedicaidPerson(ftype)}
                from
                    tmsis.{ftype}
                where
                    tmsis_actv_ind = 1
            """
        spark.sql(z)
        DQPrepETL.log(dqm, ftype + '_view', z)


   # ------------------------------------------------------
    #
    #   %macro create_mcplan_tables()
    #
    # ------------------------------------------------------
    @staticmethod
    def create_mcplan_tables(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        DQPrepETL.log(dqm, "Creating MC plan tables")

        # -------------------------------------------------------------------------
        pos = 0

        for next_tbl in DQM_Metadata.mcplan_tables.tblList:
            efctv_dt = DQM_Metadata.mcplan_tables.dtPrefix[pos] + '_efctv_dt'
            end_dt = DQM_Metadata.mcplan_tables.dtPrefix[pos] + '_end_dt'

            # %droptempviews(next_tbl)
            z = f"""
                    create or replace temporary view {dqm.taskprefix}_{next_tbl} as
                    select
                        *
                        ,'{dqm.state}' as submtg_state_cd
                    from
                        {next_tbl}_view
                    where
                        {dqm.run_id_filter()}
                        and ((({efctv_dt} <= '{dqm.m_end}' and {efctv_dt} is not null)
                        and ({end_dt} >= '{dqm.m_end}' or {end_dt} is null))
                        or ({efctv_dt} is null and {end_dt} is null))
                        {dqm.limit}
                """
            spark.sql(z)
            DQPrepETL.log(dqm, dqm.taskprefix + '_' + next_tbl, z)
            pos += 1

    # ------------------------------------------------------
    #
    #   %macro create_base_mc_view()
    #
    # ------------------------------------------------------
    @staticmethod
    def create_base_mc_view(dqm: DQMeasures, ftype: str):

        spark = SparkSession.getActiveSession()

        if ftype == "tmsis_mc_mn_data":
            saseff_dt = 'mc_mn_rec_efctv_dt'
            sasend_dt = 'mc_mn_rec_end_dt'

        elif ftype == "tmsis_mc_lctn_cntct":
            saseff_dt = 'mc_lctn_cntct_efctv_dt'
            sasend_dt = 'mc_lctn_cntct_end_dt'

        elif ftype == "tmsis_mc_sarea":
            saseff_dt = 'mc_sarea_efctv_dt'
            sasend_dt = 'mc_sarea_end_dt'

        elif ftype == "tmsis_mc_oprtg_authrty":
            saseff_dt = 'mc_op_authrty_efctv_dt'
            sasend_dt = 'mc_op_authrty_end_dt'

        elif ftype == "tmsis_mc_plan_pop_enrld":
            saseff_dt = 'mc_plan_pop_efctv_dt'
            sasend_dt = 'mc_plan_pop_end_dt'

        elif ftype == "tmsis_mc_acrdtn_org":
            saseff_dt = 'acrdtn_achvmt_dt'
            sasend_dt = 'acrdtn_end_dt'

        elif ftype == "tmsis_natl_hc_ent_id_info":
            saseff_dt = 'natl_hlth_care_ent_id_efctv_dt'
            sasend_dt = 'natl_hlth_care_ent_id_end_dt'

        elif ftype == "tmsis_chpid_shpid_rltnshp_data":
            saseff_dt = 'chpid_shpid_rltnshp_efctv_dt'
            sasend_dt = 'chpid_shpid_rltnshp_end_dt'

        z = f"""
                create or replace temporary view {ftype}_view as
                select
                     tmsis_run_id
                    ,submtg_state_cd as submtg_state_orig
                    ,state_plan_id_num
                    ,{saseff_dt}
                    ,{sasend_dt}
                    {DQM_Metadata.mcplan_tables.base_mc_view_columns.select[ftype]}
                from
                    tmsis.{ftype}
                where
                    tmsis_actv_ind = 1
            """
        spark.sql(z)
        DQPrepETL.log(dqm, ftype + '_view', z)


    # ------------------------------------------------------
    #
    #   %macro create_msng_tbl(sgmt,el_flag)
    #
    # ------------------------------------------------------
    @staticmethod
    def create_msng_tbl(dqm: DQMeasures, sgmt: str, el_flag: int):

        spark = SparkSession.getActiveSession()

        el_flag_msis_ident_num = ''
        if el_flag == 1:
            el_flag_msis_ident_num = ',msis_id as msis_ident_num'

        z = f"""
                create or replace temporary view {dqm.taskprefix}_msng_{sgmt} as
                select *
                    ,'{dqm.state}' as submtg_state_cd
                    {el_flag_msis_ident_num}
                from
                    tmsis_{sgmt}_view
                where
                    {dqm.run_id_filter()}
                {dqm.limit}
            """
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_msng_' + sgmt, z)


   # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def all_plan_ids(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        def claim_plan_ids(clm_type):
            return f"""
                        SELECT plan_id_num AS plan_id
                        FROM {DQMeasures.getBaseTable(dqm, 'clh', clm_type)}
                        WHERE plan_id_num IS NOT NULL
                            AND clm_type_cd in ('2','3','B','C')
                        GROUP BY plan_id_num
                        UNION
                    """

        z = f"""
                create or replace temporary view {dqm.taskprefix}_plan_ids AS
                SELECT mc_plan_id AS plan_id
                FROM {dqm.taskprefix}_tmsis_mc_prtcptn_data
                WHERE mc_plan_id IS NOT NULL
                GROUP BY mc_plan_id
                UNION
                {claim_plan_ids('ip')}
                {claim_plan_ids('lt')}
                {claim_plan_ids('ot')}
                {claim_plan_ids('rx')}
                SELECT state_plan_id_num AS plan_id
                FROM {dqm.taskprefix}_tmsis_mc_mn_data
                where state_plan_id_num IS NOT NULL
                GROUP BY state_plan_id_num
             """
        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_plan_ids', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run_912_other_measures_PCCM(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_mc_data_912 AS

                SELECT mc_plan_id
                    ,msis_ident_num
                    ,max(CASE
                            WHEN (enrld_mc_plan_type_cd in ('02','03') )
                                THEN 1
                            ELSE 0
                            END) AS evr_pccm_mc_plan_type
                    ,max(CASE
                            WHEN (enrld_mc_plan_type_cd in ('05','06','07','08','09','10','11',
                                                            '12','13','14','15','16','18','19') )
                                THEN 1
                            ELSE 0
                            END) AS evr_php_mc_plan_type
                    ,max(CASE
                            WHEN (enrld_mc_plan_type_cd in ('01','04','17') )
                                THEN 1
                            ELSE 0
                            END) AS evr_mco_mc_plan_type
                FROM {dqm.taskprefix}_tmsis_mc_prtcptn_data
                WHERE mc_plan_id IS NOT NULL
                GROUP BY mc_plan_id
                        ,msis_ident_num
             """
        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_mc_data_912', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def run_104_el401a(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_el401a as
                select
                    a.enrlmt_type_cd,
                    coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num
                from
                    {dqm.taskprefix}_tmsis_enrlmt_time_sgmt_data as a
                inner join
                    {dqm.taskprefix}_tmsis_var_dmgrphc_elgblty as b
                        on a.msis_ident_num=b.msis_ident_num
                where
                    b.chip_cd = '2'
             """
        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_el401a', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def preprocess_el12x(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
            create or replace temporary view {dqm.taskprefix}_el12x as
            select
                a.*
                , case when b.ZipCode    is null then 1 else 0 end as nonmatchzip
                , case when c.CountyFIPS is null then 1 else 0 end as nonmatchcounty
                , case when d.ZipCode    is null then 1 else 0 end as nonmatchzip_elgbl
                , case when e.CountyFIPS is null then 1 else 0 end as nonmatchcounty_elgbl
                , case when f.StateFIPS  is not null then 1 else 0 end as elgbl_state_cd_match
            from
                {dqm.taskprefix}_tmsis_elgbl_cntct a

            left join
                dqm_conv.zipstate_crosswalk b
                    on substring(a.elgbl_zip_cd, 1, 5) = b.ZipCode and a.submtg_state_cd = b.StateFIPS

            left join
                dqm_conv.countystate_lookup c
                    on a.elgbl_cnty_cd = c.CountyFIPS and a.submtg_state_cd = c.StateFIPS

            left join
                dqm_conv.zipstate_crosswalk d
                    on substring(a.elgbl_zip_cd, 1, 5) = d.ZipCode and a.elgbl_state_cd = d.StateFIPS

            left join
                dqm_conv.countystate_lookup e
                    on a.elgbl_cnty_cd = e.CountyFIPS and a.elgbl_state_cd = e.StateFIPS

            left join 
                (select distinct StateFIPS from dqm_conv.zipstate_crosswalk) f
                    on a.elgbl_state_cd = f.StateFIPS
                 """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_el12x', z)

    # --------------------------------------------------------------------
    #
    #   700 Series - Pregnancy Codes
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prgncy_codes(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_pcx as
                select
                    concat("'", code, "'") as code,
                    case when Type in ("CPT", "HCPCS LEVEL II") then "prc"
                    when Type = "ICD-10-CM" then "cm"
                    when Type = "ICD-10-PCS" then "pcs"
                    else ' '
                    end as prgcd
                from
                    dqm_conv.prgncy
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_pcx', z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prgncy_codes as
                select
                    a.*,
                    '{dqm.state}' as submtg_state_cd
                from (
                    select
                        collect_set(case when prgcd='cm' then code else null end) as code_cm
                        , collect_set(case when prgcd='pcs' then code else null end) as code_pcs
                        , collect_set(case when prgcd='prc' then code else null end) as code_prc
                    from
                        {dqm.taskprefix}_pcx) as a
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prgncy_codes', z)


    # --------------------------------------------------------------------
    #
    #   700 Series -
    #
    # --------------------------------------------------------------------
    @staticmethod
    def ot_prep_clm_l(dqm: DQMeasures):
        from dqm.DQClosure import DQClosure

        spark = SparkSession.getActiveSession()

        z = f"""
            create or replace temporary view {dqm.taskprefix}_ot_prep_clm_l as
            select
                submtg_state_cd
                ,tmsis_rptg_prd
                ,msis_ident_num
                ,max(case when hcpcs_srvc_cd = '1' then 1 else 0 end) as ot_hcpcs_1
                ,max(case when hcpcs_srvc_cd = '2' then 1 else 0 end) as ot_hcpcs_2
                ,max(case when hcpcs_srvc_cd = '5' then 1 else 0 end) as ot_hcpcs_5
                ,max({DQClosure.parse('case when %nmsng(hcpcs_txnmy_cd,5) then 1 else 0 end')}) as ot_val_hcpcs_txnmy
            from
                {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
            where
                {DQM_Metadata.create_base_clh_view().claim_cat['L']}
                and childless_header_flag = 0
            group by
                submtg_state_cd,
                tmsis_rptg_prd,
                msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_ot_prep_clm_l', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def utl_state_plan(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_utl_state_plan as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when (state_plan_optn_type_cd ='01') then 1 else 0 end) as stplan_denom1
                    ,max(case when (state_plan_optn_type_cd ='02') then 1 else 0 end) as stplan_denom2
                    ,max(case when (state_plan_optn_type_cd ='03') then 1 else 0 end) as stplan_denom3
                from
                    {dqm.taskprefix}_tmsis_state_plan_prtcptn
                group by
                    submtg_state_cd,
                    msis_ident_num
             """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_utl_state_plan', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def utl_wvr(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_utl_wvr as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,max(case when (wvr_type_cd in
                        ('06','07','08','09','10','11','12','13',
                        '14','15','16','17','18','19','20','33'))
                        then 1 else 0 end) as wvr_denom1
                from
                    {dqm.taskprefix}_tmsis_wvr_prtcptn_data
                group by
                    submtg_state_cd,
                    msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_utl_wvr', z)


    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def utl_el_sql(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_stplan_ot_l as
                select
                    a.submtg_state_cd
                    ,a.msis_ident_num
                    ,case when b.submtg_state_cd is not null then 1 else 0 end as ot_clm_l_flag
                    ,b.ot_hcpcs_1
                    ,b.ot_hcpcs_2
                    ,b.ot_hcpcs_5
                    ,b.ot_val_hcpcs_txnmy
                    ,1 as el_stplan_flag
                    ,a.stplan_denom1
                    ,a.stplan_denom2
                    ,a.stplan_denom3

            from
                {dqm.taskprefix}_utl_state_plan a
            left join
                {dqm.taskprefix}_ot_prep_clm_l b

                on
                    a.submtg_state_cd = b.submtg_state_cd and
                    a.msis_ident_num = b.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_stplan_ot_l', z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_stplan_wvr_ot_l as
                select
                    a.*
                    ,case when b.submtg_state_cd is not null then 1 else 0 end as el_wvr_flag
                    ,b.wvr_denom1
                    ,{dqm.m_start} as tmsis_rptg_prd
                from
                    {dqm.taskprefix}_stplan_ot_l a
                left join
                    {dqm.taskprefix}_utl_wvr b
                on
                    a.submtg_state_cd = b.submtg_state_cd and
                    a.msis_ident_num = b.msis_ident_num
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_stplan_wvr_ot_l', z)

        z = f"""
                create or replace temporary view {dqm.taskprefix}_utl_tab as
                select
                    submtg_state_cd
                    ,tmsis_rptg_prd
                    ,msis_ident_num

                    ,stplan_denom1
                    ,stplan_denom2
                    ,stplan_denom3

                    ,case when (stplan_denom1=1 and ot_clm_l_flag =1 )                       then 1 else 0 end as all2_2
                    ,case when (stplan_denom2=1 and ot_clm_l_flag =1 )                       then 1 else 0 end as all2_3
                    ,case when (stplan_denom2=1 and ot_clm_l_flag =1 and ot_hcpcs_1 =1)      then 1 else 0 end as all2_4
                    ,case when (stplan_denom3=1 and ot_clm_l_flag =1)                        then 1 else 0 end as all2_5
                    ,case when (stplan_denom3=1 and ot_clm_l_flag =1 and ot_hcpcs_2 =1)      then 1 else 0 end as all2_6

                    ,case when (el_stplan_flag=1 and wvr_denom1 =1)                          then 1 else 0 end as denom_all2_7
                    ,case when (el_stplan_flag=1 and wvr_denom1 =1 and ot_hcpcs_5 =1)        then 1 else 0 end as all2_7
                    ,case when (el_stplan_flag=1 and (wvr_denom1 =1 or stplan_denom2=1))     then 1 else 0 end as denom_all2_8
                    ,case when (el_stplan_flag=1 and (wvr_denom1 =1 or stplan_denom2=1)
                                and ot_val_hcpcs_txnmy =1) then 1 else 0 end as all2_8
                from
                    {dqm.taskprefix}_stplan_wvr_ot_l
             """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_utl_tab', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def get_prov_ever_enrld_sql(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_ever_enrld as
                select distinct
                    submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id,
                    prvdr_mdcd_efctv_dt,
                    prvdr_mdcd_end_dt
                from {dqm.taskprefix}_ever_tmsis_prvdr_mdcd_enrlmt
                where ever_enrolled_provider =1 and
                    lpad(prvdr_mdcd_enrlmt_stus_cd,2,'0') in ('01', '02', '03', '04', '05', '06')
             """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_utl_tab', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def ot_hdr_clm_ab(dqm: DQMeasures):


        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_ot_hdr_clm_ab as
                select
                    /*unique keys and other identifiers*/
                    a.submtg_state_cd
                    ,msis_ident_num  /**keep msis id to link to el files*/
                    ,tmsis_rptg_prd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,srvc_bgnng_dt
                    ,pgm_type_cd
                    ,wvr_id
                    ,case when (pgm_type_cd not in ('02') or pgm_type_cd is null) then 1 else 0 end as all13_1_orig
                    ,case when array_contains(code_cm,dgns_1_cd)
                            or array_contains(code_cm,dgns_2_cd)
                        then 1 else 0 end as prgncy_dx

                from {DQMeasures.getBaseTable(dqm, 'clh', 'ot')} a
                left join {dqm.taskprefix}_prgncy_codes b
                on a.submtg_state_cd = b.submtg_state_cd
                where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_ot_hdr_clm_ab', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def ot_prep_clm2_ab(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
            create or replace temporary view {dqm.taskprefix}_ot_prep_clm2_ab as
            select

                /*unique keys and other identifiers*/
                 submtg_state_cd
                ,tmsis_rptg_prd
                ,msis_ident_num
                ,orgnl_clm_num
                ,adjstmt_clm_num
                ,adjdctn_dt
                ,orgnl_line_num
                ,adjstmt_line_num
                ,line_adjstmt_ind
                ,max(case when hcpcs_srvc_cd = '4' then 1 else 0 end) as hcpcs_srvc_cd_eq_4

            from {DQMeasures.getBaseTable(dqm, 'cll', 'ot')}
            where {DQM_Metadata.create_base_clh_view().claim_cat['AB']}
            and childless_header_flag = 0
            group by submtg_state_cd, tmsis_rptg_prd, msis_ident_num, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                     orgnl_line_num ,adjstmt_line_num, line_adjstmt_ind
        """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_ot_prep_clm2_ab', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def msng_cll_recs(dqm: DQMeasures, claim_type):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_msng_cll_recs_{claim_type} as
                select
                    tmsis_run_id
                    ,tmsis_rptg_prd
                    ,submtg_state_cd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,orgnl_line_num
                    ,adjstmt_line_num
                    ,line_adjstmt_ind
                    ,count_cll_key
                    ,case when line_adjdctn_dt_orig is null then 1 else 0 end as msng_lne_adjdctn_dt

                from {dqm.taskprefix}_dup_cll_{claim_type}
                /*KH note: childless headers already excluded from dup_cll file*/
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_msng_cll_recs_' + claim_type, z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def msng_clh_recs(dqm: DQMeasures, claim_type):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_msng_clh_recs_{claim_type} as
                select
                    tmsis_run_id
                    ,tmsis_rptg_prd
                    ,submtg_state_cd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    ,adjstmt_ind
                    ,count_claims_key
                    ,case when adjdctn_dt_orig is null then 1 else 0 end as msng_hdr_adjdctn_dt
                from {dqm.taskprefix}_dup_clh_{claim_type}
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_msng_clh_recs_' + claim_type, z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def utl_1915_wvr(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_utl_1915_wvr as
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,wvr_id
                from {dqm.taskprefix}_tmsis_wvr_prtcptn_data
                where wvr_type_cd in ('06','07','08','09','10','11','12','13',
                                      '14','15','16','17','18','19','20','33')
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_utl_1915_wvr', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def get_prov_id_sql(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_prep as
                select distinct
                    submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id
                    from {dqm.taskprefix}_ever_tmsis_prvdr_attr_mn
                    where submtg_state_prvdr_id is not null
                union
                select distinct
                    submtg_state_cd,
                    prvdr_id
                    from {dqm.taskprefix}_ever_tmsis_prvdr_id
                    where prvdr_id is not null
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prvdr_prep', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def providers_w_location(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_loc_prep as
                select distinct
                    submtg_state_cd,
                    submtg_state_prvdr_id as prvdr_id,
                    prvdr_lctn_id
                from {dqm.taskprefix}_tmsis_prvdr_lctn_cntct
                where prvdr_adr_type_cd in ('3','4')
                    and {DQClosure.parse('%nmsng(submtg_state_prvdr_id,30)')}
                    and {DQClosure.parse('%nmsng(prvdr_lctn_id,5)')}
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prvdr_loc_prep', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def all_clms_prvdrs(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_all_clms_prvdrs as
                select
                        submtg_state_cd,
                        blg_prvdr_num,
                        prvdr_lctn_id,
                        'ip' as sourcefile
                from {DQMeasures.getBaseTable(dqm, 'clh', 'ip')}
                where {DQClosure.parse('%nmsng(blg_prvdr_num,30)')}
                    and {DQClosure.parse('%nmsng(prvdr_lctn_id,5)')}
                union
                select
                        submtg_state_cd,
                        blg_prvdr_num,
                        prvdr_lctn_id,
                        'lt' as sourcefile
                from {DQMeasures.getBaseTable(dqm, 'clh', 'lt')}
                where {DQClosure.parse('%nmsng(blg_prvdr_num,30)')}
                    and {DQClosure.parse('%nmsng(prvdr_lctn_id,5)')}
                union
                select
                        submtg_state_cd,
                        blg_prvdr_num,
                        prvdr_lctn_id,
                        'ot' as sourcefile
                from {DQMeasures.getBaseTable(dqm, 'clh', 'ot')}
                where {DQClosure.parse('%nmsng(blg_prvdr_num,30)')}
                    and {DQClosure.parse('%nmsng(prvdr_lctn_id,5)')}
                union
                select
                        submtg_state_cd,
                        blg_prvdr_num,
                        prvdr_lctn_id,
                        'rx' as sourcefile
                from {DQMeasures.getBaseTable(dqm, 'clh', 'rx')}
                where {DQClosure.parse('%nmsng(blg_prvdr_num,30)')}
                    and {DQClosure.parse('%nmsng(prvdr_lctn_id,5)')}
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_all_clms_prvdrs', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def uniq_clms_prvdrs_file(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_uniq_clms_prvdrs_file as
                select distinct
                    submtg_state_cd
                    ,blg_prvdr_num
                    ,prvdr_lctn_id
                    ,sourcefile
                from {dqm.taskprefix}_all_clms_prvdrs
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_uniq_clms_prvdrs_file', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def uniq_clms_prvdrs(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_uniq_clms_prvdrs as
                select distinct
                    submtg_state_cd
                    ,blg_prvdr_num
                    ,prvdr_lctn_id

                from {dqm.taskprefix}_all_clms_prvdrs
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_uniq_clms_prvdrs', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prv_clm(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prv_clm as
                select
                    a.submtg_state_cd
                    ,a.prvdr_id
                    ,sum(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as tot_match_loc_cnt
                    ,count(a.prvdr_lctn_id) as tot_loc_cnt
                    ,avg(case when (a.prvdr_lctn_id = b.prvdr_lctn_id) then 1 else 0 end) as pct_match_loc
                from {dqm.taskprefix}_prvdr_prep as a
                left join {dqm.taskprefix}_uniq_clms_prvdrs as b
                on  a.submtg_state_cd = b.submtg_state_cd and
                    a.prvdr_id = b.blg_prvdr_num and
                    a.prvdr_lctn_id = b.prvdr_lctn_id
                group by a.submtg_state_cd, a.prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prv_clm', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prvdr_txnmy(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_txnmy as
                select distinct submtg_state_cd,
                        submtg_state_prvdr_id ,
                        prvdr_clsfctn_type_cd
                from {dqm.taskprefix}_tmsis_prvdr_txnmy_clsfctn
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prvdr_txnmy', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prvdr_freq_t(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_freq_t as
                select
                    submtg_state_cd,
                    submtg_state_prvdr_id,
                    prvdr_clsfctn_type_cd,
                    case when (prvdr_clsfctn_type_cd in ('1','2','3','4')) then 1 else 0 end as prvdr_clsfctn_any,
                    case when ((prvdr_clsfctn_type_cd not in ('1','2','3','4')) or
                            (prvdr_clsfctn_type_cd is null)
                            ) then 1 else 0 end as prvdr_clsfctn_none
                from {dqm.taskprefix}_prvdr_txnmy
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prvdr_freq_t', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prvdr_freq_t2(dqm: DQMeasures):

        spark = SparkSession.getActiveSession()

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prvdr_freq_t2 as
                select
                    submtg_state_cd,
                    submtg_state_prvdr_id,
                    max(prvdr_clsfctn_any) as prvdr_clsfctn_any,
                    max(prvdr_clsfctn_none) as prvdr_clsfctn_none
                from {dqm.taskprefix}_prvdr_freq_t
                group by submtg_state_cd,
                    submtg_state_prvdr_id
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prvdr_freq_t2', z)

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def prep_clm(dqm: DQMeasures, claim_type, level, clmcat):

        spark = SparkSession.getActiveSession()

        if level.lower() == "cll":
            header_filter = """and childless_header_flag = 0"""
            select_by_level = """
                ,orgnl_line_num
                ,adjstmt_line_num
                ,line_adjstmt_ind"""
            group_by_level = """
                orgnl_line_num,
                adjstmt_line_num,
                line_adjstmt_ind"""
        else:
            header_filter = ""
            select_by_level = ",adjstmt_ind"
            group_by_level = "adjstmt_ind"

        z = f"""
                create or replace temporary view {dqm.taskprefix}_prep_clm_{claim_type}_{level}_{clmcat} as
                select
                    submtg_state_cd
                    ,orgnl_clm_num
                    ,adjstmt_clm_num
                    ,adjdctn_dt
                    {select_by_level}
                    ,max(case when (othr_insrnc_ind = '1') then 1 else 0 end) as othr_ins
                    ,max(case when (othr_tpl_clctn_cd in ('001', '002', '003','004', '005', '006','007'))
                                                    then 1 else 0 end) as othr_tpl

                from {DQMeasures.getBaseTable(dqm, level, claim_type)}
                where
                    {DQM_Metadata.create_base_clh_view.claim_cat[clmcat]}
                    {header_filter}
                group by
                    submtg_state_cd, orgnl_clm_num, adjstmt_clm_num, adjdctn_dt,
                    {group_by_level}
            """

        dqm.logger.debug(z)
        spark.sql(z)
        DQPrepETL.log(dqm, dqm.taskprefix + '_prep_clm_' + claim_type + '_' + level + '_' + clmcat, z)


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