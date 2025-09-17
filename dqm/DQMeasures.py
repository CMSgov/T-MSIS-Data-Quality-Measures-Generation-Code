from dqm.DQM_Metadata import DQM_Metadata

import logging
import datetime

from pyspark.sql import SparkSession


class DQMeasures:

    OPTION_MEDICAID_ONLY = 1
    OPTION_CHIP_ONLY = 2

    @staticmethod
    def MEDICAID_ONLY():
        return DQMeasures.OPTION_MEDICAID_ONLY

    @staticmethod
    def CHIP_ONLY():
        return DQMeasures.CHIP_ONLY

    PERFORMANCE = 15

    # --------------------------------------------------------------------
    #
    #   Capture the most recent run ID to use for a given state.
    #
    # --------------------------------------------------------------------
    def create_runid_view(self, run_id):

        spark = SparkSession.getActiveSession()

        valid_run_id = 0
        if run_id is None:

            z = f"""
                    create or replace temporary view runid as
                    select
                        max(tmsis_run_id) as run_id,
                        submtg_state_cd
                    from
                        {self.tmsis_input_schema}.tmsis_fhdr_rec_ip
                    where
                        submtg_state_cd = '{self.state}'

                    group by
                        submtg_state_cd
                    order by
                        submtg_state_cd;
                """

            valid_run_id = 1

        elif run_id.isnumeric():

            z = f"""
                    create or replace temporary view runid as
                    select distinct
                        tmsis_run_id as run_id,
                        submtg_state_cd
                    from
                        {self.tmsis_input_schema}.tmsis_fhdr_rec_ip
                    where
                        submtg_state_cd = '{self.state}'
                        and tmsis_run_id = {int(run_id)}
                """

            valid_run_id = 1

        else:
            self.run_id = 0

        if valid_run_id == 1:
            spark.sql(z)
            df = spark.sql('select run_id from runid').toPandas()
            if (len(df) > 0):
                self.run_id = df['run_id'].values[0]
            else:
                self.run_id = 0
        else:
            self.run_id = 0

    # --------------------------------------------------------------------
    #
    #   Check if data is there for all 9 file types 
    #
    # --------------------------------------------------------------------
    def chk_fl_exists(self):

        spark = SparkSession.getActiveSession()
        
        file_type=['elgblty','ip','lt','othr_toc','rx','ftx','prvdr','mc','tpl_data']

        # exclude FTX check for GU
        if self.state == '66':
            file_type.remove('ftx')

        for fl in file_type:
            z = f"""
                        create or replace temporary view runid_{fl} as
                        select
                            max(tmsis_run_id) as run_id,
                            submtg_state_cd
                        from
                            {self.tmsis_input_schema}.tmsis_fhdr_rec_{fl}
                        where
                            submtg_state_cd = '{self.state}'
                           and tmsis_run_id = {str(self.run_id)}

                        group by
                            submtg_state_cd
                        order by
                            submtg_state_cd;
                    """
           
            spark.sql(z)
            df = spark.sql(f"select run_id from runid_{fl}").toPandas()
            if (len(df) > 0) == 0:
                raise Exception(f"File missing for {fl}, state={self.state}, tmsis_run_id = {self.run_id}")
            else:
                print(f"File exists for {fl}, state={self.state}, tmsis_run_id = {self.run_id}")
    # --------------------------------------------------------------------
    #
    #   Instantiate a DQ Measures object
    #
    # --------------------------------------------------------------------
    def __init__(self, report_month: str, rpt_state='', separate_entity='0', run_id:str=None, turbo=True):
        from datetime import date, datetime, timedelta
        import pandas as pd
        import json

        self.now = datetime.now()
        self.initialize_logger(self.now)

        self.version = '4.00.2'
        self.progpath = '/dqm'

        self.specvrsn = 'V4.00.2'
        #This definition is now specific to PROD/STATEPROD/VAL and moved down. Please see line 225
        #self.turboDB = 'dqm_conv'
        self.isTurbo = turbo

        self.run_rules = {}
        self.run_id = 0

        self.limit = ''

        self.report_month = report_month

        self.reverse_measure_lookup = self.loadReverseMeasureLookup()

        # static metadata dataframes
        self.abd = self.load_metadata_file('abd')
        self.abd['Medicaid Determination'] = self.abd['Medicaid Determination'].str.strip()
        self.abd['Medicaid Determination'] = self.abd['Medicaid Determination'].str.upper()
        self.apdxc = self.load_metadata_file('apdxc')
        self.apdxc['Variable'] = self.apdxc['Variable'].str.lower()
        self.countystate_lookup = self.load_metadata_file('countystate_lookup')
        self.fmg = self.load_metadata_file('fmg')
        self.prgncy = self.load_metadata_file('prgncy')
        self.prgncy['Code'] = self.prgncy['Code'].str.strip()
        self.prgncy['Code'] = self.prgncy['Code'].str.upper()
        self.prgncy['Type'] = self.prgncy['Type'].str.strip()
        self.prgncy['Type'] = self.prgncy['Type'].str.upper()
        self.prvtxnmy = self.load_metadata_file('prvtxnmy')
        self.sauths = self.load_metadata_file('sauths')
        self.schip = self.load_metadata_file('schip')
        self.splans = self.load_metadata_file('splans')
        self.st_fips = self.load_metadata_file('st_fips')
        self.st_name = self.load_metadata_file('st_name')
        self.st_usps = self.load_metadata_file('st_usps')
        self.st2_name = self.load_metadata_file('st2_name')
        self.stabr = self.load_metadata_file('stabr')
        self.stc_cd = self.load_metadata_file('stc_cd')
        self.stc_cd['z_tos'] = self.stc_cd['TypeOfService'].map('{:03d}'.format)

        # Provider Classification Lookup
        self.provider_classification_lookup = self.load_metadata_file('provider_classification_lookup')
        
        # Atypical Provider Classification Lookup
        self.atypical_provider_table = self.load_metadata_file('atypical_provider_table')
        
        self.thresholds = self.load_metadata_file('thresholds')
        self.zcc = self.load_metadata_file('zcc')
        self.zipstate_lookup = self.load_metadata_file('zipstate_lookup')

        # define_state macro
        self.rpt_state = rpt_state
        self.stabbrev = rpt_state
        self.state = str(self.st_fips[self.st_fips['STABBREV'] == self.stabbrev].squeeze('rows').get('FIPS'))
        self.chipstate = None
        if self.state.find(',') > 0:
            self.chipstate = self.state.split(',', 2)[1]
            self.state = self.state.split(',', 2)[0]

        self.stname = self.schip[self.schip['STABBREV'] == self.stabbrev].squeeze('rows').get('STATE_NAME')
        self.has_schip = self.schip[self.schip['STABBREV'] == self.stabbrev].squeeze('rows').get('HAS_SCHIP')

        # sometimes separate reports needed for Medicaid and CHIP
        self.separate_entity = separate_entity
        if self.separate_entity == '1':  # Medicaid only
            self.typerun = 'M'
            self.chipstate = None
        elif self.separate_entity == '2':  # CHIP only
            self.typerun = 'C'
            self.state = self.chipstate
            self.chipstate = None
        else:
            self.typerun = ''

        # T-MSIS input schema name
        #This definition is now specific to PROD/STATEPROD/VAL and moved down. Please see line 225
        #self.tmsis_input_schema = 'state_prod_catalog.tmsis'
        #self.tmsis_input_schema = 'tmsis'

         # S3 output bucket
        spark = SparkSession.getActiveSession()

        all_tags = {}

        for tag in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")):
            all_tags[tag['key']] = tag['value']

        stack = all_tags.get('stack')

        self.s3proto = 's3a://'

        if stack.casefold() == 'val':
            self.s3bucket = 'macbis-dw-dqm-val'
            self.tmsis_input_schema = 'dc_prod_data_sources_catalog.tmsis'
            self.turboDB = 'dc_val_working_catalog.dqm_conv'

        elif stack.casefold() == 'stateprod':
            self.s3bucket = 'macbis-dw-dqm-prod'
            self.tmsis_input_schema = 'state_prod_catalog.tmsis'
            self.turboDB = 'state_prod_catalog.dqm_conv'

        elif stack.casefold() == 'prod':
            self.s3bucket = 'macbis-dw-dqm-prod'
            self.tmsis_input_schema = 'dc_prod_data_sources_catalog.tmsis'
            self.turboDB = 'dc_prod_working_catalog.dqm_conv'

        else:
            self.logger.error('Cluster tags do not have a key-value for `stack`')

        # Run ID
        self.create_runid_view(run_id)
        if self.run_id == 0:
            raise Exception('Invalid Run Id')

        self.z_run_id = '{:05d}'.format(self.run_id)

        self.specific_run_id = self.z_run_id

        # Check if file exists
        self.chk_fl_exists()

        self.logfile = None

        self.sql = {}

        self.actual_time = self.now.strftime('%d%b%Y:%H:%M:%S').upper()  # ddmmmyy:hh:mm:ss

        # restart status indicator
        self.sqlxrc = 0  # if ne 0, restart at 'start_at' = module number (e.g. 901)
        self.start_at = 0

        # missing-ness run flag
        self.msng_dq_run_flag = 1  # if 1, run module 800

        # query result limit threshold
        self.limit = ''

        self.pgmstart_date = self.now.strftime('%y%m%d').upper()  # 210201 yymmdd6.
        self.pgmstart_time = self.now.strftime('%H:%M')  # 14:20 tod5.
        self.pgmstart = self.now.strftime('%y%m%d%H%M')
        self.month_added = self.now.strftime('%Y%m').upper()

        # self.rpt_month = date.today().replace(day=1)
        self.rpt_month = datetime.strptime(self.report_month, '%Y%m')
        self.rpt_month_c = self.rpt_month.strftime('%d%b%Y')  # rpt_month_c = '21FEB2021'
        self.date9 = self.now.strftime('%d%b%y').upper()  # date9 = '09FEB21'

        # char variables
        self.rpt_month = datetime.strptime(self.rpt_month_c, '%d%b%Y')  # 01FEB2021
        self.rpt_month_name = self.rpt_month.strftime('%b%Y').upper()  # FEB2021 monyy7.
        self.rpt_fldr_name = self.rpt_month.strftime('%Y%m').upper()  # 202102 yymmn7.
        self.statistic_year_month = self.rpt_month.strftime('%m/%d/%Y') # 05/01/2021

        # reporting month
        self.m_start = self.rpt_month.strftime('%Y-%m-%d')  # 2021-02-01 yymmdd10.
        if self.rpt_month.month == 12:  # December
            last_day_selected_month = date(self.rpt_month.year, self.rpt_month.month, 31)
        else:
            last_day_selected_month = date(self.rpt_month.year, self.rpt_month.month + 1, 1) - timedelta(days=1)
        self.m_end = last_day_selected_month.strftime('%Y-%m-%d')  # 2021-02-28 yymmdd10.
        self.m_label = self.rpt_month.strftime('%B') + ' ' + self.rpt_month.strftime('%Y')  # February 2021
        self.m_col = 'month01' + self.rpt_month.strftime('%b%Y')  # FEB2021 monyy7.

        # prior month (for index of dissimilarity)
        if self.rpt_month.month == 1:
            self.prior_m_start = date(self.rpt_month.year - 1, 12, 1)
        else:
            self.prior_m_start = date(self.rpt_month.year, self.rpt_month.month - 1, 1)
        if self.prior_m_start.month == 12:
            self.prior_m_end = date(self.prior_m_start.year, self.prior_m_start.month, 31)
        else:
            self.prior_m_end = date(self.prior_m_start.year, self.prior_m_start.month + 1, 1) - timedelta(days=1)
        self.prior_m_start = self.prior_m_start.strftime('%Y-%m-%d')
        self.prior_m_end = self.prior_m_end.strftime('%Y-%m-%d')

        # --------------------------------------------------
        self.rpt_state = self.stabbrev
        self.rpt_month = self.rpt_month_name
        self.rpt_fldr = self.rpt_fldr_name
        # --------------------------------------------------

        # AREMAC database names
        self.permview = 'macbis_t12_perm'
        self.temptable = 'macbis_t12_temp_' + self.stabbrev.lower() + self.typerun.lower()
        self.wrktable = 'macbis_t12_wrk_' + self.stabbrev .lower() + self.typerun.lower()
        self.taskprefix = self.stabbrev.lower() + self.typerun.lower() + self.rpt_fldr + '_' + self.pgmstart

        self.taskprep = self.stabbrev.lower() + self.typerun.lower() + self.rpt_fldr     
     

        # path to root folder: STATE + Reporting MONTH + Run ID
        # if ((self.separate_entity == '1') or (self.separate_entity == '2')):
        #     self.s3folder = 'sas-dqm/DQ_Output/' + self.rpt_state + '-' + self.typerun + '/' + self.rpt_fldr + '/' + self.z_run_id
        # else:
        #     self.s3folder = 'sas-dqm/DQ_Output/' + self.rpt_state + '/' + self.rpt_fldr + '/' + self.z_run_id


        if stack.casefold() == 'stateprod':
            if ((self.separate_entity == '1') or (self.separate_entity == '2')):
                self.s3folder = 'state-prod/sas-dqm/DQ_Output/' + self.rpt_state + '-' + self.typerun + '/' + self.rpt_fldr + '/' + self.z_run_id
            else:
                self.s3folder = 'state-prod/sas-dqm/DQ_Output/' + self.rpt_state + '/' + self.rpt_fldr + '/' + self.z_run_id
        else:
            if ((self.separate_entity == '1') or (self.separate_entity == '2')):
                self.s3folder = 'sas-dqm/DQ_Output/' + self.rpt_state + '-' + self.typerun + '/' + self.rpt_fldr + '/' + self.z_run_id
            else:
                self.s3folder = 'sas-dqm/DQ_Output/' + self.rpt_state + '/' + self.rpt_fldr + '/' + self.z_run_id

        # path to write Excel report files
        self.s3xlsx = self.s3proto + self.s3bucket + '/' + self.s3folder

        # path to write Spark Dataframes when running measures
        self.s3path = self.s3proto + self.s3bucket + '/' + self.s3folder + '/' + self.pgmstart

        if self.typerun != '':
            self.logfile = './MACBIS_DQ_control_' + self.stabbrev.lower() + '_' + self.typerun.lower() + '_' + self.rpt_month.lower() + '_run' + str(self.run_id) + '.log'
            self.lstfile = './MACBIS_DQ_control_' + self.stabbrev.lower() + '_' + self.typerun.lower() + '_' + self.rpt_month.lower() + '_run' + str(self.run_id) + '.lst'
        else:
            self.logfile = './MACBIS_DQ_control_' + self.stabbrev.lower() + '_' + self.rpt_month.lower() + '_run' + str(self.run_id) + '.log'
            self.lstfile = './MACBIS_DQ_control_' + self.stabbrev.lower() + '_' + self.rpt_month.lower() + '_run' + str(self.run_id) + '.lst'

        self.logger.info('Reporting Month: ' + self.m_label)
        self.logger.info('State Abbreviation: ' + self.stabbrev)

        if self.typerun == 'M':
            self.logger.info('Medicaid Only')
        elif self.typerun == 'C':
            self.logger.info('CHIP Only')

        self.load_report()

        spark.conf.set('dqm.taskprefix', self.taskprefix)

        spark.conf.set('dqm.taskprep', self.taskprep)

        spark.conf.set('dqm.turboDB', self.turboDB)

        # eligibility state groups
        state = pd.merge(self.stabr, self.fmg, left_on='STNAME', right_on='State')
        state['FIPS'] = state['STABBREV'].map(DQM_Metadata.FIPS.stfips)

        # group 72
        sFIPS = state[state['FIPS'].notnull() & (state['EL Group 72'] == 'YES')]['FIPS']
        self.el_grp_72 = ','.join(f"'{l}'" for l in sFIPS.tolist())

        # group 73, 74, and 75
        sFIPS = state[state['FIPS'].notnull() & (state['EL Group 73, 74, or 75'] == 'YES')]['FIPS']
        self.el_grp_73_74_75 = ','.join(f"'{l}'" for l in sFIPS.tolist())

        # eligibility medicaid determination
        state = pd.merge(self.stabr, self.abd, left_on='STNAME', right_on='State')
        state['FIPS'] = state['STABBREV'].map(DQM_Metadata.FIPS.stfips)

        # el335
        sFIPS = state[state['FIPS'].notnull() & ((state['Medicaid Determination'] == '209B') | (state['Medicaid Determination'] == 'N/A'))]['FIPS']
        self.medicaid_el335 = ','.join(f"'{l}'" for l in sFIPS.tolist())

        # el336
        sFIPS = state[state['FIPS'].notnull() & \
            ((state['Medicaid Determination'] == '1634') | (state['Medicaid Determination'] == 'SSI') | \
             (state['Medicaid Determination'] == 'N/A'))]['FIPS']
        self.medicaid_el336 = ','.join(f"'{l}'" for l in sFIPS.tolist())

    # --------------------------------------------------------------------
    #
    #   References to S3 buckets and folders
    #
    # --------------------------------------------------------------------
    def setS3Bucket(self, s3bucket):

        self.s3proto = 's3a://'
        self.s3bucket = s3bucket

        # path to root folder: STATE + Reporting MONTH + Run ID
        self.s3folder = 'sas-dqm/DQ_Output/' + self.rpt_state + '/' + self.rpt_fldr + '/' + self.z_run_id

        # path to write Excel report files
        self.s3xlsx = self.s3proto + self.s3bucket + '/' + self.s3folder

        # path to write Spark Dataframes when running measures
        self.s3path = self.s3proto + self.s3bucket + '/' + self.s3folder + '/' + self.pgmstart

    # --------------------------------------------------------------------
    #
    #   Logging Facility
    #
    # --------------------------------------------------------------------
    def initialize_logger(self, now: datetime):

        logging.addLevelName(DQMeasures.PERFORMANCE, 'PERFORMANCE')

        def performance(self, message, *args, **kws):
            self.log(DQMeasures.PERFORMANCE, message, *args, **kws)

        logging.Logger.performance = performance

        self.logger = logging.getLogger('dqm_log')
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(ch)

    # --------------------------------------------------------------------
    #
    #   Prepare Data View contentualized to this run
    #
    # --------------------------------------------------------------------
    def init(self):
        self.prepare_base_views_and_tables()

    # --------------------------------------------------------------------
    #
    #   delete claims Data 
    #
    # --------------------------------------------------------------------
    def drop_clms(self):
        from dqm import DQPrepETL as etl

        etl.DQPrepETL.drop_views(self)
    # --------------------------------------------------------------------
    #
    #   Display runtime options
    #
    # --------------------------------------------------------------------
    def print(self):
        print('Version:\t' + self.version)
        print('Thresholds:\t' + self.specvrsn)
        print('-----------------------------------------------------')
        print(' Global Variables')
        print('-----------------------------------------------------')
        print('run_id:\t\t' + str(self.run_id))
        print('z_run_id:\t' + self.z_run_id)
        print('rpt_state:\t' + self.rpt_state)
        print('separate_entity:' + str(self.separate_entity))
        print('stabbrev:\t' + self.stabbrev)
        print('state:\t\t' + self.state)
        print('chipstate:\t' + str(self.chipstate))
        print('stname:\t\t' + self.stname)
        print('has_schip:\t' + self.has_schip)

        # --------------------------------------------------
        print('pgmstart_date:\t' + str(self.pgmstart_date))
        print('pgmstart_time:\t' + str(self.pgmstart_time))
        print('pgmstart:\t' + str(self.pgmstart))

        print('-----------------------------------------------------')
        print(' Output directory')
        print('-----------------------------------------------------')
        print('txtout:\t' + self.s3path)
        print('logout:\t' + self.s3path)

        print('-----------------------------------------------------')
        print(' T-MSIS input schema name')
        print('-----------------------------------------------------')
        print('tmsis_input_schema:\t' + str(self.tmsis_input_schema))

        print('-----------------------------------------------------')
        print(' AREMAC database names')
        print('-----------------------------------------------------')
        print('permview:\t' + str(self.permview))
        print('temptable:\t' + str(self.temptable))
        print('wrktable:\t' + str(self.wrktable))
        print('taskprefix:\t' + str(self.taskprefix))

        print('-----------------------------------------------------')
        print(' Initialize Dates')
        print('-----------------------------------------------------')
        print('rpt_month_name:\t' + str(self.rpt_month_name))
        print('rpt_month:\t' + str(self.rpt_month))
        print('rpt_fldr_name:\t' + str(self.rpt_fldr_name))
        print('rpt_fldr:\t' + str(self.rpt_fldr))
        print('m_start:\t' + str(self.m_start))
        print('m_end:\t\t' + str(self.m_end))
        print('m_label:\t' + str(self.m_label))
        print('m_col:\t\t' + str(self.m_col))
        print('prior_m_start:\t' + str(self.prior_m_start))
        print('prior_m_end:\t' + str(self.prior_m_end))

        print('-----------------------------------------------------')
        print(' Print Vars (intended for the log)')
        print('-----------------------------------------------------')
        print('rpt_state = ' + str(self.rpt_state))
        print('rpt_mnth = ' + str(self.rpt_month))
        print('specific_run_id=' + str(self.specific_run_id))

        print('limit = ' + str(self.limit))
        print('separate_entity = ' + str(self.separate_entity))

    # --------------------------------------------------------------------
    #
    #   Load in Pickle metadata files sourced from Excel
    #
    # --------------------------------------------------------------------
    def load_metadata_file(self, fn):
        import pandas as pd
        import os
        pdf = None

        this_dir, this_filename = os.path.split(__file__)
        pkl = os.path.join(this_dir + '/cfg/', fn + '.pkl')

        pdf = pd.read_pickle(pkl)

        return pdf

    # --------------------------------------------------------------------
    #
    #   Load in Pickle metadata files sourced from Excel
    #
    # --------------------------------------------------------------------
    def load_metadata_file_old(self, fn, root_folder):
        import pandas as pd
        import os
        pdf = None

        if root_folder is None:
            pkl = './data/metadata/' + fn + '.pkl'
        else:
            pkl = root_folder + '/data/metadata/' + fn + '.pkl'
        if os.path.isfile(pkl):
            pdf = pd.read_pickle(pkl)
        else:
            raise Exception('Cannot read file ' + pkl)
        return pdf

    # --------------------------------------------------------------------
    #
    #   Auto-create/ensure an output folder exists for each state
    #
    # --------------------------------------------------------------------
    def define_dq_output_folders():
        import pandas as pd
        import os
        stfldr = './DQ_Output'
        os.makedirs(stfldr, exist_ok=True)
        pkl = './data/metadata/st_usps.pkl'
        if os.path.isfile(pkl):
            df = pd.read_pickle(pkl)
            for ind in df.index:
                os.makedirs(stfldr + '/' + df['STABBREV'][ind], exist_ok=True)

    # --------------------------------------------------------------------
    #
    #   Folder for formatted report output
    #
    # --------------------------------------------------------------------
    def define_report_folder(self, rpt_dates):
        print(self.rpt_fldr_name)

    # --------------------------------------------------------------------
    #
    #   Standare contraint - state + run ID
    #
    # --------------------------------------------------------------------
    def run_id_filter(self):
        return 'submtg_state_orig = ' + str(self.state) + ' and tmsis_run_id = ' + str(self.run_id)

    # --------------------------------------------------------------------
    #
    #   Build defined data prep views
    #
    # --------------------------------------------------------------------
    def prepare_base_views_and_tables(self):
        from dqm import DQPrepETL as etl

        etl.DQPrepETL.create_all_base_views(self)
        etl.DQPrepETL.create_tables(self)

        if (self.isTurbo):
            self.materializeViews()

        etl.DQPrepETL.shared_metadata_views(self)
        etl.DQPrepETL.create_peripheral_tables(self)
        etl.DQPrepETL.create_all_missingness_views(self)

    # --------------------------------------------------------------------
    #
    #   Base Data Caching
    #
    # --------------------------------------------------------------------
    def materializeViews(self):
        from dqm import DQPrepETL as etl

        etl.DQPrepETL.materialize_views(self)

    # --------------------------------------------------------------------
    #
    #   Render formatted output report files (Excel)
    #
    # --------------------------------------------------------------------
    def reports(self, results):
        import pandas as pd
        import numpy as np
        from decimal import Decimal

        pd.options.mode.chained_assignment = None
        pd.set_option('mode.chained_assignment', None)

        # --------------------------------------------------------------------------------
        #  results x threshold data x measure library x rounding 101 + global vars
        # --------------------------------------------------------------------------------
        summary = pd.merge(results, self.thresholds, on='measure_id')
        summary = pd.merge(summary, self.loadReverseMeasureLookup(), on='measure_id')

        # type casting and cleaning of rounding factor
        rr = self.getRunRules('101')
        rr = rr[['measure_id', 'round']]
        rr['decimal_places_rr'] = rr['round'].astype(str)
        rr = rr[rr['decimal_places_rr'] != 'nan']
        rr['round'] = rr['round'].fillna(0)
        rr['round'] = rr['round'].astype(np.int64)
        summary = pd.merge(summary, rr, on='measure_id', how='left')

        if ((self.typerun == 'C') or (self.typerun == 'M')):
            summary['Report_State'] = self.rpt_state + '-' + self.typerun
        else:
            summary['Report_State'] = self.rpt_state

        # handle one value measures by moving numer to mvalue
        summary['mvalue'] = summary.apply(lambda x: x['numer'] if x['measure_id'] in DQM_Metadata.Reports.one_value.measure_ids else x['mvalue'], axis=1)

        summary['Month_Added'] = self.month_added
        summary['Measure_ID'] = summary['measure_id_w_display_order'].str.strip()
        summary['Statistic_Year_Month'] = self.statistic_year_month
        summary['SpecVersion'] = self.specvrsn
        summary['RunID'] = self.z_run_id
        summary['Calculation_Source'] = 'Python'
        summary['in_measures'] = 1
        summary['in_thresholds'] = 1
        summary['claim_type'] = summary.apply(lambda x: x['claim_file'] if x['claim_type'] == '' else x['claim_type'], axis=1)
        summary['Measure_Type'] = summary['Measure_Type'].str.strip()

        # ------------------------------------------------------------------------------------
        # Rounding
        # ------------------------------------------------------------------------------------
        def remove_zeros(num):
            return Decimal(num).to_integral() if num == Decimal(num).to_integral() else num

        # override decimal places from thresholds workbook here
        def roundingAdjustments(series, measure_id, decimal_places):

            rounded = DQM_Metadata.Rounding.round4 + \
                      DQM_Metadata.Rounding.round3 + \
                      DQM_Metadata.Rounding.round2 + \
                      DQM_Metadata.Rounding.round1 + \
                      DQM_Metadata.Rounding.round0 + \
                      DQM_Metadata.Rounding.round_noop

            if ((series in ('715', '716', '802', '803')) and (measure_id not in rounded)):
                return 3
            elif ((series in ('103', '201', '901', '902', '906', '909', '910', '911', '912', '913', '914', '915', '916', '917')) and (measure_id not in rounded)):
                return 2
            elif ((series in ('202', '903', '904', '905', '907', '908', '910')) and (measure_id not in rounded)):
                return 1
            elif ((series in ('102', '104', '105', '106', '107', '108', '109', '204', '206')) and (measure_id not in rounded)):
                return 0
            elif (measure_id in DQM_Metadata.Rounding.round4):
                return 4
            elif (measure_id in DQM_Metadata.Rounding.round3):
                return 3
            elif (measure_id in DQM_Metadata.Rounding.round2):
                return 2
            elif (measure_id in DQM_Metadata.Rounding.round1):
                return 1
            elif (measure_id in DQM_Metadata.Rounding.round0):
                return 0
            elif (measure_id in DQM_Metadata.Rounding.round_noop):
                return 14
            else:
                return decimal_places

        # apply overridden decimal places to thresholds data
        summary['decimal_places'] = summary.apply(lambda x: roundingAdjustments(x['series'], x['measure_id'], x['decimal_places']), axis=1)

        # override decimal places from an existing rounding value from rule logic (e.g. 101 run rules)
        summary['decimal_places'] = summary['round'].combine_first(summary['decimal_places'])

        # ffs module logic to handle empty numerator values
        def fill_empty_results_numer(x):
            if ((x['series'].startswith('9')) and (pd.isna(x['mvalue'])) and (pd.isna(x['valid_value'])) and (pd.isna(x['plan_id']))):
                return Decimal(0)
            else:
                return x['numer']

        # ffs module logic to handle empty denominator values
        def fill_empty_results_denom(x):
            if pd.isna(x['denom']) and (x['Measure_Type'] in ('Claims Percentage','Non-Claims Percentage','Duplicate Percentage','Ratio')):
                return Decimal(0)
            elif ((x['series'].startswith('9')) and (pd.isna(x['mvalue'])) and (pd.isna(x['valid_value'])) and (pd.isna(x['plan_id']))):
                return Decimal(0)
            else:
                return x['denom']

        # N/A, Div by 0
        def na_div_by_0(x):
            if ((x['Measure_Type'] in ('Sum', 'Count')) and (pd.isna(x['mvalue']))):
                return '0'
            elif ((x['series'].startswith('5')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('6')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('7')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('9')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('2')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('8')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['series'].startswith('5')) and (pd.isna(x['mvalue'])) and (pd.isna(x['numer'])) and (x['denom'] > 0)):
                return '0'
            elif ((x['measure_id'] in ('EL3_19','EL3_33','EL3_22','EL3_34','EL3_35','EL3_36')) and (pd.isna(x['mvalue'])) and (x['denom'] == 0)):
                return 'N/A'
            elif ((pd.isna(x['mvalue']))):
                return 'Div by 0'
            else:
                return x['fStatistic']

        # N/A, Div by 0 - Custom Reports
        def na_div_by_0_compound(x, colname):
            if (pd.isna(x[colname]) and colname in ('ip_ratio','lt_ratio','ot_ratio','rx_ratio','cap_ratio') and x['enrollment'] == 0):
                return 'Div by 0'
            elif pd.isna(x[colname]):
                return 'div by 0' # 'N/A' (DQM-164)
            else:
                if abs(x[colname]) > 999:
                    return f"{round((float(x[colname]) + float(0.000000001))):.0f}"
                else:
                    return str(remove_zeros(round((float(x[colname]) + float(0.000000001)), int(x['decimal_places']))))

        # safe rounding to string format
        def doRounding(x):
            if (x['mvalue'] is None):
                return None
            elif (abs(x['mvalue']) > 999):
                return str(remove_zeros(round((float(x['mvalue']) + float(0.000000001)), 0)))
            elif (int(x['decimal_places']) == 14):
                return str(remove_zeros(x['mvalue']))
            elif (x['decimal_places'] is not None):
                return str(remove_zeros(round((float(x['mvalue']) + float(0.000000001)), int(x['decimal_places']))))
            else:
                return str(remove_zeros(x['mvalue']))

        def blank_numer(x):
            if x['Measure_Type'] in ('Count','Frequency','Sum'):
                return ''
            elif ((x['Statistic'] == 'Div by 0') and (x['series'] in ('101', '601', '802', '803')) and (x['measure_id'] not in ('EL6_26', 'EL6_27'))):
                return ''
            else:
                return x['Numerator']

        def blank_denom(x):
            if x['Measure_Type'] in ('Count','Frequency','Sum'):
                return ''
            elif x['measure_id'] in ('EL16_5', 'EL16_6') and x['Statistic'] == 'Div by 0':
                return ''
            elif x['Statistic'] == 'Div by 0':
                return '0'
            else:
                return x['Denominator']

        # --------------------------------------------------------------------------------
        # Specialized Reports
        # --------------------------------------------------------------------------------
        # : Waiver ID - EL 7.1
        pd_el7_1 = summary[summary['measure_id'] == 'EL7_1']

        # : Plan ID - EL 8.2
        pd_el8_2 = summary[summary['measure_id'] == 'EL8_2']

        # : Plan ID - EL 9.1
        pd_el9_1 = summary[summary['measure_id'] == 'EL9_1']

        summary = summary[summary['measure_id'] != 'EL7_1']
        summary = summary[summary['measure_id'] != 'EL8_2']
        summary = summary[summary['measure_id'] != 'EL9_1']

        # ------------------------------------------------------------------------------------
        #   Standard Report
        # ------------------------------------------------------------------------------------
        if len(summary.index) > 0:

            # fill in empty values
            # summary['mvalue'] = summary.apply(lambda x: fill_empty_results_mvalue(x), axis=1)
            summary['numer'] = summary.apply(lambda x: fill_empty_results_numer(x), axis=1)
            summary['denom'] = summary.apply(lambda x: fill_empty_results_denom(x), axis=1)

            # Round mvalue by specified decimal_places values
            summary['fStatistic'] = summary.apply(lambda x: doRounding(x), axis=1)

            summary['Statistic'] = summary.apply(lambda x: na_div_by_0(x), axis=1)

            # S-CHIP Test
            def schip_test(x):
                claim_cat = str(x['claim_category'])
                if ((claim_cat[:6] == 'S-CHIP') and (x['Measure_Type'] != 'Frequency')):
                    return 'No S-CHIP Program'
                else:
                    return str(x['Statistic'])

            if self.has_schip == 'No':
                summary['Statistic'] = summary.apply(lambda x: schip_test(x), axis=1)

            # NaN on Numerator/Denominator/Valid Value
            summary = summary.fillna({'valid_value': '', 'plan_id': ''})

            summary['Numerator'] = summary.apply(lambda x:  str(remove_zeros(x['numer'])) if (pd.notnull(x['numer'])) else '', axis=1)
            summary['Denominator'] = summary.apply(lambda x: str(remove_zeros(x['denom'])) if (pd.notnull(x['denom'])) else '', axis=1)

            summary['Numerator'] = summary.apply(lambda x: blank_numer(x), axis=1)
            summary['Denominator'] = summary.apply(lambda x: blank_denom(x), axis=1)

            summary = summary.sort_values(['Measure_ID'], ascending=True)

            if not set(['valid_value']).issubset(summary.columns):
                summary['valid_value'] = ''
            if not set(['plan_id']).issubset(summary.columns):
                summary['plan_id'] = ''

            summary = summary.astype(DQM_Metadata.Reports.summary.types)
            summary = summary[DQM_Metadata.Reports.summary.columns]

            # handle various values representing not a number
            summary['Statistic'] = summary['Statistic'].str.replace('nan', '')
            summary['valid_value'] = summary['valid_value'].str.replace('nan', '')
            summary['plan_id'] = summary['plan_id'].str.replace('nan', '')
            summary['claim_type'] = summary['claim_type'].str.replace('nan', '')

        else:
            summary = None

        # ------------------------------------------------------------------------------------
        #   Waiver  7.1
        # ------------------------------------------------------------------------------------
        if len(pd_el7_1.index) > 0:
            pd_el7_1 = pd_el7_1.sort_values(['Measure_ID'], ascending=True)
            pd_el7_1['submtg_state_cd'] = str(self.state)
            pd_el7_1['statistic_type'] = 'enrollment'
            pd_el7_1['Statistic'] = pd_el7_1['mvalue'].fillna(0, inplace=True)
            pd_el7_1['Statistic'] = pd_el7_1['mvalue'].apply(lambda x: round(x))
            pd_el7_1 = pd_el7_1.astype(DQM_Metadata.Reports.waiver.types)

            pd_el7_1['waiver_id'] = pd_el7_1['waiver_id'].str.replace('nan','.')
            pd_el7_1['waiver_type'] = pd_el7_1['waiver_type'].str.replace('nan','.')

            pd_el7_1 = pd_el7_1[DQM_Metadata.Reports.waiver.columns]
        else:
            pd_el7_1 = None

        # ------------------------------------------------------------------------------------
        #   Plan 8.2
        # ------------------------------------------------------------------------------------
        if len(pd_el8_2.index) > 0:

            pd_el8_2['decimal_places'] = 2

            pd_el8_2['enrollment'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'enrollment'), axis=1)

            pd_el8_2['ip_ratio'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'ip_ratio'), axis=1)
            pd_el8_2['lt_ratio'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'lt_ratio'), axis=1)
            pd_el8_2['ot_ratio'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'ot_ratio'), axis=1)
            pd_el8_2['rx_ratio'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'rx_ratio'), axis=1)
            pd_el8_2['cap_ratio'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_ratio'), axis=1)

            pd_el8_2['cap_hmo'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_hmo'), axis=1)
            pd_el8_2['cap_php'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_php'), axis=1)
            pd_el8_2['cap_pccm'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_pccm'), axis=1)
            pd_el8_2['cap_phi'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_phi'), axis=1)
            pd_el8_2['cap_oth'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_oth'), axis=1)
            pd_el8_2['cap_tot'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'cap_tot'), axis=1)

            pd_el8_2['enc_ip'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'enc_ip'), axis=1)
            pd_el8_2['enc_lt'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'enc_lt'), axis=1)
            pd_el8_2['enc_ot'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'enc_ot'), axis=1)
            pd_el8_2['enc_rx'] = pd_el8_2.apply(lambda x: na_div_by_0_compound(x,'enc_rx'), axis=1)

            pd_el8_2 = pd_el8_2.melt(
                id_vars = DQM_Metadata.Reports.plan8_2.id_vars + ['decimal_places'],
                value_vars = DQM_Metadata.Reports.plan8_2.value_vars)
            pd_el8_2 = pd_el8_2.sort_values(['Measure_ID'], ascending=True)

            pd_el8_2['statistic_type'] = pd_el8_2.apply(lambda x: DQM_Metadata.Reports.plan8_2.statistic_type_formats[x['variable']], axis=1)
            pd_el8_2['Statistic'] = pd_el8_2['value']

            pd_el8_2['In_MCR_File'] = pd_el8_2['linked']

            pd_el8_2['Measure_Type'] = 'Data Profile'
            pd_el8_2['Active_Ind'] = 'Y'
            pd_el8_2['Display_Type'] = 'EL8.2'
            pd_el8_2['Calculation_Source'] = 'Python'
            pd_el8_2['in_measures'] = 1
            pd_el8_2['in_thresholds'] = 1

            pd_el8_2['MultiplePlanTypes_el'] = pd_el8_2.apply(lambda x: f"{x['MultiplePlanTypes_el']:,.0f}" if (pd.notnull(x['MultiplePlanTypes_el'])) else '', axis=1)
            pd_el8_2['MultiplePlanTypes_mc'] = pd_el8_2.apply(lambda x: f"{x['MultiplePlanTypes_mc']:,.0f}" if (pd.notnull(x['MultiplePlanTypes_el'])) else '', axis=1)

            pd_el8_2 = pd_el8_2.astype(DQM_Metadata.Reports.plan8_2.types)

            pd_el8_2['plan_id'] = pd_el8_2['plan_id'].str.replace('nan','.')
            pd_el8_2 = pd_el8_2[pd_el8_2['plan_id'] != '.']
            pd_el8_2 = pd_el8_2[pd_el8_2['plan_id'] != '']

            pd_el8_2['plan_type_el'] = pd_el8_2['plan_type_el'].str.replace('nan','')
            pd_el8_2['plan_type_mc'] = pd_el8_2['plan_type_mc'].str.replace('nan','')

            pd_el8_2['MultiplePlanTypes_el'] = pd_el8_2['MultiplePlanTypes_el'].str.replace('nan','')
            pd_el8_2['MultiplePlanTypes_mc'] = pd_el8_2['MultiplePlanTypes_mc'].str.replace('nan','')

            pd_el8_2 = pd_el8_2[DQM_Metadata.Reports.plan8_2.columns]
        else:
            pd_el8_2 = None

        # ------------------------------------------------------------------------------------
        #   Plan 9.1
        # ------------------------------------------------------------------------------------
        if len(pd_el9_1.index) > 0:
            pd_el9_1 = pd_el9_1.sort_values(['Measure_ID'], ascending=True)
            pd_el9_1['statistic_type'] = 'Enrollment'

            pd_el9_1['mvalue'] = pd_el9_1.apply(lambda x: remove_zeros(x['mvalue']) if (pd.notnull(x['mvalue'])) else '', axis=1)
            pd_el9_1['Statistic'] = pd_el9_1['mvalue']

            pd_el9_1['Measure_Type'] = 'Data Profile'
            pd_el9_1['Active_Ind'] = 'Y'
            pd_el9_1['Display_Type'] = 'EL9.1'
            pd_el9_1['Calculation_Source'] = 'Python'
            pd_el9_1['in_measures'] = 1
            pd_el9_1['in_thresholds'] = 1

            pd_el9_1 = pd_el9_1.astype(DQM_Metadata.Reports.plan9_1.types)

            pd_el9_1['plan_id'] = pd_el9_1['plan_id'].str.replace('nan','.')
            pd_el9_1 = pd_el9_1[pd_el9_1['plan_id'] != '.']
            pd_el9_1 = pd_el9_1[pd_el9_1['plan_id'] != '']

            pd_el9_1['plan_type_el'] = pd_el9_1['plan_type_el'].str.replace('nan','')
            pd_el9_1 = pd_el9_1[DQM_Metadata.Reports.plan9_1.columns]
        else:
            pd_el9_1 = None

        # ------------------------------------------------------------------------------------
        #   Plan 8.2 + Plan 9.1
        # ------------------------------------------------------------------------------------
        if (pd_el8_2 is not None) and (pd_el9_1 is not None):
            plan = pd_el8_2.append(pd_el9_1)
            plan = plan.sort_values(['Measure_ID','plan_id'], ascending=True)
            plan = plan.fillna('')
            plan = plan[DQM_Metadata.Reports.plan8_2.columns]
            if (len(plan) == 0):
                plan.loc[0] = ['','','','','','','','','','','','','','','','','','','','']
        elif (pd_el8_2 is not None):
            plan = pd_el8_2
        elif (pd_el9_1 is not None):
            plan = pd_el9_1
        else:
            plan = None

        return {
            'summary': summary,
            'waiver': pd_el7_1,
            'plan': plan
        }

    # --------------------------------------------------------------------
    #
    #   Helper method for AWS Secrets
    #
    # --------------------------------------------------------------------
    def get_dbutils(self, spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

        return dbutils

    # --------------------------------------------------------------------
    #
    #   Read from S3 to Pandas Dataframe
    #
    # --------------------------------------------------------------------
    def s3(self, pgmstart=None):
        import boto3
        import pandas as pd
        import io
        import os

        spark = SparkSession.getActiveSession()

        dbutils = self.get_dbutils(spark)

        os.environ["AWS_ACCESS_KEY_ID"] = dbutils.secrets.get(scope="dqm", key="DQM_AWS_ACCESS_KEY_ID")
        os.environ["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get(scope="dqm", key="DQM_AWS_SECRET_ACCESS_KEY_ID")

        session = boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )

        bucket = session.resource('s3').Bucket(self.s3bucket)

        df = pd.DataFrame(columns=DQM_Metadata.Results.columns)

        if (pgmstart is not None):
            prefix = self.s3folder + '/' + pgmstart
        else:
            prefix = self.s3folder

        self.logger.debug('Reading S3: ' + self.s3bucket + '/' + prefix)

        s3 = boto3.client('s3')
        for obj in bucket.objects.filter(Prefix=prefix):
            if (str(obj.key).find('part-') > 1):
                obj = s3.get_object(Bucket=obj.bucket_name, Key=obj.key)
                df = df.append(pd.read_csv(io.BytesIO(obj['Body'].read()), dtype=DQM_Metadata.Results.types))

        df = df.drop_duplicates()
        df = df.sort_values(by=['submtg_state_cd', 'measure_id'])

        self.logger.debug(str(df.shape[0]) + 'rows read')

        return df

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def xlsx(self, id:str, pgmstart=None):
        import os
        from pyspark.sql.functions import lit

        spark = SparkSession.getActiveSession()

        dbutils = self.get_dbutils(spark)

        os.environ["AWS_ACCESS_KEY_ID"] = dbutils.secrets.get(scope="dqm", key="DQM_AWS_ACCESS_KEY_ID")
        os.environ["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get(scope="dqm", key="DQM_AWS_SECRET_ACCESS_KEY_ID")

        df_s3 = self.s3(pgmstart)
        if df_s3 is not None:

            s3_reports = self.reports(df_s3)
            if s3_reports is not None:

                df_rpt = s3_reports.get(id)
                if df_rpt is not None:

                    spark = SparkSession.getActiveSession()
                    sdf = spark.createDataFrame(df_rpt)

                    if sdf is not None:

                        if id == 'plan' and sdf.count() == 1:
                            sdf = sdf.where(sdf.RunID != '')

                        if id == 'summary':
                            sdf = sdf.withColumn('numer', lit(""))
                            sdf = sdf.withColumn('denom', lit(""))

                        if ((self.separate_entity == '1') or (self.separate_entity == '2')):
                            fn = f"MACBIS_DQ_{self.stabbrev}-{self.typerun}_{self.rpt_fldr}_run{self.z_run_id}_{self.date9}_{DQM_Metadata.Reports.rpt_to_fn[id]}"
                        else:
                            fn = f"MACBIS_DQ_{self.stabbrev}_{self.rpt_fldr}_run{self.z_run_id}_{self.date9}_{DQM_Metadata.Reports.rpt_to_fn[id]}"

                        sdf.write.format('com.crealytics.spark.excel') \
                            .option('header', 'true') \
                            .option('dataAddress', "'" + id + "'!A1") \
                            .mode('append') \
                            .save(self.s3xlsx + '/' + fn + '.xlsx')

    # --------------------------------------------------------------------
    #
    #   Method to focus runnning of measures by series/category/claim type
    #
    # --------------------------------------------------------------------
    def where(self, series: int = None, measure_cat: str = None, claim_file: str = None):

        subset = self.thresholds

        if (measure_cat is not None):
            subset = subset[subset['measure_cat'] == measure_cat.upper()]

        if (claim_file is not None):
            subset = subset[subset['claim_file'] == claim_file.upper()]

        results = subset['measure_id'].tolist()

        if (series is not None):
            subset = self.reverse_measure_lookup[self.reverse_measure_lookup['series'] == series]['measure_id']
            results = list(set(results) & set(subset))

        return results

    # --------------------------------------------------------------------
    #
    #   Base table accessor method direct or cached
    #   dx files can be accessed as: {DQMeasures.getBaseTable(dqm, 'dx', clm_type)}
    # --------------------------------------------------------------------
    def getBaseTable(self, level, claim_type):

        if level == '':
            level = 'cll'

        if (self.isTurbo):
            types = ['IP', 'LT', 'OT', 'RX']
            if (claim_type.upper() in types) and (level.upper() in ('CLH', 'CLL')):
                return self.turboDB + '.' + self.taskprep + '_' + self.z_run_id + '_prepop_' + level + '_' + claim_type
            elif (claim_type.upper() in types) and (level.upper() in ('DX')):
                return self.turboDB + '.' + self.taskprep + '_' + self.z_run_id + '_prepop_CLH_' + level + '_' + claim_type

        return self.taskprefix + '_base_' + level + '_' + claim_type

    # --------------------------------------------------------------------
    #
    #
    # --------------------------------------------------------------------
    def show(self, v) :
        print(self.sql[v])
    # -------------------------------------------------------------------------
    #
    #   TMSIS Latest Reporting Period Production Load Report
    #
    # -------------------------------------------------------------------------
    def load_report(self):
        z = f"""
            select
                'ELG' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_eligibility as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_eligibility
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt

            union all

            select
                'IP' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_ip as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_ip
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt

            union all

            select
                'LT' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_lt as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_lt
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt

            union all

            select
                'OT' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_ot as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_ot
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt

            union all

            select
                'RX' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_rx as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_rx
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt
                
            union all

            select
                'FTX' as file_type,
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt,
                max(d.tms_run_timestamp) as tms_run_timestamp,
                max(d.tms_create_date) as tms_create_date
            from
                {self.tmsis_input_schema}.file_header_record_ftx as d
            inner join
                (select
                    tms_reporting_period,
                    submitting_state,
                    max(tms_run_id) as tms_run_id
                from
                    {self.tmsis_input_schema}.file_header_record_ftx
                group by
                    tms_reporting_period,
                    submitting_state
                having
                    tms_reporting_period = '{self.m_start}'
                    and tms_run_id = {self.run_id}
                    and submitting_state = '{self.state}') as s
                on s.tms_run_id = d.tms_run_id
                and s.tms_reporting_period = d.tms_reporting_period
            group by
                d.submitting_state,
                d.tms_run_id,
                d.tms_reporting_period,
                d.tot_rec_cnt    
                
                """

        spark = SparkSession.getActiveSession()
        df = spark.sql(z).toPandas()

        self.logger.info('--------------------------------------------------------------------------------------------------')

        self.logger.info( \
            str('File Type\t') + \
            str('State\t') + \
            str('Name\t\t') + \
            str('TMSIS Run ID\t') + \
            str('TMSIS Reporting Period\t') + \
            str('Count of Records')
        )

        self.logger.info('--------------------------------------------------------------------------------------------------')

        if len(df) > 0:
            for index, x in df.iterrows():

                self.logger.info( \
                    str(x['file_type']) + '\t\t' + \
                    str(x['submitting_state']) + '\t' + \
                    str(self.stname) + '\t\t\t' + \
                    str(x['tms_run_id']) + '\t' + \
                    str(x['tms_reporting_period']) + '\t\t' + \
                    str(x['tot_rec_cnt']) \
                )

        else:
            self.logger.error('TMSIS Data Not Available for Reporting Period')

        self.logger.info('--------------------------------------------------------------------------------------------------')

    # --------------------------------------------------------------------
    #
    #   loader for reverse lookup catalog file
    #
    # --------------------------------------------------------------------
    def loadReverseMeasureLookup(self):
        import os
        import pandas as pd

        df = None

        this_dir, this_filename = os.path.split(__file__)
        two_up = os.path.dirname(this_dir)
        pkl = two_up + '/dqm/batch/reverse_lookup.pkl'

        if os.path.isfile(pkl):
            df = pd.read_pickle(pkl)

        return df

    # --------------------------------------------------------------------
    #
    #   load directives to drive a module
    #
    # --------------------------------------------------------------------
    def getRunRules(self, series: str):
        import os
        import pandas as pd

        df_run = None

        if not (series in list(self.run_rules.keys())):

            this_dir, this_filename = os.path.split(__file__)
            two_up = os.path.dirname(this_dir)
            pkl = two_up + '/dqm/batch/run_' + series + '.pkl'

            if os.path.isfile(pkl):
                df = pd.read_pickle(pkl)
                self.run_rules[series] = df
            else:
                self.logger.info('no batch file found: ' + pkl)

        if (series in list(self.run_rules.keys())):
            df_run = self.run_rules[series]

        return df_run

    # --------------------------------------------------------------------
    #
    #   Reverse lookup which series which a measure pertains
    #
    # --------------------------------------------------------------------
    def getSeriesForMeasure(self, measure: str):

        series = None

        if measure is not None and len(measure) > 0:
            u = self.reverse_measure_lookup[self.reverse_measure_lookup['measure_id'].str.upper() == measure.upper()]
            if (u['series'].size > 0):
                series = u['series'].iloc[0]
            else:
                self.logger.error('No realization for ' + str(measure))
            self.logger.debug('Next Measure ' + str(measure) + ' is from series ' + str(series))

        return series

    # --------------------------------------------------------------------
    #
    #   Execution of scoped measures
    #
    # --------------------------------------------------------------------
    def run(self, spark, measures: list = None, interactive=True):
        from time import perf_counter
        from dqm.Module import Module
        from datetime import timedelta
        from pyspark.sql.types import StringType, DecimalType, IntegerType, LongType, DoubleType
        from pyspark.sql.functions import lit

        module = Module()

        if measures is not None:
            for m in range(len(measures)):
                measures[m] = measures[m].upper().strip()

        self.logger.debug('Measures: ' + str(measures))

        # ------------------------------------------------------------------
        #   Thresholds metdata is required to run
        if (self.thresholds is not None):

            # active measures gleaned from thresholds
            active_measures = self.thresholds['measure_id'].unique().tolist()

            # runtime-scoped measures
            if measures is not None and len(measures) > 0:

                inactive = []   # track inactive measures
                unreal = []     # track unrealized measures
                # ----------------------------------------------------------
                for m in measures:
                    if m not in active_measures:
                        self.logger.debug('not in active ' + str(m))
                        inactive.append(m)

                    if m not in list(self.reverse_measure_lookup['measure_id']):
                        self.logger.debug('not realized ' + str(m))
                        unreal.append(m)
                # ----------------------------------------------------------

                if len(inactive) > 0:
                    self.logger.info('Inactive Measures (skipping): ' + str(inactive))
                    measures = list(set(measures) - set(inactive))

                if len(unreal) > 0:
                    self.logger.error('Unrealized Measures (skipping): ' + str(unreal))
                    measures = list(set(measures) - set(unreal))

                # identify unknown measures but allow the process to continue
                df_unk = list(set(measures) - set(active_measures))
                if len(df_unk) > 0:
                    self.logger.error('Unknown Measures (skipping): ' + str(df_unk))

                measures = list(set(measures) & set(active_measures))
                self.logger.debug(str(measures))

            # defeault to all know active measures
            elif measures is None:
                measures = self.reverse_measure_lookup[self.reverse_measure_lookup['measure_id'].isin(active_measures)]['measure_id']

        # ------------------------------------------------------------------
        elif measures is None:
            return None

        m = len(measures)
        n = 1

        self.logger.info('Running ' + str(m) + ' measure(s).')

        perf_start = perf_counter()

        # iterate through measures by id
        for mid in measures:

            mid = str(mid).upper()
            self.logger.debug('Measure Id: ' + mid)

            # stopwatch
            t_start = perf_counter()

            # parent series module for this measure
            series = self.getSeriesForMeasure(mid)
            if series is None:
                self.logger.error('No series found for measure: ' + str(mid))
            else:

                # parameterized directives or a measure
                rules = self.getRunRules(series)
                if rules is None:
                    self.logger.error('No rules found for series: ' + str(series))
                else:
                    # dereference the runner which realizes this measure
                    runner = module.runners[series]
                    if runner is None:
                        self.logger.error('No runner found for series: ' + str(series))
                    else:

                        # invoke the runner for this measure
                        rule = rules[rules['measure_id'] == mid]
                        x = rule.to_dict(orient='records')[0]
                        spark_df = runner.v_table[x['cb']](spark, self, mid, x)

                        # strong type safety
                        if (spark_df is not None):
                            spark_df = spark_df.withColumn('submtg_state_cd', spark_df['submtg_state_cd'].cast(StringType()))
                            spark_df = spark_df.withColumn('measure_id', spark_df['measure_id'].cast(StringType()))
                            spark_df = spark_df.withColumn('submodule', spark_df['submodule'].cast(StringType()))
                            spark_df = spark_df.withColumn('numer', spark_df['numer'].cast(DecimalType(38,20)))
                            spark_df = spark_df.withColumn('denom', spark_df['denom'].cast(DecimalType(38,20)))
                            spark_df = spark_df.withColumn('mvalue', spark_df['mvalue'].cast(DecimalType(38,20)))

                            if 'valid_value' not in spark_df.columns:
                                spark_df = spark_df.withColumn('valid_value', lit(''))
                            spark_df = spark_df.withColumn('valid_value', spark_df['valid_value'].cast(StringType()))

                            if 'claim_type' not in spark_df.columns:
                                spark_df = spark_df.withColumn('claim_type', lit(''))
                            spark_df = spark_df.withColumn('claim_type', spark_df['claim_type'].cast(StringType()))

                            if 'plan_id' not in spark_df.columns:
                                spark_df = spark_df.withColumn('plan_id', lit(''))
                            spark_df = spark_df.withColumn('plan_id', spark_df['plan_id'].cast(StringType()))

                            if 'plan_type_el' not in spark_df.columns:
                                spark_df = spark_df.withColumn('plan_type_el', lit(''))
                            spark_df = spark_df.withColumn('plan_type_el', spark_df['plan_type_el'].cast(StringType()))

                            if 'MultiplePlanTypes_el' not in spark_df.columns:
                                spark_df = spark_df.withColumn('MultiplePlanTypes_el', lit(''))
                            spark_df = spark_df.withColumn('MultiplePlanTypes_el', spark_df['MultiplePlanTypes_el'].cast(IntegerType()))

                            if 'plan_type_mc' not in spark_df.columns:
                                spark_df = spark_df.withColumn('plan_type_mc', lit(''))
                            spark_df = spark_df.withColumn('plan_type_mc', spark_df['plan_type_mc'].cast(StringType()))

                            if 'MultiplePlanTypes_mc' not in spark_df.columns:
                                spark_df = spark_df.withColumn('MultiplePlanTypes_mc', lit(''))
                            spark_df = spark_df.withColumn('MultiplePlanTypes_mc', spark_df['MultiplePlanTypes_mc'].cast(IntegerType()))

                            if 'linked' not in spark_df.columns:
                                spark_df = spark_df.withColumn('linked', lit(''))
                            spark_df = spark_df.withColumn('linked', spark_df['linked'].cast(StringType()))

                            if 'enrollment' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enrollment', lit(''))
                            spark_df = spark_df.withColumn('enrollment', spark_df['enrollment'].cast(LongType()))

                            if 'cap_hmo' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_hmo', lit(''))
                            spark_df = spark_df.withColumn('cap_hmo', spark_df['cap_hmo'].cast(LongType()))

                            if 'cap_php' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_php', lit(''))
                            spark_df = spark_df.withColumn('cap_php', spark_df['cap_php'].cast(LongType()))

                            if 'cap_pccm' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_pccm', lit(''))
                            spark_df = spark_df.withColumn('cap_pccm', spark_df['cap_pccm'].cast(LongType()))

                            if 'cap_phi' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_phi', lit(''))
                            spark_df = spark_df.withColumn('cap_phi', spark_df['cap_phi'].cast(LongType()))

                            if 'cap_oth' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_oth', lit(''))
                            spark_df = spark_df.withColumn('cap_oth', spark_df['cap_oth'].cast(LongType()))

                            if 'cap_tot' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_tot', lit(''))
                            spark_df = spark_df.withColumn('cap_tot', spark_df['cap_tot'].cast(LongType()))

                            if 'capitation_type' not in spark_df.columns:
                                spark_df = spark_df.withColumn('capitation_type', lit(''))
                            spark_df = spark_df.withColumn('capitation_type', spark_df['capitation_type'].cast(StringType()))

                            if 'plan_type' not in spark_df.columns:
                                spark_df = spark_df.withColumn('plan_type', lit(''))
                            spark_df = spark_df.withColumn('plan_type', spark_df['plan_type'].cast(StringType()))

                            if 'enc_ip' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enc_ip', lit(''))
                            spark_df = spark_df.withColumn('enc_ip', spark_df['enc_ip'].cast(LongType()))

                            if 'enc_lt' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enc_lt', lit(''))
                            spark_df = spark_df.withColumn('enc_lt', spark_df['enc_lt'].cast(LongType()))

                            if 'enc_ot' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enc_ot', lit(''))
                            spark_df = spark_df.withColumn('enc_ot', spark_df['enc_ot'].cast(LongType()))

                            if 'enc_rx' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enc_rx', lit(''))
                            spark_df = spark_df.withColumn('enc_rx', spark_df['enc_rx'].cast(LongType()))

                            if 'enc_tot' not in spark_df.columns:
                                spark_df = spark_df.withColumn('enc_tot', lit(''))
                            spark_df = spark_df.withColumn('enc_tot', spark_df['enc_tot'].cast(LongType()))

                            if 'ip_ratio' not in spark_df.columns:
                                spark_df = spark_df.withColumn('ip_ratio', lit(''))
                            spark_df = spark_df.withColumn('ip_ratio', spark_df['ip_ratio'].cast(DoubleType()))

                            if 'lt_ratio' not in spark_df.columns:
                                spark_df = spark_df.withColumn('lt_ratio', lit(''))
                            spark_df = spark_df.withColumn('lt_ratio', spark_df['lt_ratio'].cast(DoubleType()))

                            if 'ot_ratio' not in spark_df.columns:
                                spark_df = spark_df.withColumn('ot_ratio', lit(''))
                            spark_df = spark_df.withColumn('ot_ratio', spark_df['ot_ratio'].cast(DoubleType()))

                            if 'rx_ratio' not in spark_df.columns:
                                spark_df = spark_df.withColumn('rx_ratio', lit(''))
                            spark_df = spark_df.withColumn('rx_ratio', spark_df['rx_ratio'].cast(DoubleType()))

                            if 'cap_ratio' not in spark_df.columns:
                                spark_df = spark_df.withColumn('cap_ratio', lit(''))
                            spark_df = spark_df.withColumn('cap_ratio', spark_df['cap_ratio'].cast(DoubleType()))

                            if 'encounter_type' not in spark_df.columns:
                                spark_df = spark_df.withColumn('encounter_type', lit(''))
                            spark_df = spark_df.withColumn('encounter_type', spark_df['encounter_type'].cast(StringType()))

                            if 'waiver_id' not in spark_df.columns:
                                spark_df = spark_df.withColumn('waiver_id', lit(''))
                            spark_df = spark_df.withColumn('waiver_id', spark_df['waiver_id'].cast(StringType()))

                            if 'waiver_type' not in spark_df.columns:
                                spark_df = spark_df.withColumn('waiver_type', lit(''))
                            spark_df = spark_df.withColumn('waiver_type', spark_df['waiver_type'].cast(StringType()))

                            # logging
                            self.logger.info('(' + str(n) + '/' + str(m) + ')\t' \
                                + str(round((n/m)*100)) + '%' \
                                + '\tMeasure: ' + str(x['measure_id']) \
                                + '  \t(Series: ' + str(series) + ')'
                                )
                            self.logger.performance('Elapsed Time ' + str(x['measure_id']) + ': ' + str(round((perf_counter() - t_start) * 1000) / 1000) )

                            # persist result set in S3
                            if spark_df.count() > 0:
                                spark_df.select(DQM_Metadata.Results.columns).write.csv(self.s3path, header=True, mode='append')
                                spark_df.unpersist()
                                del spark_df

                            # "empty" result
                            else:
                                ez = f"""select
                                            '{self.state}' as submtg_state_cd
                                            ,'{mid}' as measure_id
                                            ,'{series}' as submodule
                                            ,null as numer
                                            ,null as denom
                                            ,null as mvalue
                                            ,'' as valid_value
                                            ,'' as claim_type
                                            ,'' as plan_id
                                            ,'' as plan_type_el
                                            ,null as MultiplePlanTypes_el
                                            ,'' as plan_type_mc
                                            ,null as MultiplePlanTypes_mc
                                            ,'' as linked
                                            ,null as enrollment
                                            ,null as cap_hmo
                                            ,null as cap_php
                                            ,null as cap_pccm
                                            ,null as cap_phi
                                            ,null as cap_oth
                                            ,null as cap_tot
                                            ,'' capitation_type
                                            ,'' as plan_type
                                            ,null as enc_ip
                                            ,null as enc_lt
                                            ,null as enc_ot
                                            ,null as enc_rx
                                            ,null as enc_tot
                                            ,null as ip_ratio
                                            ,null as lt_ratio
                                            ,null as ot_ratio
                                            ,null as rx_ratio
                                            ,null as cap_ratio
                                            ,'' as encounter_type
                                            ,'' as waiver_id
                                            ,'' as waiver_type
                                     """
                                empty_spark_df = spark.sql(ez)

                                empty_spark_df = empty_spark_df.withColumn('submtg_state_cd', empty_spark_df['submtg_state_cd'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('measure_id', empty_spark_df['measure_id'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('submodule', empty_spark_df['submodule'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('numer', empty_spark_df['numer'].cast(DecimalType(38,20)))
                                empty_spark_df = empty_spark_df.withColumn('denom', empty_spark_df['denom'].cast(DecimalType(38,20)))
                                empty_spark_df = empty_spark_df.withColumn('mvalue', empty_spark_df['mvalue'].cast(DecimalType(38,20)))
                                empty_spark_df = empty_spark_df.withColumn('valid_value', empty_spark_df['valid_value'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('claim_type', empty_spark_df['claim_type'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('plan_id', empty_spark_df['plan_id'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('plan_type_el', empty_spark_df['plan_type_el'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('MultiplePlanTypes_el', empty_spark_df['MultiplePlanTypes_el'].cast(IntegerType()))
                                empty_spark_df = empty_spark_df.withColumn('plan_type_mc', empty_spark_df['plan_type_mc'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('MultiplePlanTypes_mc', empty_spark_df['MultiplePlanTypes_mc'].cast(IntegerType()))
                                empty_spark_df = empty_spark_df.withColumn('linked', empty_spark_df['linked'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('enrollment', empty_spark_df['enrollment'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_hmo', empty_spark_df['cap_hmo'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_php', empty_spark_df['cap_php'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_pccm', empty_spark_df['cap_pccm'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_phi', empty_spark_df['cap_phi'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_oth', empty_spark_df['cap_oth'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_tot', empty_spark_df['cap_tot'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('capitation_type', empty_spark_df['capitation_type'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('plan_type', empty_spark_df['plan_type'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('enc_ip', empty_spark_df['enc_ip'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('enc_lt', empty_spark_df['enc_lt'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('enc_ot', empty_spark_df['enc_ot'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('enc_rx', empty_spark_df['enc_rx'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('enc_tot', empty_spark_df['enc_tot'].cast(LongType()))
                                empty_spark_df = empty_spark_df.withColumn('ip_ratio', empty_spark_df['ip_ratio'].cast(DoubleType()))
                                empty_spark_df = empty_spark_df.withColumn('lt_ratio', empty_spark_df['lt_ratio'].cast(DoubleType()))
                                empty_spark_df = empty_spark_df.withColumn('ot_ratio', empty_spark_df['ot_ratio'].cast(DoubleType()))
                                empty_spark_df = empty_spark_df.withColumn('rx_ratio', empty_spark_df['rx_ratio'].cast(DoubleType()))
                                empty_spark_df = empty_spark_df.withColumn('cap_ratio', empty_spark_df['cap_ratio'].cast(DoubleType()))
                                empty_spark_df = empty_spark_df.withColumn('encounter_type', empty_spark_df['encounter_type'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('waiver_id', empty_spark_df['waiver_id'].cast(StringType()))
                                empty_spark_df = empty_spark_df.withColumn('waiver_type', empty_spark_df['waiver_type'].cast(StringType()))

                                # reorder columns to ensure all part files are in the same order
                                empty_spark_df.select(DQM_Metadata.Results.columns).write.csv(self.s3path, header=True, mode='append')
                                empty_spark_df.unpersist()
                                del empty_spark_df

                    del runner

            n += 1

        # cleanup
        del module

        del measures

        # performance metrics
        if (m > 0):

            perf_end = perf_counter()
            perf_total = perf_end - perf_start
            avg_perf_per_measure = perf_total / m
            avg_perf_per_measure = round(avg_perf_per_measure * 1000) / 1000
            perf_total = round(perf_total * 1000) / 1000
            ftime = timedelta(seconds=perf_total)
            favg_perf_per_measure = timedelta(seconds=avg_perf_per_measure)

            self.logger.performance('Total Elapsed Time: ' + str(ftime).split('.', 2)[0] + ' (' + str(m) + ' measures)\tAverage Time per Measure: ' + str(favg_perf_per_measure).split('.', 2)[0])

            if (interactive):
                return self.s3(self.pgmstart)
            else:
                return None
        else:
            return None

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