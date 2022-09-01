# Databricks notebook source
# initialize variables with parameter values passed from job
_reportMonth = dbutils.widgets.get("reportMonth")
_stateCode   = dbutils.widgets.get("stateCode")
_entity      = dbutils.widgets.get("entity")
_runID       = dbutils.widgets.get("runID")

# COMMAND ----------

# import library and instantiate dqm object
import logging
from dqm import DQMeasures as s

  
if (len(_runID)):
  dqm = s.DQMeasures(report_month    = _reportMonth
                   , rpt_state       = _stateCode
                   , separate_entity = _entity
                   , run_id          = _runID)
else:
  dqm = s.DQMeasures(report_month    = _reportMonth
                   , rpt_state       = _stateCode
                   , separate_entity = _entity)

# COMMAND ----------

# set level of verbosity for logging
dqm.logger.setLevel(logging.DEBUG)

# COMMAND ----------

# create base tables and views
dqm.init()

# COMMAND ----------

# dqm.thresholds(spark, 's3a://macbis-dw-dqm-val/sas-dqm/thresholds/Thresholds_Document_v2.5.3_CMS-OD.xlsx')
# dqm.setThresholds(spark, 's3a://macbis-dw-dqm-val/sas-dqm/thresholds/Thresholds Document v2.6.xlsx')
#dqm.setThresholds(spark, 's3a://macbis-dw-dqm-val/sas-dqm/thresholds/Thresholds Document v2.6.1_CMS-OD.xlsx')

# COMMAND ----------

#display(dqm.thresholds)

# COMMAND ----------

# configure spark with the task prefix for this run
spark.conf.set('dqm.taskprefix', dqm.taskprefix)

# COMMAND ----------

# redirect s3 bucket folder path
# conditionally handle seperate entities for medicaid and chip
if (_entity == '0'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.1/' + dqm.rpt_state + '/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path
elif (_entity == '1'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.1/' + dqm.rpt_state + '-M/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path
elif (_entity == '2'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.1/' + dqm.rpt_state + '-C/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path

# COMMAND ----------

# display useful information about this run
dqm.print()

# COMMAND ----------

# run all measures
dqm.run(spark, interactive=False)

# COMMAND ----------

# excel file output
dqm.xlsx('summary')
dqm.xlsx('plan')
dqm.xlsx('waiver')