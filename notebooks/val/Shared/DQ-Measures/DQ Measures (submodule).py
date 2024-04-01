# Databricks notebook source
from dqm import DQMeasures as s
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Job Parameters

# COMMAND ----------

_reportMonth = dbutils.widgets.get("reportMonth")
_stateCode = dbutils.widgets.get("stateCode")
_entity = dbutils.widgets.get("entity")
_module = dbutils.widgets.get("module")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2) Instantiate DQ Measures

# COMMAND ----------

dqm = s.DQMeasures(_reportMonth, _stateCode, _entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 3) Logging Threshold

# COMMAND ----------

dqm.logger.setLevel(logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 4) Initialize Run

# COMMAND ----------

dqm.init()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 5) Run Measures

# COMMAND ----------

# redirect s3 bucket folder path
# conditionally handle seperate entities for medicaid and chip
if (_entity == '0'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.2/' + dqm.rpt_state + '/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path
elif (_entity == '1'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.2/' + dqm.rpt_state + '-M/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path
elif (_entity == '2'):
  dqm.setS3Bucket('macbis-dw-dqm-val')
  dqm.s3folder = 'sas-dqm/uat/3.2/' + dqm.rpt_state + '-C/' + dqm.rpt_fldr + '/' + dqm.z_run_id
  dqm.s3xlsx = dqm.s3proto + dqm.s3bucket + '/' + dqm.s3folder
  dqm.s3path = dqm.s3xlsx + '/' + dqm.pgmstart
  dqm.s3path

# COMMAND ----------

# configure spark with the task prefix for this run
spark.conf.set('dqm.taskprefix', dqm.taskprefix)

# COMMAND ----------

# display useful information about this run
dqm.print()

# COMMAND ----------

if _module == "100":
    dqm.run(
        spark,
        dqm.where(series="101")
        + dqm.where(series="102")
        + dqm.where(series="103")
        + dqm.where(series="104")
        + dqm.where(series="105")
        + dqm.where(series="106")
        + dqm.where(series="107")
        + dqm.where(series="108")
        + dqm.where(series="109")
        + dqm.where(series="110"),
    )
elif _module == "200":
    dqm.run(
        spark,
        dqm.where(series="201")
        + dqm.where(series="202")
        + dqm.where(series="204")
        + dqm.where(series="205")
        + dqm.where(series="206"),
    )
elif _module == "500":
    dqm.run(
        spark,
        dqm.where(series="501")
        + dqm.where(series="502")
        + dqm.where(series="503")
        + dqm.where(series="504"),
    )
elif _module == "600":
    dqm.run(
        spark,
        dqm.where(series="601") + dqm.where(series="602") + dqm.where(series="603"),
    )
elif _module == "700":
    dqm.run(
        spark,
        dqm.where(series="701")
        + dqm.where(series="702")
        + dqm.where(series="703")
        + dqm.where(series="704")
        + dqm.where(series="705")
        + dqm.where(series="706")
        + dqm.where(series="707")
        + dqm.where(series="708")
        + dqm.where(series="709")
        + dqm.where(series="710")
        + dqm.where(series="711")
        + dqm.where(series="712")
        + dqm.where(series="713")
        + dqm.where(series="714")
        + dqm.where(series="715")
        + dqm.where(series="716"),
    )
elif _module == "800":
    dqm.run(spark, dqm.where(series="802") + dqm.where(series="803"))
elif _module == "900":
    dqm.run(
        spark,
        dqm.where(series="901")
        + dqm.where(series="902")
        + dqm.where(series="903")
        + dqm.where(series="904")
        + dqm.where(series="905")
        + dqm.where(series="906")
        + dqm.where(series="907")
        + dqm.where(series="909")
        + dqm.where(series="910")
        + dqm.where(series="911")
        + dqm.where(series="912")
        + dqm.where(series="913")
        + dqm.where(series="914")
        + dqm.where(series="915")
        + dqm.where(series="916")
        + dqm.where(series="917")
        + dqm.where(series="918")
        + dqm.where(series="919"),
    )