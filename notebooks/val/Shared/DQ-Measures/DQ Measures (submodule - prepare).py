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
# MAGIC 
# MAGIC ###### 5) Run submodules 100, 200, 500, 600, 700, 800, and 900 in Parallel

# COMMAND ----------

