# Databricks notebook source
# MAGIC %md
# MAGIC # Optimize and Compare Delta Table
# MAGIC Notebook de desenvolvimento resposnável por otimizar arquivos parquets em delta table através de comando optimize

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inf. Delta Table before `optimize`

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL hive_metastore.bronze_mobile.access"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exec. command 

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{delta_table_path}` WHERE dat_ref = '2024-08-01'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inf. Delta Table after `optimize`

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL hive_metastore.bronze_mobile.access"))