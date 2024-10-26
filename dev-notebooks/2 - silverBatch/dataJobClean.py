# Databricks notebook source
# MAGIC %md
# MAGIC # Data Job Clean

# COMMAND ----------

# MAGIC %md
# MAGIC Notebook de desenvolvimento responável por limpar os dados da camda bruta e criar uma tabela silver preparada para agregações 

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Foiac/MobileFraudDetectSolution/main/Editaveis/silverjobtransformer.png" alt="Clean Data" style="width: 800px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import dependecies

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Path Definitions

# COMMAND ----------

storage_account_name  = "stacmfraud"
container_name = "cont-fraud"

database_name_input = "bronze_mobile"
table_name_input = "access"
delta_table_path_input = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/{database_name_input}/{table_name_input}"

database_name_output = "silver_mobile"
table_name_output = "tab_mobil_access"
container_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/{database_name_input}"
delta_table_path_output = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/{database_name_output}/{table_name_output}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Database Create

# COMMAND ----------

spark.sql(f"""CREATE DATABASE IF NOT EXISTS {database_name_output} LOCATION '{container_path}'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {database_name_output}.{table_name_output} (
    IMEI STRING,
    MAC STRING,
    NETWORK STRING,
    IP STRING,
    LATITUDE STRING,
    LONGITUDE STRING,
    UID STRING,
    PASSWORD STRING,
    `TRANSACTION` BOOLEAN,
    `FEATURE` STRING,
    `FEATURE_FLOW` STRING,
    OPERAT_SYSTEM STRING,
    PHONE_BRAND STRING,
    APP_VERSION STRING,
    ERROR_INF STRING,
    DT_EVENT TIMESTAMP,
    DAT_REF STRING
    ) 
USING DELTA
PARTITIONED BY (DAT_REF)
LOCATION '{delta_table_path_output}'""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Databricks Catalog Table

# COMMAND ----------

df = spark.table(f"{database_name_input}.{table_name_input}").filter(F.col("dat_ref") == '2024-08-01')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove duplicate **rows**

# COMMAND ----------

df_cleaned = df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove null or empty columns

# COMMAND ----------

df_filtered = df_cleaned.filter(F.col("timestamp").isNotNull() & (F.col("timestamp") != ''))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Types conversion

# COMMAND ----------

df_converted = (df_filtered
      .withColumn("transaction", F.when(F.col("transaction") == "true", True).when(F.col("transaction") == "false", False).otherwise(None)
      )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mapping and conversion of technical information

# COMMAND ----------

error_data = [("0", "Oper. com Sucesso"), 
              ("INCORRECT_PASS", "Senha incorreta"), 
              ("USER_NOT_FOUND", "Usuário não encontrado")]

columns = ["tec_error", "func_error"]

df_error = spark.createDataFrame(error_data, schema=columns)

df_mapping = df_converted.join(df_error, df_converted.error==df_error.tec_error, how="left")
df_mapping = df_mapping.drop("error").drop("tec_error")

api_data = [("login-authentication", "Login")]
endpoint_data = [("v1/login", "Login com senha")]

df_mapping = df_mapping.withColumn('api', F.when(F.col("api") == "login-authentication", F.lit("Login")).otherwise(F.col("api")))
df_mapping = df_mapping.withColumn('endpoint', F.when(F.col("endpoint") == "v1/login", F.lit("Login com senha")).otherwise(F.col("endpoint")))

display(df_mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renomear colunas

# COMMAND ----------

df_silver = (df_mapping
             .withColumnRenamed("imei", "IMEI")
             .withColumnRenamed("mac", "MAC")
             .withColumnRenamed("network", "NETWORK")
             .withColumnRenamed("client_ip", "IP")
             .withColumnRenamed("latitude", "LATITUDE")
             .withColumnRenamed("longitude", "LONGITUDE")
             .withColumnRenamed("uid", "UID")
             .withColumnRenamed("password", "PASSWORD")
             .withColumnRenamed("transaction", "TRANSACTION")
             .withColumnRenamed("api", "FEATURE")
             .withColumnRenamed("endpoint", "FEATURE_FLOW")
             .withColumnRenamed("os", "OPERAT_SYSTEM")
             .withColumnRenamed("phone_brand", "PHONE_BRAND")
             .withColumnRenamed("app_version", "APP_VERSION")
             .withColumnRenamed("func_error", "ERROR_INF")
             .withColumnRenamed("dat_ref","DAT_REF")
             .withColumn("DT_EVENT", F.from_unixtime(F.col("timestamp").cast("long")/1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
             .drop("timestamp")
             )
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escrever dados em tabela silver

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("append") \
    .option("path", delta_table_path_output) \
    .partitionBy("DAT_REF") \
    .saveAsTable(f"{database_name_output}.{table_name_output}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.silver_mobile.tab_mobil_access where DAT_REF == '2024-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS silver_mobile.tab_mobil_access;