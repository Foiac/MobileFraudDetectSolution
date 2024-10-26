# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC Notebook de desenvolvimento responável por realizar processo streaming de ingestão de dados, o spark streaming busca os dados de um eventhub e escreve em formato delta table no 
# MAGIC Azure Data Lake Storage

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Foiac/MobileFraudDetectSolution/main/Editaveis/eventhubstreamingingestion.png" alt="SparkStreaming Ingest" style="width: 800px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import dependecies

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Path definitions and secrets

# COMMAND ----------

storage_account_name  = "stacmfraud"

database_name = "bronze_mobile"
table_name = "access" 

container_name = "cont-fraud"
container_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/{database_name}"
delta_table_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/{database_name}/{table_name}"

eventhub_name = "fraud-detect"

connection_string = dbutils.secrets.get(scope="dbwsscope", key="eh-cs-secret")

# COMMAND ----------

delta_table_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connection String Configuration

# COMMAND ----------

eh_conf = {
   'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(f"{connection_string};EntityPath={eventhub_name}")
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Database Create

# COMMAND ----------

spark.sql(f"""CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{container_path}'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        imei STRING,
        mac STRING,
        network STRING,
        client_ip STRING,
        latitude STRING,
        longitude STRING,
        uid STRING,
        password STRING,
        `transaction` STRING,
        api STRING,
        endpoint STRING,
        os STRING,
        phone_brand STRING,
        app_version STRING,
        error STRING,
        `timestamp` STRING,
        dat_ref STRING
        )
    USING DELTA
    PARTITIONED BY (dat_ref)
    LOCATION '{delta_table_path}'""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data Stream

# COMMAND ----------

df = (spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load())
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Edit dictionary

# COMMAND ----------

# Definir o esquema para o JSON
schema = StructType([
    StructField("imei", StringType(), True),
    StructField("mac", StringType(), True),
    StructField("network", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("password", StringType(), True),
    StructField("transaction", StringType(), True),
    StructField("api", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("os", StringType(), True),
    StructField("phone_brand", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("error", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("dat_ref", StringType(), True)
])

# Conversão do campo body do eventhub para um DataFrame
df = (df
      .select(F.col("Body").cast("string"))
      .withColumn("json_list", F.from_json(F.col("Body"), ArrayType(schema)))
      .select(F.explode(F.col("json_list")).alias("json_data"))
      )

# Selecionar e exibir campos individuais
df_body = (df.select(
    F.col("json_data.imei"),
    F.col("json_data.mac"),
    F.col("json_data.network"),
    F.col("json_data.client_ip"),
    F.col("json_data.latitude"),
    F.col("json_data.longitude"),
    F.col("json_data.uid"),
    F.col("json_data.password"),
    F.col("json_data.transaction"),
    F.col("json_data.api"),
    F.col("json_data.endpoint"),
    F.col("json_data.os"),
    F.col("json_data.phone_brand"),
    F.col("json_data.app_version"),
    F.col("json_data.error"),
    F.col("json_data.timestamp"))
      .withColumn("imei", F.sha2(F.col("imei"), 256))
      .withColumn("mac", F.sha2(F.col("mac"), 256))
      .withColumn("uid", F.sha2(F.col("uid"), 256))
      .withColumn("password", F.sha2(F.col("password"), 256))
      .withColumn("dat_ref", F.from_unixtime(F.col("timestamp").cast("long")/1000, "yyyy-MM-dd"))
      )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data on delta table

# COMMAND ----------

# Escrever os dados em uma tabela Delta

query = (df_body.writeStream
         .format("delta")
         .outputMode("append")
         .trigger(processingTime="2 minute")
         .option("checkpointLocation", f"{delta_table_path}/_checkpoints/")
         .partitionBy("dat_ref")
         .start(delta_table_path))


# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP DATABASE IF EXISTS bronze_mobile CASCADE;