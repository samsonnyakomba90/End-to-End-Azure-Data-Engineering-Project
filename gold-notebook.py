# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing Data

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidlakesam.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidlakesam.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidlakesam.dfs.core.windows.net", "3dd46d9b-434b-4ac8-8d52-a2b77504da5b")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidlakesam.dfs.core.windows.net", "3Ba8Q~1rFl3gZtvqkrg~AJdZBMoymo1lsn9bhaBQ")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidlakesam.dfs.core.windows.net", "https://login.microsoftonline.com/e471f015-dc23-41bb-ac78-835a60efccda/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Database creation 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read, Write and Create Delta Tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC #### storage variables

# COMMAND ----------

silver = "abfss://silver@nyctaxidlakesam.dfs.core.windows.net"
gold = "abfss://gold@nyctaxidlakesam.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA ZONE**

# COMMAND ----------

df_zone = spark.read.format("parquet")\
                        .option("inferSchema", True)\
                            .option("header", True)\
                                .load(f'{silver}/trip_zone')

df_zone.display()                    

# COMMAND ----------

df_zone.write.format("delta")\
                .mode("append")\
                    .option('path',f'{gold}/trip_zone')\
                    .saveAsTable("gold.trip_zone")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone
# MAGIC WHERE Zone = 'Astoria'

# COMMAND ----------

# MAGIC %md
# MAGIC **TRIP TYPE**

# COMMAND ----------

df_trip_type = spark.read.format("parquet")\
                        .option("inferSchema", True)\
                            .option("header", True)\
                                .load(f'{silver}/trip_type')

df_trip_type.display()                    

# COMMAND ----------

df_trip_type.write.format("delta")\
                .mode("append")\
                    .option('path',f'{gold}/trip_type')\
                    .saveAsTable("gold.trip_type")

# COMMAND ----------

# MAGIC %md
# MAGIC **TRIP DATA**

# COMMAND ----------

df_trip = spark.read.format("parquet")\
                        .option("inferSchema", True)\
                            .option("header", True)\
                                .load(f'{silver}/trip2023data')

df_trip.display()                    

# COMMAND ----------

df_trip.write.format("delta")\
                .mode("append")\
                    .option('path',f'{gold}/trip2023data')\
                    .saveAsTable("gold.trip2023")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELTA LAKE

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone
# MAGIC SET Borough = 'EMR'
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone
# MAGIC WHERE LocationID = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip_zone
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.trip_zone TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data 2023**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip2023

# COMMAND ----------

