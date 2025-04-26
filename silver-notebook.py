# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Access

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *



# COMMAND ----------

secret = '3Ba8Q~1rFl3gZtvqkrg~AJdZBMoymo1lsn9bhaBQ'
app_id = '3dd46d9b-434b-4ac8-8d52-a2b77504da5b'
ten_id = 'e471f015-dc23-41bb-ac78-835a60efccda'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidlakesam.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidlakesam.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidlakesam.dfs.core.windows.net", "3dd46d9b-434b-4ac8-8d52-a2b77504da5b")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidlakesam.dfs.core.windows.net", "3Ba8Q~1rFl3gZtvqkrg~AJdZBMoymo1lsn9bhaBQ")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidlakesam.dfs.core.windows.net", "https://login.microsoftonline.com/e471f015-dc23-41bb-ac78-835a60efccda/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxidlakesam.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **CSV Data Reading**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                        .option("inferSchema",True)\
                        .option("header", True)\
                        .load("abfss://bronze@nyctaxidlakesam.dfs.core.windows.net/trip_type")


# COMMAND ----------

display(df_trip_type)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip Zone

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                        .option("inferSchema",True)\
                        .option("header", True)\
                        .option("recusiveFileLookup", True)\
                        .load("abfss://bronze@nyctaxidlakesam.dfs.core.windows.net/trip_zone")


# COMMAND ----------

display(df_trip_zone)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip Data

# COMMAND ----------

df_trip = spark.read.format("parquet")\
    .schema(myschema)\
    .load('abfss://bronze@nyctaxidlakesam.dfs.core.windows.net/trip2023data/trip-data/')


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType

myschema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),              
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", LongType(), True),       
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),            
    StructField("trip_type", LongType(), True),               
    StructField("congestion_surcharge", DoubleType(), True)
])


# COMMAND ----------

df_trip = spark.read.format("parquet")\
                        .schema(myschema)\
                        .option("header", True)\
                        .option("recursiveFileLookup", True)\
                        .load('abfss://bronze@nyctaxidlakesam.dfs.core.windows.net/trip2023data')


# COMMAND ----------


df_trip.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type**

# COMMAND ----------

df_trip_type.display()


# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed("description", "trip_description")
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxidlakesam.dfs.core.windows.net/trip_type")\
                    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn("zone1", split(col("Zone"), "/")[0])\
                             .withColumn("zone2", split(col("Zone"), "/")[1])\
                     
df_trip_zone.display()


# COMMAND ----------

df_trip_zone.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxidlakesam.dfs.core.windows.net/trip_zone")\
                    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------


df_trip = df_trip.withColumn('trip_date', to_date(col('lpep_pickup_datetime')))\
                    .withColumn('trip_year', year(col('lpep_pickup_datetime')))\
                        .withColumn('trip_month', month(col('lpep_pickup_datetime')))

display(df_trip)
                    
            
                                
              

# COMMAND ----------

df_trip = df_trip.select("VendorID", "PULocationID", "DOLocationID","fare_amount", "total_amount") 
display(df_trip)

# COMMAND ----------

df_trip.write.format("parquet")\
                    .mode("append")\
                    .option("path","abfss://silver@nyctaxidlakesam.dfs.core.windows.net/trip2023data")\
                    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC **Data Analysis**

# COMMAND ----------

display(df_trip)

# COMMAND ----------

