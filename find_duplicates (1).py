# Databricks notebook source
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import *

dbutils = DBUtils(spark)

# COMMAND ----------

# MAGIC %md #Topics

# COMMAND ----------

topicsLs = dbutils.fs.ls("s3a://sagerx-enterprisedata-dev-useast1-comm/raw/DMD/Extracted/topics/")

eventId = "1ec163042f0ee90-ae-000-021387219554"
contentType = "guidelines"
urlType = "T"

for file in topicsLs:
    fileDF = spark.read.option("sep", "|").option("header", True).option("inferSchema", True).csv(file.path)
    fileDF = fileDF.select("*").where((col("EVENT_ID") == eventId) & (col("URL_TYPE") == urlType) & (col("CONTENT_TOPIC") == contentType))
    if(fileDF.count()):
        display(fileDF)
        print(f"file ({eventId} + {urlType} + {contentType}) => {file.path}")

# COMMAND ----------

# MAGIC %md #Events

# COMMAND ----------

eventsLs = dbutils.fs.ls("s3a://sagerx-enterprisedata-dev-useast1-comm/raw/DMD/Extracted/events/")

eventId = "1ec17d0906f6a60-ae-000-021452486598"
sessionId = "015061a1-515c-4cdd-8478-2ac3808a91fa"
dgid = "U082166143"

for file in eventsLs:
    fileDF = spark.read.option("sep", "|").option("header", True).option("inferSchema", True).csv(file.path)
    fileDF = fileDF.select("*").where((col("EVENT_ID") == eventId) & (col("SESSION_ID") == sessionId) & (col("DGID") == dgid))
    if(fileDF.count()):
        display(fileDF)
        print(f"file (`{eventId}` + `{sessionId}` + `{dgid}`) => {file.path}")

# COMMAND ----------

# MAGIC %md #Diseases

# COMMAND ----------

diseasesLs = dbutils.fs.ls("s3a://sagerx-enterprisedata-dev-useast1-comm/raw/DMD/Extracted/diseases/")

eventId = "1ec1cecca29a830-ae-000-021640681918"
urlType = "T"
diseaseCategory = "Rheumatology"
disease = "Arthritis"
subDisease = "Rheumatoid Arthritis"
order = "tertiary"

for file in diseasesLs:
    fileDF = spark.read.option("sep", "|").option("header", True).option("inferSchema", True).csv(file.path)
    fileDF = (fileDF.select("*")
        .where((col("EVENT_ID") == eventId) & 
               (col("URL_TYPE") == urlType) & 
               (col("DISEASE_CATEGORY") == diseaseCategory) & 
               (col("DISEASE") == disease) & 
               (col("SUB_DISEASE") == subDisease) & 
               (col("ORDER") == order)
        )
    )
    if(fileDF.count()):
        display(fileDF)
        print(f"file (`{eventId} + `{urlType} + `{diseaseCategory} + `{disease} + `{subDisease} + `{order}) => {file.path}")

# COMMAND ----------


