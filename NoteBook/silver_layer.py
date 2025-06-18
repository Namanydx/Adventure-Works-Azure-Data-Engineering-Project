# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Access using App

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.awdatalakee.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awdatalakee.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awdatalakee.dfs.core.windows.net", "073d254f-c94b-433c-8966-939a0c8b610b")
spark.conf.set("fs.azure.account.oauth2.client.secret.awdatalakee.dfs.core.windows.net", "Gc48Q~pGJ23-_0Rt2BJIwjjkkih02.JCTHoQTbUV")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awdatalakee.dfs.core.windows.net", "https://login.microsoftonline.com/967199aa-3abc-4202-97be-1f35dbaf57f4/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data Loading

# COMMAND ----------

df_cal = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cus = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_procat = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_subcat = spark.read.format("csv")\
                .option("header","True")\
                .option("inferSchema","True")\
                .load("abfss://bronze@awdatalakee.dfs.core.windows.net/Product_Subcategories")  

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calendars

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month',month(col('Date')))\
           .withColumn('Year',year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn('Fullname',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

# COMMAND ----------

df_cus.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

df_procat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sub-categories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn("ProductSKU",split(col("ProductSKU"),'-')[0])\
                .withColumn("ProductName",split(col("ProductName"),' ')[0])
                

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn("StockDate", to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn("OrderNumber", regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn("multiply",col('OrderLineItem')*col('orderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_Order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakee.dfs.core.windows.net/AdventureWorks_df_sales")\
            .save()