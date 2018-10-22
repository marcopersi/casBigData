# Databricks notebook source
 dbutils.fs.mount(
  source = "wasbs://landi@casbigdata.blob.core.windows.net/",
  mount_point = "/mnt/landi",
  extra_configs = {"fs.azure.account.key.casbigdata.blob.core.windows.net": "tqPINCBQrLmpTPX500LLF8xXMpPujPcR61fpe91xThFo21VGlxV6OcEmPkUyPM8h96OXZXwDyA+5UeabrCj5Ug=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/landi")

# COMMAND ----------

#online data
dfpostleitzahlen = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/PostleitzahlenSchweiz.csv")

# COMMAND ----------

dfpostleitzahlen.printSchema()
dfpostleitzahlen.write.mode("overwrite").saveAsTable("postleitzahlen")

# COMMAND ----------

storage_account_access_key = "tqPINCBQrLmpTPX500LLF8xXMpPujPcR61fpe91xThFo21VGlxV6OcEmPkUyPM8h96OXZXwDyA+5UeabrCj5Ug=="

# COMMAND ----------

file_type = "csv"
spark.conf.set(
  "fs.azure.account.key.casbigdata.blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

#online data
dfonline = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/online/onlineTransactions.csv")

# COMMAND ----------

dfonline.printSchema()

# COMMAND ----------

dfonline.write.mode("overwrite").saveAsTable("onlineTrx")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as AnzahlLieferungen, Liefermethode from onlinetrx group by Liefermethode order by AnzahlLieferungen desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from onlinetrx

# COMMAND ----------

from pyspark.sql.functions import *

dfonline = spark.table("onlinetrx")
# aggregating by product
#dfGroupedOnline = dfonline.groupBy('ProductId').agg(sum('ProduktUmsatz').alias('Umsatz'), sum('OrderQuantity').alias(' # Anzahl'), first('Product').alias('Produkt')).sort(col("Umsatz").desc()).withColumn("Umsatz", round("Umsatz",2))

dfGroupedOnline = dfonline.groupBy('ProductId').agg(sum('ProduktUmsatz').alias('Umsatz'), sum('OrderQuantity').alias('# Anzahl'), count('ProductId').alias("Anzahl Records")).sort(col("Umsatz").desc()).withColumn("Umsatz", round("Umsatz",2))

print(dfGroupedOnline.count())
display(dfGroupedOnline)

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(produktumsatz),2) from onlinetrx

# COMMAND ----------

from pyspark.sql.functions import *

dfonline = spark.table("onlinetrx")

# aggregating by date and product -> create a time series
dfGroupedOnline = dfonline.groupBy(to_date('Datum')).agg(sum('ProduktUmsatz').alias('Umsatz'), sum('OrderQuantity').alias('# Anzahl'), count('ProductId').alias("Anzahl Records")).sort(col("Umsatz").desc()).withColumn("Umsatz", round("Umsatz",2))

#dfGroupedOnline = dfonline.groupBy(to_date('Datum').format('dd.MM.yyyy')).agg(sum('ProduktUmsatz').alias('Umsatz'), sum('OrderQuantity').alias('# Anzahl'), count('ProductId').alias("Anzahl Records")).sort(col("Umsatz").desc()).withColumn("Umsatz", round("Umsatz",2))


print(dfGroupedOnline.count())
display(dfGroupedOnline)

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(Datum, 'dd.MM.yyyy') as Datum, sum(Produktumsatz) as Umsatz, sum(OrderQuantity) as Anzahl, first(Product) as Produkt from onlinetrx group by Datum, ProductId order by Umsatz desc
