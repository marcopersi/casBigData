# Databricks notebook source
 dbutils.fs.mount(
  source = "wasbs://landi@casbigdata.blob.core.windows.net/",
  mount_point = "/mnt/landi",
  extra_configs = {"fs.azure.account.key.casbigdata.blob.core.windows.net": "tqPINCBQrLmpTPX500LLF8xXMpPujPcR61fpe91xThFo21VGlxV6OcEmPkUyPM8h96OXZXwDyA+5UeabrCj5Ug=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/landi/offline")

# COMMAND ----------

storage_account_access_key = "tqPINCBQrLmpTPX500LLF8xXMpPujPcR61fpe91xThFo21VGlxV6OcEmPkUyPM8h96OXZXwDyA+5UeabrCj5Ug=="

# COMMAND ----------

file_type = "csv"
spark.conf.set(
  "fs.azure.account.key.casbigdata.blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

#creating dataframe, displaying first 5 records
df = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_O-R.csv")
df.take(5)

# COMMAND ----------

display(df.select("Lieferdatum", "VerkaufspreisInkl", "Bezeichnung", "Mandat"))


# COMMAND ----------

for name, dtype in df.dtypes:
  print(name, dtype)

# COMMAND ----------

#creating data frames
dfAF = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("encoding", "UTF-8").option("charset", "ISO-8859-1").load("/mnt/landi/offline/Offline_A-F.csv")
dfGL = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("encoding", "UTF-8").option("charset", "ISO-8859-1").load("/mnt/landi/offline/Offline_G-L.csv")
dfMN = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("encoding", "UTF-8").option("charset", "ISO-8859-1").load("/mnt/landi/offline/Offline_M-N.csv")
dfOR = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("encoding", "UTF-8").option("charset", "ISO-8859-1").load("/mnt/landi/offline/Offline_O-R.csv")
dfSZ = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("encoding", "UTF-8").option("charset", "ISO-8859-1").load("/mnt/landi/offline/Offline_S-Z.csv")


# COMMAND ----------

dfAF.printSchema()
print "dfAF contains " + str(dfAF.count()) + " records"

dfGL.printSchema()
print "dfGL contains " + str(dfGL.count()) + " records"

dfMN.printSchema()
print "dfMN contains " + str(dfMN.count()) + " records"

dfOR.printSchema()
print "dfOR contains " + str(dfOR.count()) + " records"

dfSZ.printSchema()
print "dfSZ contains " + str(dfSZ.count()) + " records"

# COMMAND ----------

#creating one offline dataframe 
appended = dfAF.union(dfGL).union(dfMN).union(dfOR).union(dfSZ)
appended.count()


# COMMAND ----------

appended.write.saveAsTable("offlinetrx")

# COMMAND ----------

#creating tables for offline data
dfAF.write.saveAsTable("OfflineAF")
dfGL.write.saveAsTable("OfflineGL")
dfMN.write.saveAsTable("OfflineMN")
dfOR.write.saveAsTable("OfflineOR")
dfSZ.write.saveAsTable("OfflineSZ")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC -- select count(*) from offlinetrx
# MAGIC -- Result was: 186608763
# MAGIC 
# MAGIC -- select count(*) from offlinetrx where Produkt like 'AEC%'
# MAGIC -- Result was: 10740581
# MAGIC 
# MAGIC -- select count(*) from offlinetrx where Produkt like 'BET%'
# MAGIC -- Result was: 3971173
# MAGIC 
# MAGIC -- select count(*) from offlinetrx where Produkt like 'BT%'
# MAGIC -- Result was: 83945
# MAGIC 
# MAGIC -- select count(*) from offlinetrx where Produkt like 'VOLG%';
# MAGIC -- Result was: 29142338
# MAGIC 
# MAGIC select count(*) from offlinetrx where Produkt not like 'BT%' and Produkt not like 'AEC%' and Produkt not like 'BET%' and Produkt not like 'VOLG%'
# MAGIC -- 142670726 --> perfect result , no invalid trx types anymore

# COMMAND ----------

#creating an improved dataframe to drop the non relevant transactions (agrola stuff)
dfoffline = spark.sql("""select * from offlinetrx where Produkt not like 'BT%' and Produkt not like 'AEC%' and Produkt not like 'BET%' and Produkt not like 'VOLG%' and Produkt not like 'QSC%'  and Produkt not like 'MAT%'""");

#checking if the no. of lines is equivalent to the count result of the SQL above
#dfoffline.count()

# COMMAND ----------

#writing the improved - while cleaned dataset to offlinetrx table
#for some reason the .mode("overwrite") did not work, no time to investigate, let's approach a workaround
dfoffline.write.saveAsTable("offlineTrxCleaned")

# COMMAND ----------

# MAGIC %sql
# MAGIC --then delete the original table
# MAGIC DROP TABLE IF EXISTS offlinetrx
# MAGIC -- and finally rename the copy to original
# MAGIC ALTER TABLE offlinetrx2 RENAME TO offlinetrx

# COMMAND ----------

#read offline table
dfoffline = spark.table("offlinetrx")

#write a CSV which could be reused in KNIME locally.....
# this would theoretically write one file but it doesn't work due to a default driver limitation of 4 GByte.
#dfoffline.toPandas().to_parquet("offlineTransactions", header=True)

#writing a parquet file using compression algo snappy
dfoffline.write.option("compression","snappy").mode("overwrite").parquet("/mnt/landi/offline/offlineTransactions")

#writing CSV
#dfoffline.write.format("csv").option("header", "true").save("/mnt/landi/offline/offlineTransactions.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC --JOIN der offline & online sales auf ProduktNiveau, Darstellung als Chart wäre nette Option, evtl. GroupBy Datum und Produkt ?
# MAGIC 
# MAGIC SELECT 
# MAGIC   o.Lieferdatum, 
# MAGIC   o.Produkt, 
# MAGIC   t.Datum,
# MAGIC   o.Liefermenge * o.VerkaufspreisInkl as offlineTurnover,
# MAGIC   t.OrderQuantity * t.UnitPrice as onlineTurnover
# MAGIC FROM 
# MAGIC   offlinetrxCleaned o, 
# MAGIC   onlinetrx t 
# MAGIC WHERE 
# MAGIC   t.ProductId = o.`Lieferantenprodukt-Nummer`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summarizing the turnover by product, without the fuel- & oiltrading, VOLG stuff...
# MAGIC SELECT round(sum(Verkaufspreisinkl),2) as Umsatz, Produkt, first(Bezeichnung) as Bezeichnung FROM offlineTrxCleaned group by Produkt order by Umsatz desc

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import sum, col, round, first
from pyspark.sql.types import *

# using the offline dataframe without fuel, oil, Volg stuff... 
dfoffline = spark.table("offlineTrxCleaned")

# grouping by product, aggregating turnover, sort by turnover
offlineGroupedByProduct = dfoffline.groupBy("Produkt").agg(sum("Verkaufspreisinkl").alias("Umsatz"), first("Bezeichnung").alias("Bezeichnung")).sort(col("Umsatz").desc())
offlineGroupedByProduct.withColumn("Umsatz", offlineGroupedByProduct.Umsatz.cast("decimal"))

#rounding & displaying figures
offlineGroupedByProduct = offlineGroupedByProduct.withColumn("Umsatz", round("Umsatz", 0))
offlineGroupedByProduct.show()

# COMMAND ----------

from pyspark.sql.types import DecimalType

dfoffline = spark.table("offlineTrxCleaned")

#offline sales grouped by location of the "Landi store"
offlineGroupedByCity = dfoffline.groupBy("MandantName").agg(sum("VerkaufspreisInkl").alias("Umsatz")).sort(col("Umsatz").desc())
changedTypedf = offlineGroupedByCity.withColumn("Umsatz", offlineGroupedByCity["Umsatz"].cast("decimal"))
changedTypedf.show(30)

# COMMAND ----------

#offline sales grouped by date and products sold on this date
#this could be used for a prediction on product sales
offlineGroupedByProductSalesOnDate = dfoffline.groupBy("Lieferdatum", "Produkt").agg(sum("Liefermenge").alias("AnzahlVerkaufteArtikel"), first('Bezeichnung').alias("Bezeichnung")).sort(col("Lieferdatum").desc())
offlineGroupedByProductSalesOnDate.withColumn("AnzahlVerkaufteArtikel", round("AnzahlVerkaufteArtikel",2))
offlineGroupedByProductSalesOnDate.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- mit rolf diese story hier anschauen, wie können wir so grosse Umsätze haben mit Akonto und blabla Zahlungen
# MAGIC 
# MAGIC select to_date(Lieferdatum, 'dd.MM.yyyy') as Datum, sum(VerkaufspreisInkl) as Umsatz, sum(Liefermenge) as Anzahl, first(Bezeichnung) as Produkt from offlinetrxcleaned group by Lieferdatum, Produkt order by Umsatz desc

# COMMAND ----------


