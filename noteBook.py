# Databricks notebook source
 dbutils.fs.mount(
  source = "wasbs://landi@casbigdata.blob.core.windows.net/",
  mount_point = "/mnt/landi",
  extra_configs = {"fs.azure.account.key.casbigdata.blob.core.windows.net": "tqPINCBQrLmpTPX500LLF8xXMpPujPcR61fpe91xThFo21VGlxV6OcEmPkUyPM8h96OXZXwDyA+5UeabrCj5Ug=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/landi/online")

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

display(df.select("Lieferdatum", "VerkaufspreisInkl"))


# COMMAND ----------

for name, dtype in df.dtypes:
  print(name, dtype)

# COMMAND ----------

#creating data frames
dfAF = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_A-F.csv")
dfGL = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_G-L.csv")
dfMN = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_M-N.csv")
dfOR = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_O-R.csv")
dfSZ = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_S-Z.csv")


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
dfoffline.count()



# COMMAND ----------

#writing the improved - while cleaned dataset to offlinetrx table
#for some reason the .mode("overwrite") did not work, no time to investigate, let's approach a workaround
dfoffline.write.saveAsTable("offlinetrx2")

# COMMAND ----------

# MAGIC %sql
# MAGIC --then delete the original table
# MAGIC DROP TABLE IF EXISTS offlinetrx

# COMMAND ----------

# MAGIC %sql
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

#online data
dfonline = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/online/onlineTransactions.csv")

# COMMAND ----------

dfonline.printSchema()

# COMMAND ----------

dfonline.write.mode("overwrite").saveAsTable("onlineTrx")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from onlinetrx order by Datum desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --JOIN der offline & online sales auf ProduktNiveau, aber verbesserungswürdig
# MAGIC -- Todo: multiplikation der units * unitprice im online case, groupBy Produkt
# MAGIC 
# MAGIC SELECT 
# MAGIC   o.Lieferdatum, 
# MAGIC   o.Produkt, 
# MAGIC   o.VerkaufspreisInkl,
# MAGIC   o.Liefermenge,
# MAGIC   t.Datum,
# MAGIC   t.OrderQuantity as onlineOrderQuantity, 
# MAGIC   t.Totalbetrag as onlineTotalAmount
# MAGIC FROM 
# MAGIC   offlinetrx o, 
# MAGIC   onlinetrx t 
# MAGIC WHERE 
# MAGIC   t.ProductId = o.`Lieferantenprodukt-Nummer`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT sum(Verkaufspreisinkl) FROM offlinetrx group by Produkt 

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import sum, col
from pyspark.sql.types import *

dfoffline = spark.table("offlinetrx")
offlineGroupedByProduct = dfoffline.groupBy("Produkt").agg(sum("Verkaufspreisinkl").alias("Umsatz")).sort(col("Umsatz").desc())
offlineGroupedByProduct.withColumn("Umsatz", offlineGroupedByProduct.Umsatz.cast("decimal"))
offlineGroupedByProduct.take(30)

# COMMAND ----------

#offline sales grouped by city
offlineGroupedByCity = dfoffline.groupBy("MandantName").agg(sum("Verkaufspreisinkl").alias("Umsatz")).sort(col("Umsatz").desc())
offlineGroupedByCity.withColumn("Umsatz", offlineGroupedByCity.Umsatz.cast("decimal"))
offlineGroupedByCity.show(30)

# COMMAND ----------

#offline sales grouped by date and products sold on this date
offlineGroupedByProductSalesOnDate = dfoffline.groupBy("Lieferdatum", "Produkt").agg(sum("Liefermenge").alias("AnzahlVerkaufteArtikel")).sort(col("AnzahlVerkaufteArtikel").desc())
offlineGroupedByProductSalesOnDate.take(30)

# COMMAND ----------

from pyspark.sql.functions import *

dfonline = spark.table("onlinetrx")

#calc the product turnover per trx
dfProduktUmsatzOnline = dfonline.select(dfonline['ProductId'], dfonline['OrderQuantity'], dfonline['UnitPrice'],
   (dfonline['OrderQuantity'] * dfonline['UnitPrice']).alias('ProduktUmsatzPerEinkauf'))

#debbuging....
#dfProduktUmsatzOnline.show()

# aggregating by product
dfGroupedOnline = dfProduktUmsatzOnline.groupBy('ProductId').agg(sum('ProduktUmsatzPerEinkauf').alias('Umsatz')).sort(col("Umsatz").desc())
dfGroupedOnline.take(30)

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth

# a python test implementation to run the FPGrowth assocation rule learning
# can actually be deleted soon.

df = spark.createDataFrame([
    (0, [1, 2, 5]),
    (1, [1, 2, 3, 5]),
    (2, [1, 2])
], ["id", "items"])

fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
model = fpGrowth.fit(df)

# Display frequent itemsets.
model.freqItemsets.show()

# Display generated association rules.
model.associationRules.show()

# transform examines the input items against all the association rules and summarize the
# consequents as prediction
model.transform(df).show()

# COMMAND ----------

from pyspark.mllib.fpm import FPGrowth

data = sc.textFile("/FileStore/tables/onlinePurchasedProducts.txt")
data.collect()
transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)

# COMMAND ----------

# MAGIC %scala
# MAGIC // association rule learning with FPGrowth from MLLib
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth
# MAGIC import org.apache.spark.rdd.RDD
# MAGIC import org.apache.spark.mllib.fpm.AssociationRules
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
# MAGIC 
# MAGIC // loading data
# MAGIC val data = sc.textFile("/FileStore/tables/onlinePurchasedProducts.txt")
# MAGIC val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
# MAGIC 
# MAGIC // checking how transactions look like
# MAGIC transactions.toDF().show()
# MAGIC 
# MAGIC // initialize FPGrowth algo, minimum support means 2 out of 5 products matches will be recognized as a 'pattern'
# MAGIC // so if in 5 orders/shopping bags , there is two times a combination product "a" and product "b" this is a pattern.
# MAGIC // the higher the figure, the less frequent a pattern will be available. The lower the figure, the more patterns will be found and therefore 
# MAGIC // more false positives will be generated,.
# MAGIC val fpg = new FPGrowth()
# MAGIC   .setMinSupport(0.2)
# MAGIC   .setNumPartitions(5)
# MAGIC val model = fpg.run(transactions)
# MAGIC 
# MAGIC model.freqItemsets.collect().foreach { itemset =>
# MAGIC   println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
# MAGIC }
# MAGIC 
# MAGIC val minConfidence = 0.5
# MAGIC model.generateAssociationRules(minConfidence).collect().foreach { rule =>
# MAGIC   println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
# MAGIC     s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.mllib.fpm.AssociationRules
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
# MAGIC 
# MAGIC /* this is not implemented yet, is a second assocation rule learning implementation using another algo than FPGrowth */
# MAGIC // TODO finish this implementation
# MAGIC 
# MAGIC val freqItemsets = sc.parallelize(Seq(
# MAGIC   new FreqItemset(Array("a"), 15L),
# MAGIC   new FreqItemset(Array("b"), 35L),
# MAGIC   new FreqItemset(Array("a", "b"), 12L)
# MAGIC ))
# MAGIC 
# MAGIC val ar = new AssociationRules().setMinConfidence(0.2)
# MAGIC val results = ar.run(freqItemsets)
# MAGIC 
# MAGIC results.collect().foreach { rule =>
# MAGIC   println(s"[${rule.antecedent.mkString(",")}=>${rule.consequent.mkString(",")} ]" +s" ${rule.confidence}")
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.mllib.fpm.PrefixSpan
# MAGIC 
# MAGIC // finally the third algorithm, implementation is experimental / sample case, must update the data read and try & error the implementation
# MAGIC 
# MAGIC val sequences = sc.parallelize(Seq(
# MAGIC   Array(Array(1, 2), Array(3)),
# MAGIC   Array(Array(1), Array(3, 2), Array(1, 2)),
# MAGIC   Array(Array(1, 2), Array(5)),
# MAGIC   Array(Array(6))
# MAGIC ), 2).cache()
# MAGIC 
# MAGIC val prefixSpan = new PrefixSpan()
# MAGIC   .setMinSupport(0.2)
# MAGIC   .setMaxPatternLength(5)
# MAGIC 
# MAGIC val model = prefixSpan.run(sequences)
# MAGIC 
# MAGIC model.freqSequences.collect().foreach { freqSequence =>
# MAGIC   println(
# MAGIC     s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
# MAGIC       s" ${freqSequence.freq}")
# MAGIC }
