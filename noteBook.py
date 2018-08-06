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
dfSZ = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/offline/Offline_S-Z.csv")

# COMMAND ----------

#creating tables for offline data
dfAF.write.saveAsTable("OfflineAF")
dfGL.write.saveAsTable("OfflineGL")
dfMN.write.saveAsTable("OfflineMN")
df.write.saveAsTable("OfflineOR")
dfSZ.write.saveAsTable("OfflineSZ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from offlineaf

# COMMAND ----------

#online data
dfonline = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("/mnt/landi/online/online.csv")

# COMMAND ----------

dfonline.printSchema()

# COMMAND ----------

dfonline.write.saveAsTable("onlineTrx")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from onlinetrx

# COMMAND ----------

# MAGIC %sql
# MAGIC --JOIN der offline & online sales auf ProduktNiveau, aber verbesserungswürdig, da aus offlinewelt nur 1 dataframe (offline g-l)
# MAGIC -- TODO: über alle offline dataframes resultat auswerten !
# MAGIC SELECT 
# MAGIC   o.Lieferdatum, 
# MAGIC   o.Produkt, 
# MAGIC   o.VerkaufspreisInkl,
# MAGIC   o.Liefermenge,
# MAGIC   t.Datum,
# MAGIC   t.OrderQuantity as onlineOrderQuantity, 
# MAGIC   t.Totalbetrag as onlineTotalAmount
# MAGIC FROM 
# MAGIC   offlinegl o, 
# MAGIC   onlinetrx t 
# MAGIC WHERE 
# MAGIC   t.ProductId = o.`Lieferantenprodukt-Nummer`

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import sum, col

# ---- attention -- attention -- attention
# improve this implementation, at least in two cases:
# 1. some of the products (for example 46709) show a turnover('Umsatz') of 3.6141SE7 which must be wrong.
# 2. if once improved, make a crosssjoin o all offline dataframes and find the product with the best turnover in the offline world.
# 3. then make another crossjoin with the online world, crossjoin of online & offline and identfy the product with the best turnover in both worlds
# 4. an optional last step could be to show the products turnover on a timeline / in which months are the best turnovers achieved ?
# -----------------------------------------

aGroupedDF = dfGL.groupBy("Produkt").agg(sum("Verkaufspreisinkl").alias("Umsatz")).sort(col("Umsatz").desc())
aGroupedDF.show()

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
# MAGIC val minConfidence = 0.2
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
# MAGIC val ar = new AssociationRules().setMinConfidence(0.8)
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
# MAGIC   .setMinSupport(0.5)
# MAGIC   .setMaxPatternLength(5)
# MAGIC 
# MAGIC val model = prefixSpan.run(sequences)
# MAGIC 
# MAGIC model.freqSequences.collect().foreach { freqSequence =>
# MAGIC   println(
# MAGIC     s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
# MAGIC       s" ${freqSequence.freq}")
# MAGIC }

# COMMAND ----------


