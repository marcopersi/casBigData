# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC // association rule learning for ONLINE with FPGrowth from MLLib 
# MAGIC import org.apache.spark.rdd.RDD
# MAGIC import org.apache.spark.mllib.fpm.AssociationRules
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth
# MAGIC 
# MAGIC // loading data
# MAGIC val data = sc.textFile("/FileStore/tables/onlinePurchasedProducts.txt")
# MAGIC val onlineTrx: RDD[Array[String]] = data.map(s => s.trim.split(' '))
# MAGIC println("Read: " + onlineTrx.count() + " online baskets")
# MAGIC 
# MAGIC // checking how transactions look like
# MAGIC val dataframe = onlineTrx.toDF()
# MAGIC println("Schema of transactions looks like: ")
# MAGIC dataframe.printSchema()
# MAGIC 
# MAGIC println("Content of transactions looks like: ")
# MAGIC dataframe.show()
# MAGIC 
# MAGIC val fpg = new FPGrowth()
# MAGIC val model = fpg
# MAGIC   .setMinSupport(0.005)
# MAGIC   .setNumPartitions(1)
# MAGIC   .run(onlineTrx)
# MAGIC 
# MAGIC println("Found the following frequent item sets:")
# MAGIC model.freqItemsets.collect().foreach { itemset =>
# MAGIC   println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
# MAGIC }
# MAGIC 
# MAGIC println("Found the following association rules:")
# MAGIC model.generateAssociationRules(0.4).collect().foreach { rule =>
# MAGIC   println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
# MAGIC     s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
# MAGIC }

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# a python implementation to run the FPGrowth assocation rule learning
dfoffline = spark.table("offlinetrx")

products = dfoffline.groupby('Beleg').agg(F.collect_set('Produkt').alias('items'))
#products.show()
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.2, minConfidence=0.6)
model = fpGrowth.fit(products)

# Display frequent itemsets.
model.freqItemsets.show()

# Display generated association rules.
model.associationRules.show()

# transform examines the input items against all the association rules and summarize the
# consequents as prediction
model.transform(products).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // association rule learning for OFFLINE with FPGrowth from MLLib
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth
# MAGIC import org.apache.spark.rdd.RDD
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.mllib.fpm.PrefixSpan
# MAGIC import org.apache.spark.SparkContext
# MAGIC import org.apache.spark.api.java.JavaRDD
# MAGIC import java.lang.String
# MAGIC import org.apache.spark.api.java.function.FlatMapFunction
# MAGIC import java.util.Arrays
# MAGIC import java.util.Iterator
# MAGIC import org.apache.spark.mllib.linalg.Vectors
# MAGIC 
# MAGIC val dfoffline = spark.table("offlinetrx")
# MAGIC val products = dfoffline
# MAGIC   .groupBy("Beleg")
# MAGIC   .agg(
# MAGIC     collect_set("Produkt") as "items")
# MAGIC 
# MAGIC val columnProducts = products.select("items")
# MAGIC columnProducts.printSchema()
# MAGIC columnProducts.show()
# MAGIC 
# MAGIC /*
# MAGIC root
# MAGIC  |-- items: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC 
# MAGIC +--------------------+
# MAGIC |               items|
# MAGIC +--------------------+
# MAGIC |[19420.01, 46872.01]|
# MAGIC |[AEC003.01, AEC00...|
# MAGIC |  [BT102.01, BET103]|
# MAGIC */
# MAGIC 
# MAGIC // convert the array in one concatenated string
# MAGIC //val codesAsSingleString = columnProducts.withColumn("items", concat_ws(" ", $"items"))
# MAGIC 
# MAGIC val rdd = columnProducts.rdd
# MAGIC 
# MAGIC /*
# MAGIC codesAsSingleString.printSchema()
# MAGIC codesAsSingleString.show()
# MAGIC 
# MAGIC root
# MAGIC  |-- items: string (nullable = false)
# MAGIC +--------------------+
# MAGIC |               items|
# MAGIC +--------------------+
# MAGIC |   19420.01 46872.01|
# MAGIC |AEC003.01 AEC004....|
# MAGIC |     BT102.01 BET103|
# MAGIC */
# MAGIC 
# MAGIC /*
# MAGIC val rdd = codesAsSingleString.rdd.map( x => x(0)).collect()
# MAGIC rdd.foreach(println)
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC // val rdd = codesAsSingleString.map{row => row.getAs[Seq[String]]("items").toArray}
# MAGIC // val rdd = codesAsSingleString.map{x:Row => x.getAs[List](0)}
# MAGIC 
# MAGIC /* 
# MAGIC problem: 
# MAGIC   found   : org.apache.spark.api.java.JavaRDD[org.apache.spark.sql.Row]
# MAGIC   required: org.apache.spark.api.java.JavaRDD[Basket]
# MAGIC */
# MAGIC 
# MAGIC val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(6)
# MAGIC val model = fpg.run(rdd)
# MAGIC 
# MAGIC 
# MAGIC /* the prefix span section, prefix span takes an array of arrays
# MAGIC val prefixSpan = new PrefixSpan()
# MAGIC   .setMinSupport(0.2)
# MAGIC   .setMaxPatternLength(5)
# MAGIC 
# MAGIC val model = prefixSpan.run(rdd)
# MAGIC model.freqSequences.collect().foreach { freqSequence =>
# MAGIC   println(
# MAGIC     s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
# MAGIC       s" ${freqSequence.freq}")
# MAGIC }
# MAGIC 
# MAGIC model.freqItemsets.collect().foreach { itemset =>
# MAGIC   println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
# MAGIC }
# MAGIC 
# MAGIC val minConfidence = 0.8
# MAGIC model.generateAssociationRules(minConfidence).collect().foreach { rule =>
# MAGIC   println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
# MAGIC     s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
# MAGIC }
# MAGIC */

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.SparkContext
# MAGIC import org.apache.spark.mllib.fpm.FPGrowth
# MAGIC 
# MAGIC 
# MAGIC     val transactions = Seq(
# MAGIC       "r z h k p",
# MAGIC       "z y x w v u t s",
# MAGIC       "s x o n r",
# MAGIC       "x z y m t s q e",
# MAGIC       "z",
# MAGIC       "x z y r q t p")
# MAGIC       .map(_.split(" "))
# MAGIC     val rdd = sc.parallelize(transactions, 2).cache()
# MAGIC 
# MAGIC val dataframe = rdd.toDF()
# MAGIC println("Schema of transactions looks like: ")
# MAGIC dataframe.printSchema()
# MAGIC dataframe.show()
# MAGIC 
# MAGIC     val fpg = new FPGrowth()
# MAGIC     val model = fpg
# MAGIC       .setMinSupport(0.2)
# MAGIC       .setNumPartitions(1)
# MAGIC       .run(rdd)
# MAGIC 
# MAGIC     model.freqItemsets.collect().foreach { itemset =>
# MAGIC         println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
# MAGIC     }
# MAGIC 
# MAGIC   println("--------------------------------------------------------------------------------")
# MAGIC 
# MAGIC   model.generateAssociationRules(0.4).collect().foreach { rule =>
# MAGIC   println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
# MAGIC     s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
# MAGIC   }
