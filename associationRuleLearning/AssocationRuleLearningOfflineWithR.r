# Databricks notebook source
# loading libraries
library(SparkR)
library(dplyr)

# read CSV to create a dataframe
dfOfflineOR =read.df("/mnt/landi/offline/Offline_O-R.csv", source="csv", header="true", inferSchema="true")


# COMMAND ----------

# debugg/printing the first 10 records from the dataframe named dfOfflineOR
head(dfOfflineOR, n=10)

# COMMAND ----------

# reading all the CSV creating appropriate data frames
dfOfflineAF <- read.df("/mnt/landi/offline/Offline_A-F.csv", source="csv", header="true", inferSchema="true")
dfOfflineGL <- read.df("/mnt/landi/offline/Offline_G-L.csv", source="csv", header="true", inferSchema="true")
dfOfflineMN <- read.df("/mnt/landi/offline/Offline_M-N.csv", source="csv", header="true", inferSchema="true")
dfOfflineOR <- read.df("/mnt/landi/offline/Offline_O-R.csv", source="csv", header="true", inferSchema="true")
dfOfflineSZ <- read.df("/mnt/landi/offline/Offline_S-Z.csv", source="csv", header="true", inferSchema="true")

# COMMAND ----------

dfOfflineAF$Lieferantenprodukt-Nummer <- toString(dfOfflineAF$Lieferantenprodukt-Nummer)
#dfOfflineAF <- withColumn(dfOfflineAF, "Lieferantenprodukt-Nummer", cast(dfOfflineAF$Lieferantenprodukt-Nummer, "string"))

# COMMAND ----------

printSchema(dfOfflineAF)
printSchema(dfOfflineGL)
printSchema(dfOfflineMN)
printSchema(dfOfflineOR)
printSchema(dfOfflineSZ)

# COMMAND ----------

# Union the dataframes to one offline dataframe
firstPair <- rbind(dfOfflineAF,dfOfflineGL)
secondPair <- rbind(dfOfflineMN, dfOfflineOR)
nextMerge <- rbind(firstPair, dfOfflineSZ)

# the whole offline dataframe
total <- rbind(secondPair,nextMerge)

# COMMAND ----------

# debugg, printing the first 100 records from the new offline dataframe
head(total,n=100)

# COMMAND ----------

# persisting the dataframe, so that next time can be started by reading the table
saveAsTable(total, "offlinetrx", source = NULL, mode = "overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC --testing SQL access to the persisted table
# MAGIC SELECT * FROM offlinetrxcleaned where Produkt like 'BT%'

# COMMAND ----------

# Installing latest version of Rcpp
install.packages("Rcpp")
# Installing sparklyr takes a few minutes, becauses it installs +10 dependencies.
install.packages("sparklyr")

# COMMAND ----------

#https://longhowlam.wordpress.com/2017/11/23/association-rules-using-fpgrowth-in-spark-mllib-through-sparklyr/

# Load sparklyr package.
library(SparkR)
library(sparklyr)
library(magrittr)

# filtering transactions from fuel/oil trading (AGrola) as well as agricultural /wholesale transactions (UFA/Melior) which are not in scope for assocation rules learning
improved <- sql("SELECT * FROM offlinetrxcleaned")

# data needs to be aggregated by id, the items need to be in a list
trx_agg = improved %>% 
   group_by(improved$Beleg) %>% 
   summarize(
      products = collect_list(improved$Produkt)
   )

xyz = select(trx_agg, alias(trx_agg$products, "ProductList"))
display(xyz)

# debugging
#head(trx_agg, 10)
#display(trx_agg)

# preparing the FPGrowth input
ml_fpgrowth = function(
  x, 
  features_col = "products",
  support = 0.002,
  confidence = 0.01
)
{
  ensure_scalar_character(features_col)
  ensure_scalar_double(support)
  ensure_scalar_double(confidence)
  
  uid = sparklyr:::random_string("fpgrowth_")
  jobj = invoke_new(sc, "org.apache.spark.ml.fpm.FPGrowth", uid) 
  
  jobj %>% 
    invoke("setItemsCol", features_col ) %>%
    invoke("setMinConfidence", confidence) %>%
    invoke("setMinSupport", support)  %>%
    invoke("fit", spark_dataframe(x))

##### extract rules
# The nasty thing is that antecedent (LHS) and consequent (RHS) are lists
# We can split them and collect them to R

ml_fpgrowth_extract_rules = function(FPGmodel, nLHS = 2, nRHS = 1)
{
  rules = FPGmodel %>% invoke("associationRules")
  sdf_register(rules, "rules")
  
  exprs1 <- lapply(
    0:(nLHS - 1), 
    function(i) paste("CAST(antecedent[", i, "] AS string) AS LHSitem", i, sep="")
  )
  exprs2 <- lapply(
    0:(nRHS - 1), 
    function(i) paste("CAST(consequent[", i, "] AS string) AS RHSitem", i, sep="")
  )
  
  splittedLHS = rules %>% invoke("selectExpr", exprs1) 
  splittedRHS = rules %>% invoke("selectExpr", exprs2) 
  p1 = sdf_register(splittedLHS, "tmp1")
  p2 = sdf_register(splittedRHS, "tmp2")
  
  ## collecting output rules to R should be OK and not flooding R
  bind_cols(
    sdf_bind_cols(p1, p2) %>% collect(),
    rules %>% collect() %>% select(confidence)
  )
}

#### Plot resulting rules in a networkgraph
plot_rules = function(rules, LHS = "LHSitem0", RHS = "RHSitem0", cf = 0.2)
{
  rules = rules %>% filter(confidence > cf)
  nds = unique(
    c(
      rules[,LHS][[1]],
      rules[,RHS][[1]]
    )
  )
  
  nodes = data.frame(id = nds, label = nds, title = nds) %>% arrange(id)
  
  edges = data.frame(
    from =  rules[,LHS][[1]],
    to = rules[,RHS][[1]]
  )
  visNetwork(nodes, edges, main = "Groceries network", size=1) %>%
    visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
    visEdges(smooth = FALSE) %>%
    visPhysics(
      solver = "barnesHut", 
      forceAtlas2Based = list(gravitationalConstant = -20, maxVelocity = 1)
    )
}

###### example calls ##########################################################
FPGmodel = ml_fpgrowth(trx_agg, "products", support = 0.02, confidence = 0.05)

GroceryRules =  ml_fpgrowth(
  trx_agg
) %>%
  ml_fpgrowth_extract_rules()

plot_rules(GroceryRules)
  }

# COMMAND ----------

install.packages("SparkR")
install.packages("sparklyr")
install.packages("magrittr")

# COMMAND ----------

library(SparkR)
#library(sparklyr)
library(magrittr)

# filtering transactions from fuel/oil trading (AGrola) as well as agricultural /wholesale transactions (UFA/Melior) which are not in scope for assocation rules learning
dataframe <- sql("SELECT Beleg, Produkt FROM offlinetrxcleaned")
display(dataframe)

trx_agg = improved %>% 
   group_by(improved$Beleg) %>% 
   summarize(
      products = collect_list(improved$Produkt)
   )

# COMMAND ----------

library(SparkR)
#library(sparklyr)
library(magrittr)

aggr = dataframe %>%
  group_by(dataframe$Beleg) %>%
  summarize(
    products = concat_ws(sep=' ', first(dataframe$Produkt))
  )

display(aggr)

# COMMAND ----------


