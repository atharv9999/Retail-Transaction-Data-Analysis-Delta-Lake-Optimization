# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.atharvk9999storage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.atharvk9999storage.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.atharvk9999storage.dfs.core.windows.net", 
               "ClientID")  # client ID
spark.conf.set("fs.azure.account.oauth2.client.secret.atharvk9999storage.dfs.core.windows.net", 
               "ClientSecret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.atharvk9999storage.dfs.core.windows.net", 
               "https://login.microsoftonline.com/TenantID/oauth2/token")  # tenant ID

# COMMAND ----------

from pyspark.sql import functions as F

base_path = "abfss://data@atharvk9999storage.dfs.core.windows.net"

# Load CSVs
t_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/Transactions/transactions.csv")
p_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/Products/products.csv")
c_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/Customers/customers.csv")
l_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/Locations/locations.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC # Basic Analysis and Visualization on individual dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction DF
# MAGIC

# COMMAND ----------

t_df.show(10)

# COMMAND ----------

t_df.describe().show()

# COMMAND ----------

t_df.printSchema()

# COMMAND ----------

t_df = t_df.withColumnRenamed("locid","t_locid")
t_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, when
print(f"Total transactions: {t_df.count()}")
print("Null values")
t_df.select([count(when(col(c).isNull(), c)).alias(c) for c in t_df.columns]).show()

# COMMAND ----------

t_df.select("quantity", "amount").describe().show()   #summary statistics for quantity and amount

# COMMAND ----------

t_df.groupBy("payment_type").count().orderBy("count", ascending=False).show()   #distribution of payment types

# COMMAND ----------

t_df.groupBy("channel").count().orderBy("count", ascending=False).show() #Distribution of channels

# COMMAND ----------

t_df.groupBy("tdate").count().orderBy("tdate").display() #Transactions per day

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek

t_df_date = t_df.withColumn("year", year(col("tdate"))).withColumn("month", month(col("tdate"))).withColumn("dayofweek", dayofweek(col("tdate")))

#monthly transaction volume
t_df_date.groupBy("year", "month").agg(count("tid").alias("transactions")).orderBy("year", "month").show()

# COMMAND ----------

#revenue trend by month
t_df_date.groupBy("year", "month").agg(sum("amount").alias("monthly_revenue")).orderBy("year", "month").show()

# COMMAND ----------

#unique customers and products
t_df.agg(countDistinct("cid").alias("unique_customers"),countDistinct("pid").alias("unique_products")).show()

# COMMAND ----------

#Top 10 customers by spending
t_df.groupBy("cid").agg(sum("amount").alias("total_spent")).orderBy(col("total_spent").desc()).show(10)

# COMMAND ----------

# Top 10 products by total quantity
t_df.groupBy("pid").agg(sum("quantity").alias("total_quantity")).orderBy(col("total_quantity").desc()).show(10)

# COMMAND ----------

#Revenue trend
monthly_rev_df = t_df_date.groupBy("year", "month").agg(sum("amount").alias("revenue"))
display(monthly_rev_df)

# COMMAND ----------

#transactions per payment_type
payment_df = t_df.groupBy("payment_type").count()
display(payment_df)

# COMMAND ----------

#Channel distribution
channel_df = t_df.groupBy("channel").count()
display(channel_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer DF

# COMMAND ----------

c_df.show(10)

# COMMAND ----------

c_df = c_df.withColumnRenamed("locid", "c_locid")
c_df.show(10)

# COMMAND ----------

c_df.describe().show()

# COMMAND ----------

c_df.printSchema()

# COMMAND ----------

c_df.selectExpr("count(*) as TotalCustomer").show()

# COMMAND ----------

print("Null values")
c_df.select([count(when(col(c).isNull(), c)).alias(c) for c in c_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products DF

# COMMAND ----------

p_df.show(10)

# COMMAND ----------

p_df.printSchema()

# COMMAND ----------

p_df.describe().show()

# COMMAND ----------

p_df.selectExpr("count(pid) as TotalProducts").show()

# COMMAND ----------

print("Null values")
p_df.select([count(when(col(c).isNull(), c)).alias(c) for c in p_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locations DF

# COMMAND ----------

l_df.show(10)

# COMMAND ----------

l_df.printSchema()

# COMMAND ----------

l_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Join All Dataframes

# COMMAND ----------

final_df = (t_df.join(p_df, on="pid", how="left").join(c_df, on="cid", how="left").join(l_df, t_df["t_locid"] == l_df["locid"], how="left"))

final_df.cache() #cache the dataframe for faster access

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.filter(final_df["channel"] == "store").select(col("c_locid"),col("t_locid"), col("locid")).show()

# COMMAND ----------

final_df.filter(final_df["channel"] != "store" ).select(col("channel"),col("c_locid"),col("t_locid"), col("locid")).show()

# COMMAND ----------

final_df = final_df.drop("locid")
final_df = final_df.withColumnsRenamed({"city":"t_city", "state_region":"t_state_region", "country":"t_country"})
final_df.show(10)

# COMMAND ----------

final_df.display()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Delta Table

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").saveAsTable("silver.transactions_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver.transactions_enriched ZORDER BY (tid)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.transactions_enriched LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Bussiness Logic aggregates 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create DATABASE IF NOT EXISTS gold;

# COMMAND ----------

enriched_df = spark.table("silver.transactions_enriched")   #get joint data from delta table as Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Order Value per Customer

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

aov_df = (enriched_df.groupBy("cid", "cname").agg(F.count("tid").alias("total_orders"),F.sum("amount").alias("total_spent"),F.round(F.avg("amount"), 2).alias("avg_order_value")).orderBy(F.desc("avg_order_value")))

aov_df.display()

# COMMAND ----------

aov_df.write.format("delta").mode("overwrite").saveAsTable("gold.average_order_value_per_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most Popular Products

# COMMAND ----------

popular_products_df = (enriched_df.groupBy("pid", "pname").agg(F.sum("quantity").alias("total_quantity_sold"),F.countDistinct("cid").alias("unique_customers")).orderBy(F.desc("total_quantity_sold")))
popular_products_df.display()

# COMMAND ----------

popular_products_df.write.format("delta").mode("overwrite").saveAsTable("gold.popular_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Categories by Revenue

# COMMAND ----------

category_revenue_df = (enriched_df.groupBy("category").agg(F.sum("amount").alias("total_revenue"),F.count("tid").alias("total_transactions")).orderBy(F.desc("total_revenue")))

category_revenue_df.display()


# COMMAND ----------

category_revenue_df.write.format("delta").mode("overwrite").saveAsTable("gold.category_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Channel-wise Revenue and Order Count

# COMMAND ----------

channel_stats_df = (enriched_df.groupBy("channel").agg(F.sum("amount").alias("revenue"),F.count("tid").alias("transactions")).orderBy(F.desc("revenue")))

channel_stats_df.display()

# COMMAND ----------

channel_stats_df.write.format("delta").mode("overwrite").saveAsTable("gold.channel_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Country and City

# COMMAND ----------

geo_revenue_df = (enriched_df.groupBy("t_country", "t_city").agg(F.sum("amount").alias("total_revenue"),F.count("tid").alias("transaction_count")).orderBy(F.desc("total_revenue")))

geo_revenue_df.display()

# COMMAND ----------

geo_revenue_df.write.format("delta").mode("overwrite").saveAsTable("gold.geo_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue and Transactions by Country

# COMMAND ----------

country_revenue_df = (enriched_df.groupBy("t_country").agg(F.sum("amount").alias("total_revenue"),F.count("tid").alias("transaction_count")).orderBy(F.desc("total_revenue")))

country_revenue_df.display()

# COMMAND ----------

country_revenue_df.write.format("delta").mode("overwrite").saveAsTable("gold.country_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue Trend

# COMMAND ----------

monthly_revenue_df = (enriched_df.withColumn("month", F.date_format("tdate", "yyyy-MM")).groupBy("month").agg(F.sum("amount").alias("monthly_revenue"),F.count("tid").alias("transaction_count")).orderBy("month"))

monthly_revenue_df.display()

# COMMAND ----------

monthly_revenue_df.write.format("delta").mode("overwrite").saveAsTable("gold.monthly_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Yearly Revenue Trend

# COMMAND ----------

anual_revenue_df = (enriched_df.withColumn("year", F.date_format("tdate", "yyyy")).groupBy("year").agg(F.sum("amount").alias("anual_revenue"),F.count("tid").alias("transaction_count")).orderBy("year"))

anual_revenue_df.display()

# COMMAND ----------

anual_revenue_df.write.format("delta").mode("overwrite").saveAsTable("gold.anual_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Country and Channel wise Transactions

# COMMAND ----------

country_channel_insights = enriched_df.groupBy("t_country", "channel").agg(F.count("tid").alias("transaction_count"),F.round(F.sum("amount"), 2).alias("total_revenue"),F.round(F.avg("amount"), 2).alias("avg_order_value")).orderBy("t_country", "channel")

country_channel_insights.display()

# COMMAND ----------

country_channel_insights.write.format("delta").mode("overwrite").saveAsTable("gold.country_channel_insights")

# COMMAND ----------

