# Databricks notebook source
from pyspark.sql.functions import col, round, avg

# COMMAND ----------

## Load the Dataset

df= spark.read.parquet("/FileStore/yellow_tripdata_2018_01.parquet", header=True, inferSchema=True)
display(df)

# COMMAND ----------

for column , col_type in df.dtypes:
    print(f"Column: {column} Type: {col_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Add a column named "Revenue" which is the sum of:Fare_amount + Extra + MTA_tax + Improvement_surcharge + Tip_amount + Tolls_amount + Total_amount

# COMMAND ----------


## Compute the revenue column

df_revenue = df.withColumn(
    'Revenue',
    round(
        col('Fare_amount') + 
        col('Extra') + 
        col('MTA_Tax') + 
        col('Improvement_surcharge') + 
        col('Tip_amount') + 
        col('Tolls_amount') + 
        col('Total_amount'),
        2
    )
)
## Display the revenue column
df_revenue.select("Revenue").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Increasing count of total passengers in NYC by area

# COMMAND ----------

## Grouping by passenger count and aggregating them to find total number of passengers( for multi- cab)

df_passenger_count= df.groupBy('PULocationID')\
    .agg({'passenger_count': 'sum'})\
    .withColumnRenamed("sum(passenger_count)","Total_Passengers")\
    .orderBy('Total_Passengers')
df_passenger_count.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Query 3: Realtime Average fare / total earning amount earned by 2 vendor

# COMMAND ----------

df.groupBy("VendorID").agg(
    avg("fare_amount").alias("Avg_Fare"),
    avg("total_amount").alias("Avg_Total_Earnings")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Query 4: Moving count of payments made by each payment mode

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import count, col

window_spec= Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime").rowsBetween(-10, 0)

df.withColumn("Moving_Count",count("payment_type").over(window_spec)) \
  .select("tpep_pickup_datetime","payment_type","Moving_Count") \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Query 5: Highest two gaining vendors on a particular date with no. of passengers and total distance

# COMMAND ----------

from pyspark.sql.functions import to_date, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

df_day=df.withColumn("date", to_date("tpep_pickup_datetime"))

agg_df =df_day.groupBy("date", "VendorID")\
    .agg(
        sum("total_amount").alias("Total_Earning"),
        sum("passenger_count").alias("Total_Passengers"),
        sum("trip_distance").alias("Total_Distance")
    )

ranked= agg_df.withColumn("rank", rank().over(Window.partitionBy("date").orderBy(col("Total_Earning").desc())))

ranked.filter("rank <= 2").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 6: Most number of passengers between a route of two locations

# COMMAND ----------

df.groupBy("PULocationID", "DOLocationID") \
  .agg(sum("passenger_count").alias("Total_Passengers")) \
  .orderBy("Total_Passengers", ascending=False) \
  .show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Query 7: Get top pickup locations with most passengers in last 5 or 10 seconds
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, unix_timestamp,lit

# Simulate current time
current_time =df.selectExpr("max(tpep_pickup_datetime) as now").collect()[0]["now"]

# Filter last 10 seconds
recent_df =df.filter(
    (unix_timestamp(lit(current_time)) -unix_timestamp("tpep_pickup_datetime"))<=10
)

recent_df.groupBy("PULocationID") \
    .agg(sum("passenger_count").alias("Total_Passengers")) \
    .orderBy("Total_Passengers", ascending=False) \
    .show()


# COMMAND ----------

