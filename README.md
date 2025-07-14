# 🔄 JSON Flattening and NYC Taxi Analysis Projects

This repository contains two sub-projects:

1. **JSON Flattening** – Load, flatten, and save nested JSON as a Parquet-backed table in Databricks.
2. **NYC Taxi Data Analysis** – Load and query NYC taxi data using PySpark DataFrame APIs.

---

## 📁 Project 1: JSON Flattening and Parquet Table Creation

### 📌 Objective

To load a nested JSON file into Databricks, recursively flatten all nested fields (structs and arrays), and save it as an external Parquet table.

---

### 🛠️ Steps Involved

1. **Read Nested JSON**
   - File location: `/FileStore/tables/random.json`
   - Use `multiLine=True` to read multi-line JSON properly.

2. **Explode JSON Array**
   - Exploded the `results` array to get one row per user.

3. **Flatten Nested Structs Recursively**
   - Used a custom recursive function `flatten_all()` to flatten all nested fields.
   - Nested field names were flattened with underscores (e.g., `location_coordinates_latitude`).

4. **Save as External Parquet Table**
   - File path: `/FileStore/tables/flattened_random_users.parquet`
   - Registered as table: `random_users_flattened`

---

### 💻 Sample Code

```python
# Read nested JSON
df = spark.read.option("multiLine", True).json("/FileStore/tables/random.json")

# Explode top-level array
df_exp = df.selectExpr("explode(results) as person")

# Flatten all nested structs and arrays
df_flat = flatten_all(df_exp)

# Save as Parquet
df_flat.write.mode("overwrite").parquet("/FileStore/tables/flattened_random_users.parquet")

# Register as table
spark.sql("DROP TABLE IF EXISTS random_users_flattened")
spark.sql("""
    CREATE TABLE random_users_flattened
    USING PARQUET
    LOCATION '/FileStore/tables/flattened_random_users.parquet'
""")
```

---

### 📊 Sample Query

```sql
SELECT person_name_first, person_location_city, person_dob_age
FROM random_users_flattened
LIMIT 5;
```

---

## 📁 Project 2: NYC Taxi Dataset – PySpark Queries

### 📌 Objective

To analyze NYC taxi trip data using PySpark by computing revenue metrics, vendor performance, route popularity, and temporal trends.

---

### 🧠 PySpark Queries

#### ✅ Query 1: Add Revenue Column

```python
df = df.withColumn("Revenue", (
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
))
```

---

#### ✅ Query 2: Total Passengers by Pickup Area

```python
df.groupBy("PULocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "Total_Passengers") \
  .orderBy("Total_Passengers", ascending=False)
```

---

#### ✅ Query 3: Average Fare and Total Amount by Vendor

```python
df.groupBy("VendorID") \
  .agg(
    {"fare_amount": "avg", "total_amount": "avg"}
  ) \
  .withColumnRenamed("avg(fare_amount)", "Avg_Fare") \
  .withColumnRenamed("avg(total_amount)", "Avg_Total")
```

---

#### ✅ Query 4: Count of Payments by Type

```python
df.groupBy("payment_type") \
  .count() \
  .orderBy("count", ascending=False)
```

---

#### ✅ Query 5: Top 2 Gaining Vendors by Date

```python
df.groupBy("VendorID", to_date("tpep_pickup_datetime").alias("Date")) \
  .agg(
    {"passenger_count": "sum", "trip_distance": "sum", "total_amount": "sum"}
  ) \
  .orderBy(col("sum(total_amount)").desc()) \
  .limit(2)
```

---

#### ✅ Query 6: Most Frequent Route by Passenger Count

```python
df.groupBy("PULocationID", "DOLocationID") \
  .sum("passenger_count") \
  .orderBy("sum(passenger_count)", ascending=False) \
  .limit(1)
```

---

#### ✅ Query 7: Top Pickup Locations in Last N Seconds

```python
seconds = 10
df_recent = df.filter(
    expr(f"tpep_pickup_datetime >= current_timestamp() - interval {seconds} seconds")
)

df_recent.groupBy("PULocationID") \
    .sum("passenger_count") \
    .orderBy("sum(passenger_count)", ascending=False)
```

---

## 🧰 Tech Stack

- **Platform**: Databricks Community Edition
- **Language**: PySpark (Apache Spark)
- **Storage**: DBFS (Databricks File System)
- **Output**: Parquet-backed Spark SQL table

---

