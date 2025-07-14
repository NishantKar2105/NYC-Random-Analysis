# Databricks notebook source
## Reading our dataset
df =spark.read.option("multiLine",True).json("/FileStore/tables/random.json")

## Printing Schema to confirm presence of nested fields
df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import explode

''' We notice that the results fields has all our user records in a single row
to resolve this we will use the explode function of separate rows'''

# Explode the 'results' array into individual rows
df_exp = df.select(explode("results").alias("person"))




# COMMAND ----------

def get_nested_fields(df):
    nested_fields=[]
    for field in df.schema.fields:
        if "StructType" in str(field.dataType) or "ArrayType" in str(field.dataType):
            nested_fields.append((field.name, field.dataType))
    return nested_fields


# COMMAND ----------

nested_fields=get_nested_fields(df)

# COMMAND ----------

from pyspark.sql.functions import col, explode_outer
from pyspark.sql import DataFrame

## # This recursive function flattens all nested struct and array-of-struct fields

def flatten_all(df: DataFrame) -> DataFrame:
    ## The output goes here
    flat_cols=[]
    nested_cols=[]

    ## Get all field types and field name
    for field in df.schema.fields:
        dtype=str(field.dataType)
        name= field.name

        ## Flatten all the struct type fields
        if "StructType" in dtype:   
            # Struct: extract subfields with prefix
            for subfield in df.select(f"{name}.*").columns:
                flat_cols.append(col(f"{name}.{subfield}").alias(f"{name}_{subfield}"))
        
        ## Flatten all the array of struct type fields
        elif "ArrayType" in dtype and "StructType" in dtype:
            # Explode array of structs
            df= df.withColumn(name, explode_outer(col(name)))
            for subfield in df.select(f"{name}.*").columns:
                flat_cols.append(col(f"{name}.{subfield}").alias(f"{name}_{subfield}"))
        else:
            flat_cols.append(col(name))             ## Atomic fields

    ## Building our atomic dataframe
    df_flat = df.select(*flat_cols)

    # Recursively flatten if nested fields still exist
    if any(("StructType" in str(f.dataType) or "ArrayType" in str(f.dataType)) for f in df_flat.schema.fields):
        return flatten_all(df_flat)

    return df_flat


# COMMAND ----------

## Calling funtion to flatten
df_flat = flatten_all(df_exp)

## Printing Schema of our flattened dataframe
df_flat.printSchema()

# COMMAND ----------

# Save as a Parquet file 
df_flat.write.mode("overwrite").parquet("/FileStore/tables/flattened_random_users.parquet")

# Register as an external Parquet table
spark.sql("DROP TABLE IF EXISTS random_users_flattened")

spark.sql("""
    CREATE TABLE random_users_flattened
    USING PARQUET
    LOCATION '/FileStore/tables/flattened_random_users.parquet'
""")


# COMMAND ----------

