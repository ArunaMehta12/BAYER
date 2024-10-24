# Databricks notebook source
display(dbutils.fs.ls('/mnt/landing_zone'))

# COMMAND ----------


# IMPORT LIBRARIES...
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


#define path
#filelocation= /mnt/landing_zone/customer.csv
df = spark.read.csv("/mnt/landing_zone/customer.csv", header=True, inferSchema=True)
#df1 = df.write.format('csv').load('/bayerstorage/Squad_1/Bronze_customer.csv')
df.display()
#loas data into bronze layer
bronze_path= "abfss://bayerstorage@bayershackadls.blob.core.windows.net/bronze/customerfile"
loaded_df= df.write.mode("overwrite").save(bronze_path)

# Move Customer data from bronze to silver
# Remove rows with blank phone numbers in customer file
customer_df_clean = customer_df.filter(col("phone").isNotNull())
customer_df_clean.printSchema()

# Remove rows in order file for customers with blank phone numbers
order_df_clean = order_df.join(customer_df_clean, "customer_id", "inner").select("customer_id", "order_id", "order_date", "order_channel", "store_code", "total_purchase_value", order_df["state"].alias("order_state"), "order_country");
#print("Order DF Clean......")

#scd


# Define the columns 
key_columns = ["customer_id"]
attribute_columns = ["first_name", "last_name", "email", "gender", "address_line_1", "address_line_2","city", "state", "country", "zipcode", "phone"]

# Identify the records that have changed
changed_records = customer_df_clean.alias("old").join(
    updated_df.alias("new"),
    on=key_columns,  # Explicitly specify the join column as a list
    how="inner"
).filter(
    (col("old.first_name") != col("new.first_name")) |
    (col("old.last_name") != col("new.last_name")) |
    (col("old.email") != col("new.email")) |
    (col("old.gender") != col("new.gender")) |
    (col("old.address_line_1") != col("new.address_line_1")) |
    (col("old.address_line_2") != col("new.address_line_2")) |
    (col("old.city") != col("new.city")) |
    (col("old.state") != col("new.state")) |
    (col("old.country") != col("new.country")) |
    (col("old.zipcode") != col("new.zipcode")) |
    (col("old.phone") != col("new.phone"))
).select("old.*")

# Mark records as inactive
inactive_df = changed_records.withColumn("last_updated_date", current_date()).withColumn("active", lit("FALSE"))

# Combine the DataFrames
customer_scd2_df = updated_df.union(inactive_df)

#Move Custome Data from Silver to gold

final_customer_data = customer_df.uion(customer_scd2_df ).
                        union(Order_data).union(Order_line).
                        union(Order_line)
        final_customer_data.display



