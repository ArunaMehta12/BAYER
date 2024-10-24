# Databricks notebook source
# MAGIC %run "./Squad_common_function"

# COMMAND ----------

# Initialize variables
source_files = {

    ('order', '/mnt/landing_zone/order', 'csv', 'full', None), 
    ('customer_old', '/mnt/landing_zone/customer', 'csv', 'full', None)
        } 
target_dir = '/mnt/wm/Squad_1/order/' 
silver_target_table = 'order' 
gold_target_table = 'fact_order' 
key_columns = ['cust_id'] 
sk_column = '' 
type_2_flag = False 
partition_column = None 
partition_value = None 

# COMMAND ----------

 # Create dataframe(s) and temporary view(s) 
 try:
     create_dataframe_and_view(source_files) 
except: (sys.exc_info(), 1) 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view transformed_data as 
# MAGIC select 
# MAGIC b.customer_id, 
# MAGIC a.order_id, 
# MAGIC a.order_date, 
# MAGIC a.order_channel, 
# MAGIC a.store_code, 
# MAGIC a.total_purchase_value,
# MAGIC a.state, 
# MAGIC a.order_country 
# MAGIC from fact_order a 
# MAGIC left join customer_old b 
# MAGIC on a.customer_id = b.customer_id
# MAGIC and b.customer_id is not null 
# MAGIC

# COMMAND ----------

df = spark.sql('''select * from transformed_data''')

# COMMAND ----------

df.write.format('delta').save('/mnt/landing_zone/squad_1/silver/order')

# COMMAND ----------

# Define the file path for the gold path
gold_path = '/mnt/landing_zone/squad_1/gold/fact_order'

# Write the DataFrame in CSV format to the gold path
df.write \
    .format('csv') \
    .mode('overwrite') \
    .save(gold_path)
