# Databricks notebook source
# MAGIC %run "./Squad_common_function"

# COMMAND ----------

# Initialize variables
source_files = {

    ('customer_behaviour', '/mnt/landing_zone/,customer_behaviour', 'csv', 'full', None) 
    } 

target_dir = '/mnt/wm/Squad_1/dim_customer_behaviour/' 
target_file_loc = None
silver_target_table = 'customer_behaviour' 
gold_target_table = 'dim_customer_behaviour' 
key_columns = ['customer_id', 'website_visits'] 
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
# MAGIC customer_id, 
# MAGIC order_frequency,
# MAGIC average_order_value, 
# MAGIC customer_lifetime_value,
# MAGIC website_visits,
# MAGIC seconds_spent_on_website,
# MAGIC page_views,
# MAGIC cart_abandonment_rate
# MAGIC from 
# MAGIC customer_behaviour 
# MAGIC where customer_id is not null

# COMMAND ----------

df= spark.sql('''select * from transformed_data''')
display(df)

# COMMAND ----------

df.write.format('delta').save('/mnt/landing_zone/squad_1/silver/stg_customer_behaviour')

# COMMAND ----------

dbutils.fs.ls('/mnt/landing_zone/squad_1/silver/stg_customer_behaviour')

# COMMAND ----------

# Define the file path for the gold path
gold_path = '/mnt/landing_zone/squad_1/gold/dim_customer_behaviour'

# Write the DataFrame in CSV format to the gold path
df.write \
    .format('csv') \
    .mode('overwrite') \
    .save(gold_path)
