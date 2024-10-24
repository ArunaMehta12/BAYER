# Databricks notebook source
# MAGIC %run "./Squad_common_function"

# COMMAND ----------

# Initialize variables
source_files = {

    ('dim_customer_old', '/mnt/landing_zone/customer', 'csv', 'full', None), 
    ('dim_customer_new', '/mnt/landing_zone/customer_SCD2_data', 'csv', 'full', None)
    } 

target_dir = '/mnt/wm/Squad_1/dim_customer/' 
silver_target_table = 'dim_customer' 
gold_target_table = 'dim_dim_customer' 
key_columns = ['customer_id'] 
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

df_customer_old = spark.sql('''
                   select 
                   customer_id, 
                    first_name, 
                    last_name, 
                    email, 
                    gender, 
                    Address, 
                    city, 
                    state, 
                    country, 
                    zipcode, 
                    phone, 
                    date_format(to_timestamp(Created_date, 'M/d/yyyy H:mm'), 'yyyy-MM-dd') AS Created_date, 
                    date_format(to_timestamp(last_updated_date, 'M/d/yyyy H:mm'), 'yyyy-MM-dd') AS last_updated_date, 
                    active from dim_customer_old''')
df_customer_new = spark.sql('''
                            select 
                            customer_id, 
                            first_name, 
                            last_name, 
                            email, 
                            gender, 
                            Address, 
                            city, 
                            state, 
                            country, 
                            zipcode, 
                            phone, 
                            date_format(to_timestamp(Created_date, 'M/d/yyyy H:mm'), 'yyyy-MM-dd') AS Created_date from dim_customer_new''')

# COMMAND ----------

display(df_customer_old)
display(df_customer_new)

# COMMAND ----------

df_customer_new1 = df_customer_new.withColumn("active", lit("TRUE")).withColumn("last_updated_date",  lit("9999")).withColumn("Created_date", current_date())

# COMMAND ----------

df_customer_new1.display()

# COMMAND ----------

#Check the changes records
update_df = df_customer_old.join(df_customer_new1, "customer_id", "inner")\
                                .filter(hash(df_customer_old.Address, df_customer_old.city,df_customer_old.email,df_customer_old.country)!=(hash(df_customer_new.Address, df_customer_new.city,df_customer_new.email,df_customer_new.country))).select(df_customer_old.customer_id, df_customer_old.first_name, df_customer_old.last_name, df_customer_old.email, df_customer_old.gender, df_customer_old.Address, df_customer_old.city, df_customer_old.state, df_customer_old.country, df_customer_old.zipcode, df_customer_old.phone, df_customer_old.Created_date,
                                df_customer_old.last_updated_date, df_customer_old.active)

# COMMAND ----------

display(update_df)

# COMMAND ----------

#No changes data
df_no_change = df_customer_old.join(df_customer_new1, df_customer_old.customer_id == df_customer_new.customer_id ,"left_anti")
df_no_change.display()

# COMMAND ----------

df_final = update_df.union(df_no_change)
print(df_final.count())

# COMMAND ----------

df_final.write.format('delta').save('/mnt/landing_zone/squad_1/silver/dim_customer')

# COMMAND ----------

# Define the file path for the gold path
gold_path = '/mnt/landing_zone/squad_1/gold/dim_customer'

# Write the DataFrame in CSV format to the gold path
df_final.write \
    .format('csv') \
    .mode('overwrite') \
    .save(gold_path)
