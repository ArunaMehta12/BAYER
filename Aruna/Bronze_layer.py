# Databricks notebook source
display(dbutils.fs.ls('/mnt/landing_zone'))

# COMMAND ----------

#df = spark.read.csv("dbfs:/mnt/landing_zone/customer.csv", header=True, inferSchema=True)
#df_customer= df.write.option("ifNotExists", True).saveAsTable("CustomerAA")
df1= spark.sql("select * from CustomerAA")
CustomerAA.display()

