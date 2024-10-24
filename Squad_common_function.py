# Databricks notebook source
# Import modules
import os
import re
import sys
import datetime
import json
import requests
import hashlib
import inspect
import glob
import pandas as pd
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *
from time import sleep
from dateutil.relativedelta import relativedelta

# COMMAND ----------

def create_dataframe_and_view(source_files): 
    current_timestamp = datetime.datetime.now() 
    try: 
        for f in source_files: 
            df = spark.read.csv(
                f'{f[1]}.{f[2]}', 
                header=True, 
                inferSchema=True
            ) 
            print(df) 
            print(f'{current_timestamp}: Data_frame for {f[0]} created') 
            print(f'{current_timestamp}: Total records of {f[0]}: {df.count()}') 
            df.createOrReplaceTempView(f'{f[0]}') 
            print(f'{current_timestamp}: {f[0]} Temp view has been created') 
    except Exception as e: 
        print(f'[{current_timestamp}] Error: {e}') 
        print(f'[{current_timestamp}] Error Type: {sys.exc_info()[0]}') 
        print(f'[{current_timestamp}] Error Details: {sys.exc_info()[1]}') 
        raise
