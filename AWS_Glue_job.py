#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()
source_file_path = f"s3://{bucket}/{fileName}"
destination_file_path = f"s3://cdc-s3-output-pyspark/output"

if "LOAD" in fileName:
    full_load_df = spark.read.csv(source_file_path)
    full_load_df = full_load_df.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    full_load_df.write.mode("overwrite").csv(destination_file_path)
else:
    updated_df = spark.read.csv(source_file_path)
    updated_df = updated_df.withColumnRenamed("_c0","action").withColumnRenamed("_c1","id").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    final_df = spark.read.csv(destination_file_path)
    final_df = final_df.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    for row in updated_df.collect(): 
      if row["action"] == 'U':
        final_df = final_df.withColumn("FullName", when(final_df["id"] == row["id"], row["FullName"]).otherwise(final_df["FullName"]))      
        final_df = final_df.withColumn("City", when(final_df["id"] == row["id"], row["City"]).otherwise(final_df["City"]))
    
      if row["action"] == 'I':
        insertedRow = [list(row)[1:]]
        columns = ['id', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        final_df = final_df.union(newdf)
    
      if row["action"] == 'D':
        final_df = final_df.filter(final_df.id != row["id"])
        
    final_df.write.mode("overwrite").csv(destination_file_path)   

