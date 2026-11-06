"""
Silver layer is to perform all the transformations on top of the bronze layer.

here we are creating 1 streaming view and 1 streaming table

VIEW : It is to perform all the transformations / and It plays as source to the golden layer/STREAM TABLE to perform UPSERT operation 
TABLE : It is to store the final output

"""


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# CREATING SILVER STORES STREAMING VIEW 
@dlt.view(
    name = "stores_silver_view"
)
def stores_silver_view():
    df_stores = spark.readStream.table("stores_bronze")
    # Applying transformations
    df_stores = df_stores.withColumn("store_name", regexp_replace(col("store_name"),"_",""))
    df_stores = df_stores.withColumn("processDate", current_timestamp())
    return df_stores

    
# CREATING SILVER STORES STREAMING TABLE (WITH UPSERT)
# If we want to perform UPSERT first we need EMPTY SREAMING TABLE

dlt.create_streaming_table(name = "stores_silver")

dlt.create_auto_cdc_flow(
    source = 'stores_silver_view',
    target = 'stores_silver',
    keys = ['store_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 1
)




