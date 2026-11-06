"""
Silver layer is to perform all the transformations on top of the bronze layer.

here we are creating 1 streaming view and 1 streaming table

VIEW : It is to perform all the transformations / and It plays as source to the golden layer/STREAM TABLE to perform UPSERT operation 
TABLE : It is to store the final output

"""


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# CREATING SILVER PRODUCTS STREAMING VIEW 
@dlt.view(
    name = "products_silver_view"
)
def products_silver_view():
    df_products = spark.readStream.table("products_bronze")
    # Applying transformations
    df_products = df_products.withColumn("processDate", current_timestamp())
    return df_products

    
# CREATING SILVER PRODUCTS STREAMING TABLE (WITH UPSERT)
# If we want to perform UPSERT first we need EMPTY SREAMING TABLE

dlt.create_streaming_table(name = "products_silver")

dlt.create_auto_cdc_flow(
    source = 'products_silver_view',
    target = 'products_silver',
    keys = ['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 1
)




