"""
Silver layer is to perform all the transformations on top of the bronze layer.

here we are creating 1 streaming view and 1 streaming table

VIEW : It is to perform all the transformations / and It plays as source to the golden layer/STREAM TABLE to perform UPSERT operation 
TABLE : It is to store the final output

"""


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# CREATING SILVER SALES STREAMING VIEW 
@dlt.view(
    name = "sales_silver_view"
)
def sales_silver_view():
    df_sales = spark.readStream.table("sales_bronze")
    # Applying transformations
    df_sales = df_sales.withColumn("pricePerSale",round(col("total_amount")/col("quantity"),2))
    df_sales = df_sales.withColumn("processDate", current_timestamp())
    return df_sales

    
# CREATING SILVER SALES STREAMING TABLE (WITH UPSERT)
# If we want to perform UPSERT first we need EMPTY SREAMING TABLE

dlt.create_streaming_table(name = "sales_silver")

dlt.create_auto_cdc_flow(
    source = 'sales_silver_view',
    target = 'sales_silver',
    keys = ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 1
)




