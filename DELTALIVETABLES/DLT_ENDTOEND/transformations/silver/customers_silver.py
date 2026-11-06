"""
Silver layer is to perform all the transformations on top of the bronze layer.

here we are creating 1 streaming view and 1 streaming table

VIEW : It is to perform all the transformations / and It plays as source to the golden layer/STREAM TABLE to perform UPSERT operation 
TABLE : It is to store the final output

"""


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# CREATING SILVER CUSTOMERS STREAMING VIEW 
@dlt.view(
    name = "customers_silver_view"
)
def customers_silver_view():
    df_customers = spark.readStream.table("customers_bronze")
    # Applying transformations
    df_customers = df_customers.withColumn("processDate", current_timestamp())
    df_customers = df_customers.withColumn("domain", split(col("email"),"@")[1])
    df_customers = df_customers.withColumn("name", upper(col("name")))
    return df_customers

    
# CREATING SILVER CUSTOMERS STREAMING TABLE (WITH UPSERT)
# If we want to perform UPSERT first we need EMPTY SREAMING TABLE

dlt.create_streaming_table(name = "customers_silver")

dlt.create_auto_cdc_flow(
    source = 'customers_silver_view',
    target = 'customers_silver',
    keys = ['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 1
)




