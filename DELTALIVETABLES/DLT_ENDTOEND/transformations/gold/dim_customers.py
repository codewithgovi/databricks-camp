"""

"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# GOLD STREAMING VIEW on top of PRODUCTS_SILVER_VIEW (NOT SILVER TABLE)
@dlt.view(name = 'customers_gold_view')
def customers_gold_view():
    df_gold_customers = spark.readStream.table("customers_silver_view")
    return df_gold_customers



# CREATING DIMENSION SCD TYPE-2 TABLE (WITH AUTO CDC)
dlt.create_streaming_table(name = 'dim_customers')

dlt.create_auto_cdc_flow(
    source = 'customers_gold_view',
    target = 'dim_customers',
    keys = ['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)






