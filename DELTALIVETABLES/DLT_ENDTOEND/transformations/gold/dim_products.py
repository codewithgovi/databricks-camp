"""

"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# GOLD STREAMING VIEW on top of PRODUCTS_SILVER_VIEW (NOT SILVER TABLE)
@dlt.view(name = 'products_gold_view')
def products_gold_view():
    df_gold_products = spark.readStream.table('products_silver_view')
    return df_gold_products



# CREATING DIMENSION SCD TYPE-2 TABLE (WITH AUTO CDC)
dlt.create_streaming_table(name = 'dim_products')

dlt.create_auto_cdc_flow(
    source = 'products_gold_view',
    target = 'dim_products',
    keys = ['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)






