"""

"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# GOLD STREAMING VIEW on top of STORES_SILVER_VIEW (NOT SILVER TABLE)
@dlt.view(name = 'stores_gold_view')
def stores_gold_view():
    df_gold_stores = spark.readStream.table('stores_silver_view')
    return df_gold_stores



# CREATING DIMENSION SCD TYPE-2 TABLE (WITH AUTO CDC)
dlt.create_streaming_table(name = 'dim_stores')

dlt.create_auto_cdc_flow(
    source = 'stores_gold_view',
    target = 'dim_stores',
    keys = ['store_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)






