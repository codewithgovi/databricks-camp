"""

"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# GOLD STREAMING VIEW on top of SALES_SILVER_VIEW (NOT SILVER TABLE)
@dlt.view(name = 'sales_gold_view')
def sales_gold_view():
    df_gold_sales = spark.readStream.table('sales_silver_view')
    return df_gold_sales



# CREATING FACT TABLE (WITH AUTO CDC)
dlt.create_streaming_table(name = 'fact_sales')

dlt.create_auto_cdc_flow(
    source = 'sales_gold_view',
    target = 'fact_sales',
    keys = ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 1
)






