"""
Here we are Implementing the SCD logic of TYPE-1 called UPSERT 

"""


# =================== SCD TYPE 1 =========================

import dlt


# STAGING TABLE
@dlt.table(
    name = "scd_stg"
)
def scd1_stg():
    df = spark.readStream.table("databricksgovi.bronze.source")
    return df

# CREATE AN EMPTY STREAMING TABLE
dlt.create_streaming_table(
    name = 'scd1_table'
)



dlt.create_auto_cdc_flow(
    source = "scd_stg",
    target = "scd1_table",
    keys = ["product_id"],
    sequence_by = "processDate",
    stored_as_scd_type = 1
)


# =================== SCD TYPE 2 =========================

# CREATE AN EMPTY STREAMING TABLE
dlt.create_streaming_table(
    name = 'scd3_table'
)



dlt.create_auto_cdc_flow(
    source = "scd_stg",
    target = "scd3_table",
    keys = ["product_id"],
    sequence_by = "processDate",
    stored_as_scd_type = 2,
    except_column_list= ['processDate']
)


