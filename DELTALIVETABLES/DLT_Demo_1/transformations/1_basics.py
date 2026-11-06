
"""

import dlt

# CREATE A STREAMING TABLE
# Used for Streaming
@dlt.table(
    name = 'demo_stream_table'
)
def delta_stream_table():
    df = spark.readStream.table("databricksgovi.silver.sales_enr")
    return df


# CREATE A MATERIALIZED VIEW
# Used for batch queries and loading into downstream systems
@dlt.table(
    name = 'demo_mat_view'
)
def demo_mat_view():
    df = spark.read.table("databricksgovi.silver.sales_enr")
    return df

# CREATE TEMPORARY VIEW - for BATCH
@dlt.view(
    name = 'demo_temp_view'
)
def demo_temp_view():
    df = spark.read.table("databricksgovi.silver.sales_enr")
    return df

# CREATE TEMPORARY VIEW - for STREAM
@dlt.view(
    name = 'demo_stream_view'
)
def demo_stream_view():
    df = spark.readStream.table("databricksgovi.silver.sales_enr")
    return df

"""
