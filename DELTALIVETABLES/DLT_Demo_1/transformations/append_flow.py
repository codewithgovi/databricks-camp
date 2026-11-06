"""
Here we are trying to append/combine two streaming tables into 1 single table into 1 single streaming table which can takes care of performance incremental loading


Steps:
    1. First we need to create empty streaming table
    2. Second we need to append flow 1 table into empty streaming table
    3. Third we need to append flow 2 table into empty streaming table
"""

import dlt

 # CREATE EMPTY STREAMING TABLE
dlt.create_streaming_table(
    name = 'append_table'
)


# CREATING FLOW 1
@dlt.append_flow(
    target = 'append_table'
)
def flow_1():
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .load("/Volumes/databricksgovi/bronze/autoload_vol/flow1/")
    return df


# CREATING FLOW 2
@dlt.append_flow(
    target = 'append_table'
)
def flow_2():
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .load("/Volumes/databricksgovi/bronze/autoload_vol/flow2/")
    return df
