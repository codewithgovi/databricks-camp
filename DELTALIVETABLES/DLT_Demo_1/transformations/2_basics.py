
# CREATING A SAMPLE PIPELINE TO INTERCONNECT EACH OTHER

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = 'sales_stg'
)
def sales_stg():
    df = spark.readStream.option("skipChangeCommits", True)\
    .table("databricksgovi.silver.sales_enr")
    return df


# CREATE A MAT VIEW
@dlt.table(
    name = 'sales_enr'
)
def sales_trn():
    df = spark.read.table("sales_stg")
    df = df.withColumn("priceAfterDiscount",col("total_amount") - col("discount"))
    return df


# CREATE A MAT VIEW
@dlt.table(
    name = 'sales_cur'
)
def sales_cur():
    df = spark.read.table("sales_enr")
    return df


