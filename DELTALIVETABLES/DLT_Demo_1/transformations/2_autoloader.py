import dlt
from pyspark.sql.functions import *

# CREATE STREAMING TABLE [USING AUTOLOADER]

@dlt.table(
    name = 'autoloadvol_table'
)
def autoloadvol_table():
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .load("/Volumes/databricksgovi/bronze/autoload_vol/raw2/")
    return df


# CREATE  STREAMING TABLE
@dlt.table(
    name = 'autoloadvol_table_enr'
)
def autoloadvol_table_enr():
    df = spark.read.table("autoloadvol_table")
    df = df.withColumn("flag", lit("Yes"))
    return df





