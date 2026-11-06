"""
Expectation are the set of rules/checks for the data.
                        
                            PASS------> WE CAN LAOD TO ---> Target/Success
Source ---> Expectations ====|
                            FAIL------> WE CAN --> WARN/DROP/FAIL/ 

@dlt.expect_all/@dlt.expect_all_WARN --> both are same
@dlt.expect_all_drop --> Drop the data
@dlt.expect_all_fail --> Fail the pipeline

"""
# IMPLEMENTATION BELOW

import dlt

table_name = spark.conf.get("table_name")

expectations = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "category IS NOT NULL"
}


@dlt.table(
    name = "expect_table"
)
@dlt.expect_all_or_drop(expectations)
def expect_table():
    return spark.read.table(f"databricksgovi.silver.{table_name}")
    return df







