from pyspark.sql.types import StructType, StructField, StringType, LongType

customers = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
    ]
)
