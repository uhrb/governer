from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

orders = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("order_status", StringType(), True),
    ]
)
