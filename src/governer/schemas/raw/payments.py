from pyspark.sql.types import DecimalType, StructType, StructField, StringType, TimestampType, LongType

payments = StructType(
    [
        # payment_id,order_id,payment_method,payment_status,payment_amount,created
        StructField("payment_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", DecimalType(10, 2), True),
        StructField("created", TimestampType(), True),
    ]
)
