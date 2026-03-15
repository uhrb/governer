from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

orders = StructType(
    [
        StructField("order_id", LongType(), True, metadata={"comment": "Unique identifier for each order"}),
        StructField(
            "customer_id",
            LongType(),
            True,
            metadata={"comment": "Identifier for the customer who placed the order"},
        ),
        StructField("order_date", TimestampType(), True, metadata={"comment": "Date when the order was placed"}),
        StructField("order_status", StringType(), True, metadata={"comment": "Current status of the order"}),
    ]
)
