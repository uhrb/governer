from pyspark.sql.types import DecimalType, StructType, StructField, StringType, TimestampType, LongType

payments = StructType(
    [
        # payment_id,order_id,payment_method,payment_status,payment_amount,created
        StructField("payment_id", LongType(), True, metadata={"comment": "Unique identifier for each payment"}),
        StructField(
            "order_id",
            LongType(),
            True,
            metadata={"comment": "Identifier for the order to which the payment belongs"},
        ),
        StructField("payment_method", StringType(), True, metadata={"comment": "Method used for the payment"}),
        StructField("payment_status", StringType(), True, metadata={"comment": "Current status of the payment"}),
        StructField("payment_amount", DecimalType(10, 2), True, metadata={"comment": "Amount of the payment"}),
        StructField("created", TimestampType(), True, metadata={"comment": "Timestamp when the payment was created"}),
    ]
)
