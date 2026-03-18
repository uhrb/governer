from pyspark.sql.types import StructType, StructField, StringType, LongType

customers = StructType(
    [
        StructField("customer_id", LongType(), True, metadata={"comment": "Unique identifier for each customer"}),
        StructField("first_name", StringType(), True, metadata={"comment": "First name of the customer"}),
        StructField("last_name", StringType(), True, metadata={"comment": "Last name of the customer"}),
    ],
)
