from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

table_comment_actions = StructType(
    [
        StructField("id", StringType(), True, metadata={"comment": "Unique identifier for each govern run"}),
        StructField("table_name", StringType(), True, metadata={"comment": "Table name"}),
        StructField("status", StringType(), True, metadata={"comment": "Last name of the customer"}),
        StructField("sql", StringType(), True, metadata={"comment": "SQL to adjust"}),
        StructField("action", StringType(), True, metadata={"comment": "Action to execute"}),
        StructField("validity", FloatType(), True, metadata={"comment": "Validity of the model answer"}),
        StructField("timestamp", TimestampType(), False, metadata={"comment": "Time generated"}),
    ]
)
