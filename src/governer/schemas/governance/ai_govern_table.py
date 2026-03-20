from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BooleanType

ai_govern_table = StructType(
    [
        StructField("record_id", StringType(), False, metadata={"comment": "Record id"}),
        StructField("run_id", StringType(), True, metadata={"comment": "Unique identifier for each govern run"}),
        StructField("table_name", StringType(), True, metadata={"comment": "Table name"}),
        StructField("govern_success", BooleanType(), True, metadata={"comment": "Govern run status"}),
        StructField("govern_error", StringType(), True, metadata={"comment": "Govern run error text"}),
        StructField("evaluation", FloatType(), True, metadata={"comment": "Probabitlity that generated value is good"}),
        StructField("sql", StringType(), True, metadata={"comment": "Generated SQL"}),
        StructField("status", StringType(), True, metadata={"comment": "Status of the govern record"}),
        StructField("timestamp", TimestampType(), False, metadata={"comment": "Time generated"}),
    ]
)
