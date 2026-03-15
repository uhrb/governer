from pyspark import pipelines as dp
from governer.schemas.raw.payments import payments as raw_payments
from governer.schemas.bronze.payments import payments as bronze_payments


@dp.table(
    schema=bronze_payments,
    comment="Bronze table for payments data loaded from CSV files using Auto Loader",
    # tags are not supported
)
def payments():
    """
    Streaming table that incrementally loads payment CSV files using Auto Loader.
    Auto Loader monitors the source directory and processes new files automatically.
    """
    catalog = spark.conf.get("catalog")
    schema = spark.conf.get("schema")
    volume = spark.conf.get("volume")
    print(f"Loading payments from /Volumes/{catalog}/{schema}/{volume}/payments")  # Debugging statement
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(raw_payments)
        .load(f"/Volumes/{catalog}/{schema}/{volume}/payments/*.csv")
    )
