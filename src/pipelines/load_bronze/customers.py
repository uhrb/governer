from pyspark import pipelines as dp
from governer.schemas.raw.customers import customers as raw_customers
from governer.schemas.bronze.customers import customers as bronze_customers


@dp.table(
    schema=bronze_customers,
    comment="Bronze table for customers data loaded from CSV files using Auto Loader",
    # tags are not supported
)
def customers():
    """
    Streaming table that incrementally loads customer CSV files using Auto Loader.
    Auto Loader monitors the source directory and processes new files automatically.
    """
    catalog = spark.conf.get("catalog")
    schema = spark.conf.get("schema")
    volume = spark.conf.get("volume")
    print(f"Loading customers from /Volumes/{catalog}/{schema}/{volume}/customers")  # Debugging statement
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(raw_customers)
        .load(f"/Volumes/{catalog}/{schema}/{volume}/customers/*.csv")
    )
