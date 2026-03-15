from pyspark import pipelines as dp
from governer.schemas.raw.orders import orders as raw_orders
from governer.schemas.bronze.orders import orders as bronze_orders


@dp.table(
    comment="Bronze table for orders data loaded from CSV files using Auto Loader",
    schema=bronze_orders,
    # tags are not supported
)
def orders():
    """
    Streaming table that incrementally loads order CSV files using Auto Loader.
    Auto Loader monitors the source directory and processes new files automatically.
    """
    catalog = spark.conf.get("catalog")
    schema = spark.conf.get("schema")
    volume = spark.conf.get("volume")
    print(f"Loading orders from /Volumes/{catalog}/{schema}/{volume}/orders")  # Debugging statement
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(raw_orders)
        .load(f"/Volumes/{catalog}/{schema}/{volume}/orders/*.csv")
    )
