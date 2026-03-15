from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from databricks.sdk.dbutils import RemoteDbUtils


def save_frame_csv(df: DataFrame, output_path: str, dbutils: RemoteDbUtils) -> str:
    """Save a DataFrame to a single CSV file in the specified output path."""
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    files = dbutils.fs.ls(output_path)
    csv_file = [f.path for f in files if f.path.endswith(".csv")][0]
    target_file_name = output_path + ".csv"
    try:
        dbutils.fs.rm(target_file_name, True)  # remove if already exists
        # raises error when not exists but on remote execution
    except Exception:
        pass
    dbutils.fs.mv(csv_file, target_file_name)
    dbutils.fs.rm(output_path, True)
    return target_file_name


def validate_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Validate and prepare DataFrame to match exact schema."""
    try:
        # This will fail if columns are missing or types don't match
        validated_df = df.select(*[col.name for col in schema.fields])
        # Cast to ensure exact types
        for field in schema.fields:
            validated_df = validated_df.withColumn(field.name, validated_df[field.name].cast(field.dataType))
        return validated_df
    except Exception as e:
        raise ValueError(f"Schema validation failed: {e}")


def widget_or_default(dbutils: RemoteDbUtils, key: str, default=None):
    """Get a widget value or return default if widget doesn't exist or is empty."""
    try:
        value = dbutils.widgets.get(key)
        # Return default if value is None or empty string
        return value if value and value.strip() else default
    except Exception:
        return default
