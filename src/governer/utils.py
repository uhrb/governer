from dataclasses import dataclass
from enum import Enum
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieSpace, MessageStatus
import json

from pyspark.sql.catalog import Table


def save_frame_csv(df: DataFrame, output_path: str, dbutils: any) -> str:
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


def get_valid_govern_tables(catalog: str, schema: str, tables: List[Table]) -> List[str]:
    out_tables = []

    for table in tables:
        if not table.name.startswith("ai_"):
            out_tables.append(f"{catalog}.{schema}.{table.name}")

    return out_tables


def get_genie_workspace(space_title: str, warehouse_id: str, tables: list[str]):
    space_def = {
        "version": "2",
        "data_sources": {
            "tables": [
                #  {"identifier": "rowdoc.uladzimir_harabtsou.bronze_customers", "description": ["Customer master data"]},
                #  {"identifier": "rowdoc.uladzimir_harabtsou.bronze_orders", "description": ["Orders"]},
                #  {"identifier": "rowdoc.uladzimir_harabtsou.bronze_payments", "description": ["Payment transactions"]},
            ],
        },
        ###
        ###    "instructions": {
        ###    "text_instructions": [
        ###    {
        ###        "id": "01f0b37c378e1c9100000000000000a1",
        ###        "content": [
        ###        "General instructions for the space."
        ###        ]
        ###    }
        ###    ],
        ### }
    }
    for table in tables:
        space_def["data_sources"]["tables"].append({"identifier": table})

    client = WorkspaceClient()
    spaces = client.genie.list_spaces().spaces
    for space in spaces:
        if space.title == space_title:
            client.genie.trash_space(space.space_id)
            break
    space = client.genie.create_space(
        warehouse_id=warehouse_id,
        title=space_title,
        description="Genie workspace for Governer demo",
        serialized_space=json.dumps(space_def),
    )

    return space


@dataclass
class AiResponse:
    prompt: str
    text: str | None
    success: bool
    error: str | None
    queries: List[str]


@dataclass
class AiResponseWithEval(AiResponse):
    eval: float | None


@dataclass
class GovernStatus(Enum):
    GENERATED = "GENERATED"
    DECLINED = "DECLINED"
    ARCHIVED = "ARCHIVED"
    ACCEPTED = "ACCEPTED"


def trash_genie_workspace(space: GenieSpace) -> None:
    client = WorkspaceClient()
    client.genie.trash_space(space.space_id)


def task_genie(space: GenieSpace, task: str) -> AiResponse:
    client = WorkspaceClient()
    response = AiResponse(task, None, False, None, [])
    # try:
    message = client.genie.start_conversation_and_wait(space.space_id, task)
    if message.status == MessageStatus.COMPLETED:
        if len(message.attachments) > 0:
            for att in message.attachments:
                if att.text is not None:
                    response.text = att.text.content
                if att.query is not None:
                    query_sql = att.query.query
                    response.queries.append(query_sql)

            response.success = True
        else:
            response.error = f"Unexpected length of attachements = {len(message.attachments)}"
    else:
        response.error = f"Unexpected status = {message.status.name}"
    # except Exception as e:
    #    response.error = e.__str__

    return response
