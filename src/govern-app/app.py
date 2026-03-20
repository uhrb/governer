import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
import os
import math

cfg = Config()
session_data_key = "data"
session_meta_key = "metadata"


# Use app service principal authentication
def get_connection(http_path):
    server_hostname = cfg.host
    if server_hostname.startswith("https://"):
        server_hostname = server_hostname.replace("https://", "")
    elif server_hostname.startswith("http://"):
        server_hostname = server_hostname.replace("http://", "")
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


def sql_query(query: str, warehouse: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def get_govern_records(fqn_govern_table: str, warehouse: str) -> pd.DataFrame:
    return sql_query(
        query=f"select * from {fqn_govern_table} where status=='GENERATED' order by timestamp desc", warehouse=warehouse
    )


def get_tables_metadata(catalog: str, warehouse: str):
    return sql_query(
        query=f"""
                    WITH table_tags_agg AS (
                SELECT
                    catalog_name,
                    schema_name,
                    table_name,
                    array_sort(collect_set(concat(tag_name, '=', tag_value))) AS table_tags
                FROM {catalog}.information_schema.table_tags
                GROUP BY catalog_name, schema_name, table_name
            ),

            column_tags_agg AS (
                SELECT
                    catalog_name,
                    schema_name,
                    table_name,
                    column_name,
                    array_sort(collect_set(concat(tag_name, '=', tag_value))) AS column_tags
                FROM {catalog}.information_schema.column_tags
                GROUP BY catalog_name, schema_name, table_name, column_name
            ),

            columns_agg AS (
                SELECT
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    collect_list(
                        struct(
                            c.column_name,
                            c.comment AS column_comment,
                            coalesce(ct.column_tags, array()) AS column_tags
                        )
                    ) AS columns
                FROM {catalog}.information_schema.columns c
                LEFT JOIN column_tags_agg ct
                    ON c.table_catalog = ct.catalog_name
                    AND c.table_schema = ct.schema_name
                    AND c.table_name = ct.table_name
                    AND c.column_name = ct.column_name
                GROUP BY c.table_catalog, c.table_schema, c.table_name
            )

            SELECT
                concat(t.table_catalog,'.',t.table_schema,'.',t.table_name) as fq_table_name,
                t.comment AS table_comment,
                coalesce(tt.table_tags, array()) AS table_tags,
                coalesce(ca.columns, array()) AS columns
            FROM {catalog}.information_schema.tables t
            LEFT JOIN table_tags_agg tt
                ON t.table_catalog = tt.catalog_name
                AND t.table_schema = tt.schema_name
                AND t.table_name = tt.table_name
            LEFT JOIN columns_agg ca
                ON t.table_catalog = ca.table_catalog
                AND t.table_schema = ca.table_schema
                AND t.table_name = ca.table_name
            
        """,
        warehouse=warehouse,
    )


def reload_session(catalog: str, fqn_govern_table: str, warehouse: str):
    st.session_state[session_data_key] = get_govern_records(fqn_govern_table=fqn_govern_table, warehouse=warehouse)
    st.session_state[session_meta_key] = get_tables_metadata(catalog=catalog, warehouse=warehouse)


def set_record_status(record_id: str, status: str, catalog: str, fqn_govern_table: str, warehouse: str):
    sql_query(
        query=f"update {fqn_govern_table} set status='{status}' where record_id='{record_id}'", warehouse=warehouse
    )
    reload_session(catalog=catalog, fqn_govern_table=fqn_govern_table, warehouse=warehouse)


def accept_change(record_id: str, sql: str, catalog: str, fqn_govern_table: str, warehouse: str):
    sql_query(query=sql, warehouse=warehouse)
    set_record_status(
        record_id=record_id, status="ACCEPTED", catalog=catalog, fqn_govern_table=fqn_govern_table, warehouse=warehouse
    )


warehouse = os.getenv("warehouse")
catalog = os.getenv("catalog")
schema = os.getenv("schema")
govern_table = os.getenv("govern_table")
fqn_govern_table = f"{catalog}.{schema}.{govern_table}"

if session_data_key not in st.session_state:
    reload_session(catalog=catalog, fqn_govern_table=fqn_govern_table, warehouse=warehouse)

st.set_page_config(layout="wide", page_title="AI Data Governance")

data: pd.DataFrame = st.session_state[session_data_key]
metadata: pd.DataFrame = st.session_state[session_meta_key]


st.header("AI Data Governance")
st.caption(f"{warehouse}/{fqn_govern_table}")

red = "🔴"
green = "🟢"
yellow = "🟡"

if data.empty:
    st.caption("No generated records found")

for index, row in data.iterrows():
    current_table_name = row["table_name"]
    st.header(current_table_name)
    govern = red
    if row["govern_success"]:
        govern = green

    eval = red

    if row["evaluation"] is not None:
        if row["evaluation"] < 0 or math.isnan(row["evaluation"]):
            eval = red
        elif row["evaluation"] <= 0.7:
            eval = yellow
        else:
            eval = green

    record_data = {
        "Evaluation": [f"{eval} {row['evaluation']}"],
        "Record Id": [row["record_id"]],
        "Run Id": [row["run_id"]],
        "Created At": [row["timestamp"]],
        "Process status": [f"{govern} {row['status']}"],
    }
    st.table(record_data)

    if current_table_name in metadata["fq_table_name"].values:
        metarow_frame = metadata[metadata["fq_table_name"] == current_table_name]
        if not metarow_frame.empty:
            metarow = metarow_frame.iloc[0]
            st.caption("Current comment:")
            st.code(metarow["table_comment"], language="text")
            tags = metarow["table_tags"]
            for tag in tags:
                st.markdown(f":blue-background[{tag}]")
            columns = metarow["columns"]
            display = {"Column Name": [], "Column Comment": [], "Column Tags": []}
            for column in columns:
                display["Column Name"].append(column["column_name"])
                display["Column Comment"].append(column["column_comment"])
                ctagstr = ""
                for ctag in column["column_tags"]:
                    ctagstr += f"{ctag} "
                display["Column Tags"].append(ctagstr)
            st.table(display)

    if row["govern_error"] is not None:
        st.caption("Error:")
        st.code(row["govern_error"], language="python", wrap_lines=True)

    sufficient = row["sql"] == "SUFFICIENT"
    have_sql = (row["sql"] is not None) and (not sufficient)

    if sufficient:
        st.caption("No Action required for this governance record")
    elif have_sql:
        st.caption("Proposed SQL:")
        st.code(row["sql"], language="sql", wrap_lines=True)
    else:
        st.caption("Error in SQL generation")

    if have_sql:
        st.button(
            "Accept and Execute",
            key=f"accept-{row['record_id']}",
            on_click=accept_change,
            args=[row["record_id"], row["sql"], catalog, fqn_govern_table, warehouse],
            # record_id: str, sql: str, catalog: str, fqn_govern_table: str, warehouse: str
        )

        st.button(
            "Decline",
            key=f"decline-{row['record_id']}",
            on_click=set_record_status,
            args=[row["record_id"], "DECLINED", catalog, fqn_govern_table, warehouse],
            # record_id: str, status: str, catalog: str, fqn_govern_table: str, warehouse: str
        )
    else:
        st.button(
            "Archive",
            key=f"archive-{row['record_id']}",
            on_click=set_record_status,
            args=[row["record_id"], "ARCHIVED", catalog, fqn_govern_table, warehouse],
        )
    st.divider()
