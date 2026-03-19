import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
import os
import math

cfg = Config()


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


def get_table_data(table: str, warehouse: str) -> pd.DataFrame:
    return sql_query(
        query=f"select * from {table} where status=='GENERATED' order by timestamp desc", warehouse=warehouse
    )


def accept_change(record_id: str, sql: str, fqn_govern_table: str, warehouse: str):
    sql_query(query=sql, warehouse=warehouse)
    sql_query(
        query=f"update {fqn_govern_table} set status='ACCEPTED' where record_id='{record_id}'", warehouse=warehouse
    )
    st.session_state["data"] = get_table_data(table=fqn_govern_table, warehouse=warehouse)


def decline_change(record_id: str, fqn_govern_table: str, warehouse: str):
    sql_query(
        query=f"update {fqn_govern_table} set status='DECLINED' where record_id='{record_id}'", warehouse=warehouse
    )
    st.session_state["data"] = get_table_data(table=fqn_govern_table, warehouse=warehouse)


def archive_change(record_id: str, fqn_govern_table: str, warehouse: str):
    sql_query(
        query=f"update {fqn_govern_table} set status='ARCHIVED' where record_id='{record_id}'", warehouse=warehouse
    )
    st.session_state["data"] = get_table_data(table=fqn_govern_table, warehouse=warehouse)


warehouse = os.getenv("warehouse")
catalog = os.getenv("catalog")
schema = os.getenv("schema")
govern_table = os.getenv("govern_table")
fqn_govern_table = f"{catalog}.{schema}.{govern_table}"

if "data" not in st.session_state:
    st.session_state["data"] = get_table_data(table=fqn_govern_table, warehouse=warehouse)


st.set_page_config(layout="wide", page_title="Table comments governance")

data: pd.DataFrame = st.session_state["data"]


st.header("Table comments governance application")
st.caption(f"{warehouse}/{fqn_govern_table}")

red = "🔴"
green = "🟢"
yellow = "🟡"

if data.empty:
    st.caption("No generated records found")

for index, row in data.iterrows():
    st.header(f"{row['table_name']}")
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

    if row["govern_error"] is not None:
        st.caption("Error:")
        st.code(row["govern_error"], language="python", wrap_lines=True)

    sufficient = row["sql"] == "SUFFICIENT"
    have_sql = (row["sql"] is not None) and (not sufficient)

    if sufficient:
        st.caption("Governer decided that current description is sufficient")
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
            args=[row["record_id"], row["sql"], fqn_govern_table, warehouse],
        )

        st.button(
            "Decline",
            key=f"decline-{row['record_id']}",
            on_click=decline_change,
            args=[row["record_id"], fqn_govern_table, warehouse],
        )
    else:
        st.button(
            "Archive",
            key=f"archive-{row['record_id']}",
            on_click=archive_change,
            args=[row["record_id"], fqn_govern_table, warehouse],
        )
