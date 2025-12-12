"""
Staging query to partition the loggable_response BigQuery table which is append only subscription.
We need some specific columns for the logFlattener job such as key_base64 and value_base64
We keep the Binary columns as well as it's simpler casting to UTF-8 in bigQuery.
"""
from ai.chronon.staging_query import StagingQuery, TableDependency

v0 = StagingQuery(
    dependencies=[TableDependency(table="data.loggable_response")],
    query="""
      SELECT
        DATE(TIMESTAMP_MILLIS(tsMillis)) AS ds
        , tsMillis as ts_millis
        , BASE64(keyBytes) as key_base64
        , BASE64(valueBytes) as value_base64
        , joinName as name
        , schemaHash as schema_hash
        , keyBytes
        , valueBytes
      FROM data.loggable_response
      WHERE DATE(TIMESTAMP_MILLIS(tsMillis)) BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="data",
    version=2,
)
