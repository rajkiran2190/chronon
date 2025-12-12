"""
The log flattener job requires a schema table to query.
The schema table provides the latest schema based on the schema hash (stored in keyBytes)

This implementation assumes a pubsub bigQuery subscription to create the loggable_response table.
The fields are stored as bytes and such the schema can be decoded by casting to string.
"""
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Accuracy
from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects
import staging_queries.aws.partitioned_logging as partitioned_logging

source = EventSource(
    table=partitioned_logging.v0.table,
    query=Query(
        selects=selects(
            schema_hash="CAST(keyBytes AS STRING)",
            schema_value="CAST(valueBytes AS STRING)"
        ),
        start_partition="2025-09-23",
        partition_column="ds",
        wheres=["name='SCHEMA_PUBLISH_EVENT'"],
        time_column="ts_millis",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["schema_hash"],
    backfill_start_date="2025-09-23",
    aggregations=[Aggregation(input_column="schema_value", operation=Operation.LAST)],
    accuracy=Accuracy.SNAPSHOT,
    version=2,
)
