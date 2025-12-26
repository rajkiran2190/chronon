from gen_thrift.api.ttypes import EventSource, Source, JoinSource

from group_bys.gcp import dim_listings, dim_merchants
from staging_queries.gcp import exports

from ai.chronon.group_by import Aggregation, Operation, TimeUnit, Window
from ai.chronon.join import Derivation, Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from ai.chronon.types import EnvironmentVariables

source = EventSource(
    # This will be the BigQuery table that receives the PubSub data
    table=exports.user_activities.table,
    topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
    query=Query(
        selects=selects(user_id="user_id", listing_id="listing_id", row_id="event_id"),
        time_column="unix_millis(TIMESTAMP(event_time_ms))",
    ),
)

"""
This Join serves as a parent Join that includes listing features.
Can be used in a downstream GroupBy to enrich the GB with listing attributes.

Left source: User activity event stream - as this is an upstream Join, the left source stream is one
that is amenable for enrichment in the downstream GB app and includes user as well as listing identifiers.
"""
parent_join = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    version=0,
    online=True,
    output_namespace="data",
    offline_schedule="17 0 * * *",
)

upstream_join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            price_cents="listing_id_price_cents",
        ),
        time_column="ts",
    ),
)
