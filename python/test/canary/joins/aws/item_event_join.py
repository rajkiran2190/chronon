from gen_thrift.api.ttypes import EventSource, Source
from group_bys.aws import item_event_canary, purchases

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects

source = Source(
    events=EventSource(
        table="data.item_events_parquet_compat",
        query=Query(
            selects=selects(
              listing_id="EXPLODE(TRANSFORM(SPLIT(COALESCE(attributes['sold_listing_ids'], attributes['listing_id']), ','), e -> CAST(e AS LONG)))",
              user_id="attributes['user_id']",
            ),
            time_column="timestamp",
        ),
    )
)

# Join with just a streaming GB
canary_streaming_v1 = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=item_event_canary.actions_v1)
    ],
    version=0,
    online=True,
)

# Join with just a batch GB
canary_batch_v1 = Join(
    left=source,
    row_ids="listing_id",
    version=0,
    right_parts=[
        JoinPart(group_by=purchases.v1_test)
    ],
    online=True,
)

# Join with a streaming and batch GB
canary_combined_v1 = Join(
    left=source,
    row_ids=["user_id"],
    version=0,
    right_parts=[
        JoinPart(group_by=item_event_canary.actions_v1),
        JoinPart(group_by=purchases.v1_test)
    ],
    online=True,
)
