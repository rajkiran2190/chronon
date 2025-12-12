from gen_thrift.api.ttypes import EventSource, Source

from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.query import Query, selects
from ai.chronon.types import ConfigProperties, EnvironmentVariables

_action_events = [
    "backend_add_to_cart",
    "view_listing",
    "backend_cart_payment",
    "backend_favorite_item2",
]
_action_events_csv = ", ".join([f"'{event}'" for event in _action_events])
_action_events_filter = f"event_type in ({_action_events_csv})"

def build_source(topic: str) -> Source:
    return Source(
        events=EventSource(
            # This source table contains a custom struct ('attributes') that enables
            # attributes['key'] style access pattern in a BQ native table.
            table="data.item_events_parquet_compat_partitioned",
            topic=topic,
            query=Query(
                selects=selects(
                    listing_id="EXPLODE(TRANSFORM(SPLIT(COALESCE(attributes['sold_listing_ids'], attributes['listing_id']), ','), e -> CAST(e AS LONG)))",
                    add_cart="IF(event_type = 'backend_add_to_cart', 1, 0)",
                    view="IF(event_type = 'view_listing', 1, 0)",
                    purchase="IF(event_type = 'backend_cart_payment', 1, 0)",
                    favorite="IF(event_type = 'backend_favorite_item2', 1, 0)",
                ),
                wheres=[_action_events_filter],
                time_column="timestamp",
            ),
        )
    )

def build_actions_groupby(source: Source) -> GroupBy:
    return GroupBy(
        sources=[source],
        keys=["listing_id"],
        online=True,
        version=0,
        aggregations=[
            Aggregation(input_column="add_cart", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="view", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="purchase", operation=Operation.SUM, windows=["1d"]),
            Aggregation(input_column="favorite", operation=Operation.SUM, windows=["1d"]),
        ],
        conf=ConfigProperties(
            common={
                "spark.chronon.partition.column": "_DATE",
            }
        ),
        env_vars=EnvironmentVariables(
            common={
                "CHRONON_ONLINE_ARGS": "-Ztasks=4",
            }
        ),
    )

# AWS MSK cluster configuration
kafka_topic = f"kafka://test-item-event-data/serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event"
kafka_source = build_source(kafka_topic)

actions_v1 = build_actions_groupby(kafka_source)
