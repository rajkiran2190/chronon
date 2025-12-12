from gen_thrift.api.ttypes import EventSource, Source
from group_bys.aws import purchases

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects(
                "user_id"
            ),  # The primary key used to join various GroupBys together
            time_column="ts",
        ),  # The event time used to compute feature values as-of
    )
)

v1_test = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.v1_test)
    ],
    version=0,
)

v1_hub = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.v1_test)
    ],
    version=0,
)

v1_dev = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.v1_dev)
    ],
    version=0,
)
