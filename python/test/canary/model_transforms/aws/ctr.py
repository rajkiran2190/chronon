from joins.aws import demo
from models.aws import click_through_rate

# Create a ModelTransforms where we enrich the demo join fields with the ctr model score
from ai.chronon.model import ModelTransforms
from ai.chronon.source import JoinSource

from ai.chronon.data_types import DataType

source = JoinSource(join=demo.derivations_v1)

v1 = ModelTransforms(
    sources=[source], # noticed that this source is used in both ModelTransform and Models
    models=[click_through_rate.ctr_model],
    # include relevant pass through fields from the source / join lookup
    passthrough_fields=["user_id", "listing_id", "user_id_click_event_average_7d", "listing_id_price_cents", "price_log", "price_bucket"],
    version=1,
    output_namespace="data",
    key_fields=[
        ("user_id_click_event_average_7d", DataType.DOUBLE),
        ("listing_id_price_cents", DataType.LONG),
        ("price_log", DataType.DOUBLE),
        ("price_bucket", DataType.INT)
    ]
)
