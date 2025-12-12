from joins.aws import demo
from models.aws import listing

# Create a listing_model transforms
from ai.chronon.model import ModelTransforms
from ai.chronon.query import Query
from ai.chronon.source import JoinSource

from ai.chronon.data_types import DataType

source = JoinSource(
    join=demo.v1,
    # filter rows where the headline / long_description is null as Vertex doesn't like empty content strings
    query=Query(
        wheres=["(listing_id_headline IS NOT NULL AND listing_id_headline != '') OR (listing_id_long_description IS NOT NULL AND listing_id_long_description != '')"]
    )
)

v1 = ModelTransforms(
    sources=[source],
    models=[listing.item_description_model],
    # include a couple of pass through fields from the source / join lookup
    passthrough_fields=["user_id", "listing_id", "listing_id_is_active"],
    version=2,
    output_namespace="data",
    key_fields=[
        ("listing_id_headline", DataType.STRING),
        ("listing_id_long_description", DataType.STRING),
    ]
)
