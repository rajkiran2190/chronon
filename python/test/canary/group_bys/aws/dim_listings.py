from staging_queries.aws import exports

from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, selects
from ai.chronon.source import EntitySource

"""
This GroupBy creates a simple passthrough transformation on the dim_listings table.
It selects key columns from the dimension table with no aggregations,
providing a clean interface to listing attributes for joins and feature engineering.
"""

source = EntitySource(
    snapshot_table=exports.dim_listings.table,
    query=Query(
        selects=selects(
            listing_id="listing_id",
            merchant_id="merchant_id",
            headline="headline",
            brief_description="brief_description",
            long_description="long_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            weight_grams="weight_grams",
            tags="tags",
            # Derived features
            is_expensive="IF(price_cents > 10000, 1, 0)",  # Over $100
            is_in_stock="IF(inventory_count > 0, 1, 0)",
            main_image_path="main_image_path",
            secondary_image_paths="secondary_image_paths",
        ),
        start_partition="2025-01-01"
    ),

)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],  # Key by listing_id for point lookups
    online=True,
    version=0,
    aggregations=None,  # No aggregations - this is a simple passthrough
)
