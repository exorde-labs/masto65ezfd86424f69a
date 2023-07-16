from masto65ezfd86424f69a import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    try:
        # Example parameters dictionary
        parameters = {
            "max_oldness_seconds":3600,
            "maximum_items_to_collect": 5,
            "min_post_length": 5,
            "special_keywords_checks": 10,
            "url_parameters":{
                "keyword":"Ethereum  (ETH)"
            }
        }
        async for item in query(parameters):
            assert isinstance(item, Item)
    except ValueError as e:
        print(f"Error: {str(e)}")