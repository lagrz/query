import logging
from typing import Any, Dict

from jinja2 import Environment

logger = logging.getLogger(__name__)


def parse_initial_data(initial_data: str) -> Dict[str, Any]:
    if not initial_data:
        return {}

    result = {}
    try:
        for pair in initial_data.split(","):
            key, value = pair.split("=")
            result[key.strip()] = value.strip()
        return result
    except ValueError as e:
        logger.error(f"Invalid initial data format: {e}")
        raise ValueError("Initial data must be in format: key1=value1,key2=value2")


def get_template_environment() -> Environment:
    def find_by_key_value(haystack, key, value, default=None):
        """
        Finds an item in a list or dictionary by key and value.
        Returns the item if found, otherwise returns the default value.

        Parameters:
        haystack (list or dict): The list or dictionary to search.
        key (str): The key to search for.
        value (any): The value to search for.
        default (any): The default value to return if the item is not found.

        Returns:
        any: The item found, or the default value if not found.

        Example:
            {{ db2.transaction_information | find_by_key_value('transaction_id', db2.transactions[0].id) }}
        Gets the transaction information for the first transaction in the db2.transactions list.
        """
        if isinstance(haystack, list):
            for item in haystack:
                if key in item and item[key] == value:
                    return item
            return default
        elif isinstance(haystack, dict):
            if key in haystack and haystack[key] == value:
                return haystack
            return default
        else:
            return default

    env = Environment()
    env.filters["find_by_key_value"] = find_by_key_value
    return env
