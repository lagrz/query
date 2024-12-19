import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from jinja2 import Environment

from .database import DatabaseAdapter
from .dataclass import AdapterSettings, Query, Output

logger = logging.getLogger(__name__)


class QueryProcessor:
    def __init__(
        self,
        config_path: Path,
        template_env: Environment,
        initial_data: Optional[Dict[str, Any]] = None,
    ):
        self.config_path = config_path
        self.data = initial_data or {}
        self.template_env = template_env
        self.config = self._load_config()
        self.adapters: Dict[str, DatabaseAdapter] = {}

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            raise

    def _get_adapter(self, adapter_name: str) -> DatabaseAdapter:
        if adapter_name not in self.adapters:
            settings = self.config["adapter_settings"][adapter_name]
            self.adapters[adapter_name] = DatabaseAdapter(AdapterSettings(**settings))
        return self.adapters[adapter_name]

    def _process_query(self, query: Query) -> Any:
        template = self.template_env.from_string(query.query)
        rendered_query = template.render(**self.data)

        logger.debug(f"Executing query: {rendered_query}")
        adapter = self._get_adapter(query.adapter)

        # Split multiple queries if present
        queries = [q.strip() for q in rendered_query.split("\n") if q.strip()]
        results = []

        for single_query in queries:
            if single_query:
                result = adapter.execute_query(single_query)
                results.extend(result)

        return results

    def _update_data_structure(self, table: str, results: List[Dict[str, Any]]) -> None:
        db, table_name = table.split(".")
        if db not in self.data:
            self.data[db] = {}

        if len(results) == 1:
            self.data[db][table_name] = results[0]
        else:
            self.data[db][table_name] = results

    def process(self) -> str:
        try:
            for query_config in self.config["queries"]:
                query = Query(**query_config)
                results = self._process_query(query)
                self._update_data_structure(query.table, results)

            output = Output(**self.config["output"])
            template = self.template_env.from_string(output.template)
            context = {**self.data, "template_context": output.template_context}
            return template.render(**context)
        except Exception as e:
            logger.error(f"Error processing queries: {e}")
            raise
