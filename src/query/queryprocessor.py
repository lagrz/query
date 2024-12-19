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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        for adapter in self.adapters.values():
            adapter.close()
        self.adapters.clear()

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)
                if not isinstance(config, dict):
                    raise ValueError("Config must be a dictionary")
                required_keys = {"adapter_settings", "queries", "output"}
                missing_keys = required_keys - set(config.keys())
                if missing_keys:
                    raise ValueError(
                        f"Missing required config sections: {missing_keys}"
                    )
                return config
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            raise

    def _get_adapter(self, adapter_name: str) -> DatabaseAdapter:
        if adapter_name not in self.adapters:
            if adapter_name not in self.config["adapter_settings"]:
                raise ValueError(f"Adapter settings not found for: {adapter_name}")
            settings = self.config["adapter_settings"][adapter_name]
            self.adapters[adapter_name] = DatabaseAdapter(AdapterSettings(**settings))
        return self.adapters[adapter_name]

    def _process_query(self, query: Query) -> List[Dict[str, Any]]:
        template = self.template_env.from_string(query.query)
        try:
            logger.debug(f"Using data: {self.data}")
            rendered_query = template.render(self.data)
        except Exception as e:
            logger.error(f"Error rendering query template: {e}")
            raise

        logger.debug(f"Executing query: {rendered_query}")
        adapter = self._get_adapter(query.adapter)

        # Split multiple queries if present
        queries = [q.strip() for q in rendered_query.split(";") if q.strip()]
        results = []

        for single_query in queries:
            if single_query:
                result = adapter.execute_query(single_query.strip())
                results.extend(result)

        return results

    def _update_data_structure(self, table: str, results: List[Dict[str, Any]]) -> None:
        parts = table.split(".")

        if len(parts) == 1:
            # Case: table_name
            self.data[parts[0]] = results[0] if len(results) == 1 else results
        else:
            # Cases: db.table_name or db.alias.table_name
            db = parts[0]
            table_name = parts[-1]  # Last part is always the table name

            if db not in self.data:
                self.data[db] = {}

            current = self.data[db]
            # Navigate through intermediate parts if any
            for part in parts[1:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]

            current[table_name] = results[0] if len(results) == 1 else results

    def process(self) -> str:
        try:
            for query_config in self.config["queries"]:
                query = Query(**query_config)
                results = self._process_query(query)
                self._update_data_structure(query.table, results)

            output = Output(**self.config["output"])
            template = self.template_env.from_string(output.template)
            context = {**self.data, "template_context": output.template_context}
            return template.render(**context).strip()
        except Exception as e:
            logger.error(f"Error processing queries: {e}")
            raise
        finally:
            self.close()
