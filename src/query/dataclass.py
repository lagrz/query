from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AdapterSettings:
    adapter: str
    database: Optional[str] = None
    pool_name: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    base_url: Optional[str] = None
    base_path: Optional[str] = None
    base_payload: Optional[Dict[str, Any]] = None
    base_headers: Optional[Dict[str, Any]] = None
    http_method: Optional[str] = None


@dataclass
class Query:
    table: str
    adapter: str
    query: str


@dataclass
class Output:
    template: str
    template_context: Dict[str, Any]
