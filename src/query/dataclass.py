from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AdapterSettings:
    adapter: str
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None


@dataclass
class Query:
    table: str
    adapter: str
    query: str


@dataclass
class Output:
    template: str
    template_context: Dict[str, Any]
