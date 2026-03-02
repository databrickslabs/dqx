import json
from collections.abc import Callable
from typing import Any

import yaml

# Keep format constants isolated to avoid import cycles (config <-> checks_serializer).
FILE_SERIALIZERS: dict[str, Callable[[list[dict[Any, Any]]], str]] = {
    ".json": json.dumps,
    ".yml": yaml.safe_dump,
    ".yaml": yaml.safe_dump,
}

FILE_DESERIALIZERS: dict[str, Callable] = {
    ".json": json.load,
    ".yaml": yaml.safe_load,
    ".yml": yaml.safe_load,
}
