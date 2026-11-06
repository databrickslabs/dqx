"""Demo saved views: the personal views the demo seed adds to the Tables, Results, Rules and Collections pages.

The views live in the bundled ``saved_views.json`` template (the single source
of truth, also read by the UI drift test). Values that point at the demo data
use two placeholder tokens, ``{catalog}`` and ``{schema}``, which are replaced
with the installation's catalog and demo schema when the seed runs.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path

from databricks_labs_dqx_app.backend.saved_views_models import SavedViewPage, SavedViewPayload

SAVED_VIEWS_TEMPLATE = Path(__file__).parent / "saved_views.json"

CATALOG_TOKEN = "{catalog}"
SCHEMA_TOKEN = "{schema}"
# Single pass over both exact tokens, so a substituted value is never re-scanned.
_TOKEN_PATTERN = re.compile(f"{re.escape(CATALOG_TOKEN)}|{re.escape(SCHEMA_TOKEN)}")


@dataclass(frozen=True)
class DemoSavedViewSpec:
    """One demo personal view.

    Args:
        page: Page the view belongs to.
        name: View name (unique among the owner's views on the page).
        payload: Column layout, filter layout and filter values.
    """

    page: SavedViewPage
    name: str
    payload: SavedViewPayload


def _substitute(node: object, catalog: str, schema: str) -> object:
    """Replace the two placeholder tokens in every string value of *node* (keys are left alone)."""
    if isinstance(node, str):
        return _TOKEN_PATTERN.sub(lambda match: catalog if match.group(0) == CATALOG_TOKEN else schema, node)
    if isinstance(node, list):
        return [_substitute(item, catalog, schema) for item in node]
    if isinstance(node, dict):
        return {key: _substitute(value, catalog, schema) for key, value in node.items()}
    return node


def demo_saved_views(catalog: str, schema: str, template: str | None = None) -> list[DemoSavedViewSpec]:
    """Build the demo saved views for the given demo catalog and schema.

    Args:
        catalog: Catalog holding the demo tables.
        schema: Bare name of the demo schema.
        template: JSON template text; defaults to the bundled *saved_views.json*.

    Returns:
        One spec per template entry, with the placeholders substituted.

    Raises:
        ValueError: If the template is malformed or a payload is invalid.
    """
    raw = template if template is not None else SAVED_VIEWS_TEMPLATE.read_text(encoding="utf-8")
    parsed: object = json.loads(raw)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("views"), list):
        raise ValueError("Demo saved views template must hold a 'views' list")
    specs: list[DemoSavedViewSpec] = []
    for entry in parsed["views"]:
        if not isinstance(entry, dict):
            raise ValueError("Each demo saved view must be an object")
        page = SavedViewPage(str(entry.get("page")))
        name = str(entry.get("name") or "")
        if not name:
            raise ValueError("Each demo saved view needs a name")
        payload = SavedViewPayload.model_validate(_substitute(entry.get("payload") or {}, catalog, schema))
        specs.append(DemoSavedViewSpec(page=page, name=name, payload=payload))
    return specs
