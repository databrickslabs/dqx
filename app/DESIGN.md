# DQX Server-Driven UI (SDUI) Design Document

## Overview

Migrate DQX configuration classes (`RunConfig`, `WorkspaceConfig`) from dataclasses to Pydantic models with **typed UI metadata**. The backend defines form rendering via strongly-typed Python, and the frontend renders forms dynamically using [react-jsonschema-form](https://rjsf-team.github.io/react-jsonschema-form/) with [`@rjsf/shadcn`](https://github.com/rjsf-team/react-jsonschema-form/tree/main/packages/shadcn).

## Core Design

### Typed UI Schema

```python
# src/databricks/labs/dqx/ui_schema.py

from dataclasses import dataclass, fields
from typing import Annotated, Literal, Any
from pydantic_core import core_schema
from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue

WidgetType = Literal[
    "text", "textarea", "password", "hidden", "checkbox", "radio", "select",
    "range", "file", "date", "datetime", "time",
    # Custom DQX widgets
    "tableSelector", "warehouseSelector", "keyValueEditor", "codeEditor", "secretSelector"
]


@dataclass(frozen=True)
class WithUISchema:
    """
    Typed annotation for rjsf uiSchema properties.
    
    Usage:
        location: Annotated[str, WithUISchema(widget="tableSelector", placeholder="catalog.schema.table")]
    """
    widget: WidgetType | str | None = None
    title: str | None = None
    help: str | None = None
    placeholder: str | None = None
    enum_names: tuple[str, ...] | None = None
    disabled: bool = False
    readonly: bool = False
    hidden: bool = False
    autofocus: bool = False
    class_names: str | None = None
    # DQX extensions
    section: str | None = None
    collapsible: bool = False
    collapsed: bool = False
    order: int | None = None
    visible_if: dict[str, Any] | None = None
    options: dict[str, Any] | None = None
    
    def to_ui_dict(self) -> dict[str, Any]:
        """Convert to rjsf uiSchema format with 'ui:' prefixed keys."""
        mapping = {
            "widget": "ui:widget",
            "title": "ui:title",
            "help": "ui:help",
            "placeholder": "ui:placeholder",
            "enum_names": "ui:enumNames",
            "disabled": "ui:disabled",
            "readonly": "ui:readonly",
            "autofocus": "ui:autofocus",
            "class_names": "ui:classNames",
            "section": "ui:section",
            "collapsible": "ui:collapsible",
            "collapsed": "ui:collapsed",
            "order": "ui:order",
            "visible_if": "ui:visibleIf",
            "options": "ui:options",
        }
        
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if value and f.name in mapping:
                result[mapping[f.name]] = value
        
        if self.hidden:
            result["ui:widget"] = "hidden"
            
        return result
    
    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return handler(source_type)
    
    def __get_pydantic_json_schema__(
        self, _core_schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(_core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema.update(self.to_ui_dict())
        return json_schema


# Reusable type aliases
TablePath = Annotated[str, WithUISchema(widget="tableSelector", placeholder="catalog.schema.table")]
WarehouseId = Annotated[str | None, WithUISchema(widget="warehouseSelector")]
SecretRef = Annotated[str, WithUISchema(widget="secretSelector", placeholder="scope/key")]
SparkOptions = Annotated[dict[str, str], WithUISchema(widget="keyValueEditor")]
```

### Example Migration

```python
# src/databricks/labs/dqx/config.py

from typing import Annotated, Literal
from pydantic import BaseModel, Field
from databricks.labs.dqx.ui_schema import WithUISchema, TablePath, SparkOptions

class InputConfig(BaseModel):
    location: Annotated[str, WithUISchema(
        widget="tableSelector",
        placeholder="catalog.schema.table",
        section="Source"
    )]
    
    format: Annotated[Literal["delta", "parquet", "csv", "json"], WithUISchema(
        widget="select",
        enum_names=("Delta Lake", "Parquet", "CSV", "JSON"),
        section="Source"
    )] = "delta"
    
    is_streaming: Annotated[bool, WithUISchema(
        help="Enable for streaming sources",
        section="Source"
    )] = False
    
    options: SparkOptions = Field(default_factory=dict)
```

### Backend Schema Endpoint

```python
# app/src/databricks_labs_dqx_app/backend/router.py

@router.get("/api/schema/{config_type}")
def get_config_schema(config_type: str) -> dict:
    models = {"run_config": RunConfig, "input_config": InputConfig, ...}
    model = models.get(config_type)
    if not model:
        raise HTTPException(404)
    
    schema = model.model_json_schema()
    ui_schema = extract_ui_schema(schema)  # Extracts ui:* into separate dict
    return {"schema": schema, "uiSchema": ui_schema}


def extract_ui_schema(schema: dict) -> dict:
    """Extract ui:* properties from JSON schema into rjsf uiSchema."""
    ui = {k: schema.pop(k) for k in list(schema) if k.startswith("ui:")}
    for name, prop in schema.get("properties", {}).items():
        if nested := extract_ui_schema(prop):
            ui[name] = nested
    return ui
```

### Frontend Form

```tsx
// app/src/databricks_labs_dqx_app/ui/components/ConfigForm.tsx

import { withTheme } from '@rjsf/core';
import { Theme as ShadcnTheme } from '@rjsf/shadcn';
import validator from '@rjsf/validator-ajv8';

const Form = withTheme(ShadcnTheme);

const customWidgets = {
  tableSelector: TableSelectorWidget,
  warehouseSelector: WarehouseSelectorWidget,
  keyValueEditor: KeyValueEditorWidget,
};

export function ConfigForm({ configType, initialData, onSubmit }) {
  const { data } = useQuery({
    queryKey: ['schema', configType],
    queryFn: () => fetch(`/api/schema/${configType}`).then(r => r.json()),
  });

  if (!data) return <Skeleton />;

  return (
    <Form
      schema={data.schema}
      uiSchema={data.uiSchema}
      formData={initialData}
      validator={validator}
      widgets={customWidgets}
      onSubmit={({ formData }) => onSubmit(formData)}
    />
  );
}
```

## References

- [Pydantic JSON Schema](https://docs.pydantic.dev/latest/concepts/json_schema/)
- [rjsf uiSchema](https://rjsf-team.github.io/react-jsonschema-form/docs/api-reference/uiSchema)
- [@rjsf/shadcn](https://github.com/rjsf-team/react-jsonschema-form/tree/main/packages/shadcn)
