# databricks.labs.dqx.profiler.semantic

Semantic-type classification for the DQProfiler.

Introduces a lightweight classification stage between metric collection and check generation so a column receives one consistent family of checks (e.g. an enum column no longer receives both *is_in* and *min_max*).

All three mutually-dependent public models — *DQSemanticType*, *DQProfileContext*, and *DQSemanticTypeDetector* — live in this single module to keep the forward-reference cycle (*DQSemanticTypeDetector.detect* depends on *DQProfileContext*; *DQProfileContext.semantic_type* depends on *DQSemanticType*) resolvable at class-creation time without quoted forward refs or *model_rebuild()*.

Public surface:

* models: *DQSemanticType*, *DQProfileContext*, *DQSemanticTypeDetector*
* properties: *DQSemanticTypeProperties*, *EnumProperties*, *MeasurementProperties*
* registry: *SemanticRegistry* — construction: *SemanticRegistry.default()*, *SemanticRegistry.of(...)*, constructor *SemanticRegistry(detectors=(...))* — composition: *prepend*, *append*, *insert(name, detector)*, *replace(name, detector)*, *remove(name)*
* detectors: *DEFAULT_ENUM_DETECTOR*, *DEFAULT_KEY_DETECTOR*, *DEFAULT_MEASUREMENT_DETECTOR*, *DEFAULT_TEXT_DETECTOR*, *default_semantic_detectors()*
* thresholds: *ENUM_MAX_CARDINALITY_RATIO*, *KEY_MIN_DISTINCTNESS_RATIO*, *KEY_MIN_DENSITY_RATIO*, *KEY_MIN_LENGTH_STABILITY_RATIO*

## DQSemanticTypeProperties Objects[​](#dqsemantictypeproperties-objects "Direct link to DQSemanticTypeProperties Objects")

```python
class DQSemanticTypeProperties(BaseModel)

```

Base class for detector-specific properties emitted alongside a *DQSemanticType*.

Extension pattern: user-defined detectors subclass this and pass an instance as the *properties* field of the *DQSemanticType* they emit. The model is frozen and forbids extra fields, matching *DQSemanticType* itself.

## EnumProperties Objects[​](#enumproperties-objects "Direct link to EnumProperties Objects")

```python
class EnumProperties(DQSemanticTypeProperties)

```

Properties for the built-in *enum* semantic type.

**Attributes**:

* `values` - Collected distinct values. Homogeneous by construction — the enum detector only fires on a single scalar column type, so the set carries a single scalar type per instance.

## MeasurementProperties Objects[​](#measurementproperties-objects "Direct link to MeasurementProperties Objects")

```python
class MeasurementProperties(DQSemanticTypeProperties)

```

Properties for the built-in *measurement* semantic type.

**Attributes**:

* `distribution` - Best-effort distribution family guess. *unknown* when statistics were insufficient to classify.

## DQSemanticType Objects[​](#dqsemantictype-objects "Direct link to DQSemanticType Objects")

```python
class DQSemanticType(BaseModel)

```

Classification of a column's semantic meaning.

**Attributes**:

* `name` - Globally unique identifier (e.g. *key*, *enum*, *measurement*). Used in generated check metadata and for detector lookup.
* `description` - Optional human-readable description.
* `properties` - Detector-specific properties as a *DQSemanticTypeProperties* subclass instance, or *None* when the detector emits no properties. The *frozen* model config blocks reassignment of the field itself; each properties subclass is frozen too, so the returned instance is fully immutable.

### get_typed_properties[​](#get_typed_properties "Direct link to get_typed_properties")

```python
def get_typed_properties(
        properties_type: type[_PropertiesT]) -> _PropertiesT | None

```

Return *self.properties* narrowed to *properties_type*, or *None* when the type does not match.

**Arguments**:

* `properties_type` - Concrete *DQSemanticTypeProperties* subclass to cast to.

**Returns**:

The properties instance typed as *properties_type* when it is an instance of that class, else *None*.

## DQProfileContext Objects[​](#dqprofilecontext-objects "Direct link to DQProfileContext Objects")

```python
class DQProfileContext(BaseModel)

```

Immutable context passed to semantic detectors and profile builders.

**Attributes**:

* `df` - DataFrame for this column with non-null rows only. Strings are trimmed when *profiler_options\["trim_strings"]* is True.
* `column_name` - Name of the column being profiled.
* `column_type` - Spark DataType of the column.
* `metrics` - Column-level statistics (count, count_null, empty_count, count_non_null, ...). Same shape as *summary_stats\[column_name]*.
* `options` - Profiler options for this run (max_null_ratio, max_empty_ratio, max_in_count, trim_strings, filter, ...).
* `semantic_type` - Detected semantic type for this column, or *None* if no detector matched. Populated only for profile builders — always *None* for semantic detectors.

### with_metrics[​](#with_metrics "Direct link to with_metrics")

```python
def with_metrics(metrics: Mapping[str, Any]) -> "DQProfileContext"

```

Return a new context whose *metrics* is a fresh snapshot of the supplied mapping.

The model is frozen and Pydantic materializes *metrics* as a copy on construction, so contextual profile builders further down the chain would otherwise not observe write-backs made to the outer mutable metrics dict between builder invocations (e.g. the min/max write-back performed by *DQProfiler.\_build_profiles_for_column* after the *min_max* builder resolves outlier-adjusted bounds). Callers pass the current mutable dict here to obtain a new frozen context that wraps its snapshot; all other fields are preserved.

**Arguments**:

* `metrics` - Column-level metrics to snapshot into the returned context.

**Returns**:

A new *DQProfileContext* instance with *metrics* replaced by a fresh dict copy of the supplied mapping.

## DQSemanticTypeDetector Objects[​](#dqsemantictypedetector-objects "Direct link to DQSemanticTypeDetector Objects")

```python
class DQSemanticTypeDetector(BaseModel)

```

Named detector that classifies a column's semantic type.

**Attributes**:

* `name` - Detector identifier (also used as the produced type's name when the detector emits a single canonical type).
* `detect` - Callable receiving a *DQProfileContext* and returning a *DQSemanticType* when the column matches, otherwise *None*.

### default_semantic_detectors[​](#default_semantic_detectors "Direct link to default_semantic_detectors")

```python
def default_semantic_detectors() -> tuple[DQSemanticTypeDetector, ...]

```

Return the built-in detector chain in first-match-wins order.

Order: enum → key (statistical shape) → measurement (numeric fallback) → text (string fallback).

## SemanticRegistry Objects[​](#semanticregistry-objects "Direct link to SemanticRegistry Objects")

```python
class SemanticRegistry(BaseModel)

```

Immutable, ordered, name-unique collection of semantic-type detectors.

Detectors are consulted in insertion order; the first non-None match wins. The registry is immutable — the model is frozen (Pydantic rejects field assignment) and the *detectors* field is a tuple, so external callers cannot mutate the chain by aliasing. All construction paths (direct instantiation, *of()*, *default()*, *prepend()*, *append()*, *insert()*, *replace()*, *remove()*) route through the model validator, which enforces detector-name uniqueness.

**Attributes**:

* `detectors` - Ordered tuple of detectors in first-match-wins order. Defaults to an empty tuple; use *SemanticRegistry.default()* to obtain the built-in chain or *SemanticRegistry.of(...)* to build an arbitrary chain from scratch.

### default[​](#default "Direct link to default")

```python
@classmethod
def default(cls) -> "SemanticRegistry"

```

Return a registry populated with *default_semantic_detectors()*.

### of[​](#of "Direct link to of")

```python
@classmethod
def of(cls, *detectors: DQSemanticTypeDetector) -> "SemanticRegistry"

```

Return a registry whose chain is exactly *detectors*, in argument order.

Ergonomic alternative to the *SemanticRegistry(detectors=(...))* constructor for whole-chain construction from scratch. Reads well when composing with the built-in defaults, e.g. *SemanticRegistry.of* with a custom detector followed by the unpacked *default_semantic_detectors()*.

**Arguments**:

* `*detectors` - Detectors in first-match-wins order.

**Returns**:

A new *SemanticRegistry*. Detector-name uniqueness is enforced by the model validator, so duplicate names raise *pydantic.ValidationError*.

### prepend[​](#prepend "Direct link to prepend")

```python
def prepend(detector: DQSemanticTypeDetector) -> "SemanticRegistry"

```

Return a new registry with *detector* at position 0 (highest priority).

The current instance is left unchanged. Re-invokes the constructor so the *\_validate_unique_names* model-validator runs on the derived instance — using *model_copy(update=...)* would skip validation and allow duplicate detector names.

### append[​](#append "Direct link to append")

```python
def append(detector: DQSemanticTypeDetector) -> "SemanticRegistry"

```

Return a new registry with *detector* appended at the end (lowest-priority fallback).

The current instance is left unchanged. Re-invokes the constructor so the uniqueness validator runs on the derived instance.

### insert[​](#insert "Direct link to insert")

```python
def insert(name: str, detector: DQSemanticTypeDetector) -> "SemanticRegistry"

```

Return a new registry with *detector* inserted immediately after the entry named *name*.

Semantic: "insert *detector* after the detector identified by *name*". Position of the target detector is preserved; every subsequent detector shifts down by one.

**Arguments**:

* `name` - Name of an existing detector; *detector* is placed at the position immediately after it.
* `detector` - Detector to insert.

**Returns**:

A new *SemanticRegistry* with the additional entry.

**Raises**:

* `ValueError` - If *name* does not match any detector in the current chain.
* `pydantic.ValidationError` - If *detector*'s name collides with a non-target entry.

### replace[​](#replace "Direct link to replace")

```python
def replace(name: str, detector: DQSemanticTypeDetector) -> "SemanticRegistry"

```

Return a new registry with the entry named *name* swapped for *detector*.

Position is preserved; the rest of the chain is unchanged. To construct a whole new chain, use *SemanticRegistry.of(...)* or the *SemanticRegistry(detectors=(...))* constructor instead.

**Arguments**:

* `name` - Name of the detector to swap out.
* `detector` - Replacement detector. May reuse *name* (position-preserving update) or take a different name.

**Returns**:

A new *SemanticRegistry* with the entry replaced.

**Raises**:

* `ValueError` - If *name* does not match any detector in the current chain.
* `pydantic.ValidationError` - If the replacement's name collides with a non-target entry in the chain.

### remove[​](#remove "Direct link to remove")

```python
def remove(name: str) -> "SemanticRegistry"

```

Return a new registry with the entry named *name* filtered out.

**Arguments**:

* `name` - Name of the detector to drop.

**Returns**:

A new *SemanticRegistry* without the entry.

**Raises**:

* `ValueError` - If *name* does not match any detector in the current chain.
