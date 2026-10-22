# databricks.labs.dqx.profiler.profile

## DQProfile Objects[​](#dqprofile-objects "Direct link to DQProfile Objects")

```python
@dataclass(frozen=True)
class DQProfile()

```

Data quality profile class representing a data quality rule candidate.

**Attributes**:

* `name` - Profile name (e.g. *is_not_null_or_empty*, *min_max*).
* `column` - Column the profile applies to.
* `description` - Optional human-readable description.
* `parameters` - Optional parameters that specialise the rule.
* `filter` - Optional filter expression scoping the rule.
* `semantic_type` - Optional detected semantic type name (e.g. *enum*, *key*, *measurement*, *text*) — records *why* the profile was generated when semantic profiling is enabled. Defaults to *None*, so pre-existing YAML/JSON round-trips are unaffected.

## DQProfileBuilder Objects[​](#dqprofilebuilder-objects "Direct link to DQProfileBuilder Objects")

```python
class DQProfileBuilder(BaseModel)

```

Named builder that may produce a *DQProfile* for a column.

Exactly one of *builder* or *contextual_builder* must be provided.

**Attributes**:

* `name` - Profile type identifier (e.g. *min_max*). Used to look up the builder in the registry and in generated rule metadata.
* `builder` - Legacy 5-argument callback. Left in place for backward compatibility so existing user-authored builders keep working when wrapped in *DQProfileBuilder* or registered via *@register_profile_builder* without *kind="context"*. Does not receive semantic-type information — prefer *contextual_builder* for new code.
* `contextual_builder` - Preferred single-argument callback receiving a *DQProfileContext*. Sees the detected semantic type via *ctx.semantic_type* and can gate its output accordingly.
