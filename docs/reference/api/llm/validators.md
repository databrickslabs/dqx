# databricks.labs.dqx.llm.validators

## LLMValidationScoreWeights Objects[​](#llmvalidationscoreweights-objects "Direct link to LLMValidationScoreWeights Objects")

```python
@dataclass(frozen=True)
class LLMValidationScoreWeights()

```

Weights for rule validation scoring.

### \_\_post\_init\_\_[​](#__post_init__ "Direct link to __post_init__")

```python
def __post_init__()

```

Validate weights sum to 1.0.

## RuleValidator Objects[​](#rulevalidator-objects "Direct link to RuleValidator Objects")

```python
class RuleValidator()

```

Validates generated data quality rules using DQX engine.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(custom_check_functions: dict[str, Callable] | None = None,
             score_weights: LLMValidationScoreWeights | None = None)

```

Initialize the rule validator.

**Arguments**:

* `custom_check_functions` - Optional custom check functions to include in validation.
* `score_weights` - Weights for scoring different aspects of validation.

### validate[​](#validate "Direct link to validate")

```python
def validate(rules_json: str) -> float

```

Validate generated rules with granular scoring.

Scoring breakdown:

* JSON parsing: Checks if the output can be parsed as valid JSON.
* Rules validation: Ensures rules pass DQX checks validation.

**Arguments**:

* `rules_json` - JSON string of the generated rules.

**Returns**:

Score between 0.0 and 1.0 representing the quality of the generated rules.
