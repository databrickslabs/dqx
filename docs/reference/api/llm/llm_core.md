# databricks.labs.dqx.llm.llm\_core

## LLMModelConfigurator Objects[​](#llmmodelconfigurator-objects "Direct link to LLMModelConfigurator Objects")

```python
class LLMModelConfigurator()

```

Configures DSPy language models.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(model_config: LLMModelConfig)

```

Initialize model configurator.

**Arguments**:

* `model_config` - Configuration for the LLM model.

### create\_lm[​](#create_lm "Direct link to create_lm")

```python
def create_lm() -> dspy.LM

```

Create an LM instance with current config for per-request override.

**Returns**:

A new LM instance configured with the current model config.

## DspySchemaGuesserSignature Objects[​](#dspyschemaguessersignature-objects "Direct link to DspySchemaGuesserSignature Objects")

```python
class DspySchemaGuesserSignature(dspy.Signature)

```

Guess a table schema based on business description.

## DspySchemaGuesser Objects[​](#dspyschemaguesser-objects "Direct link to DspySchemaGuesser Objects")

```python
class DspySchemaGuesser(dspy.Module)

```

Guess table schema from business description.

### forward[​](#forward "Direct link to forward")

```python
def forward(
        business_description: str) -> dspy.primitives.prediction.Prediction

```

Guess schema based on business description.

**Arguments**:

* `business_description` - Natural language description of the dataset.

**Returns**:

Prediction containing guessed schema and assumptions.

## DspyRuleSignature Objects[​](#dspyrulesignature-objects "Direct link to DspyRuleSignature Objects")

```python
class DspyRuleSignature(dspy.Signature)

```

Generate data quality rules with improved output format.

## DspyRuleGeneration Objects[​](#dspyrulegeneration-objects "Direct link to DspyRuleGeneration Objects")

```python
class DspyRuleGeneration(dspy.Module)

```

Generate data quality rules.

Now focused solely on rule generation, with schema inference delegated.

### forward[​](#forward-1 "Direct link to forward")

```python
def forward(schema_info: str, business_description: str,
            available_functions: str) -> dspy.primitives.prediction.Prediction

```

Generate data quality rules.

**Arguments**:

* `schema_info` - JSON string containing table schema.
* `business_description` - Natural language description of requirements.
* `available_functions` - JSON string of available check functions.

**Returns**:

Prediction containing quality\_rules and reasoning.

## DspyRuleGenerationWithSchemaInference Objects[​](#dspyrulegenerationwithschemainference-objects "Direct link to DspyRuleGenerationWithSchemaInference Objects")

```python
class DspyRuleGenerationWithSchemaInference(dspy.Module)

```

Combines schema inference and rule generation.

Follows Dependency Inversion Principle by depending on abstractions (protocols).

### forward[​](#forward-2 "Direct link to forward")

```python
def forward(schema_info: str, business_description: str,
            available_functions: str) -> dspy.primitives.prediction.Prediction

```

Generate rules with optional schema inference.

**Arguments**:

* `schema_info` - JSON string of schema (can be empty to trigger inference).
* `business_description` - Natural language requirements.
* `available_functions` - JSON string of available functions.

**Returns**:

Prediction with quality\_rules, reasoning, and optional schema info.

## DspyRuleUsingDataStatsSignature Objects[​](#dspyruleusingdatastatssignature-objects "Direct link to DspyRuleUsingDataStatsSignature Objects")

```python
class DspyRuleUsingDataStatsSignature(dspy.Signature)

```

Generate data quality rules using data summary statistics.

## DspyRuleUsingDataStats Objects[​](#dspyruleusingdatastats-objects "Direct link to DspyRuleUsingDataStats Objects")

```python
class DspyRuleUsingDataStats(dspy.Module)

```

Generate data quality rules using data summary statistics.

### forward[​](#forward-3 "Direct link to forward")

```python
def forward(
    data_summary_stats: str,
    available_functions: str,
    business_description: str | None = None
) -> dspy.primitives.prediction.Prediction

```

Generate data quality rules.

**Arguments**:

* `data_summary_stats` - JSON string containing summary statistics of the data.
* `available_functions` - JSON string of available check functions.
* `business_description` - Optional natural language description of data quality requirements.

**Returns**:

Prediction containing quality\_rules and reasoning.

## LLMRuleCompiler Objects[​](#llmrulecompiler-objects "Direct link to LLMRuleCompiler Objects")

```python
class LLMRuleCompiler()

```

Compiles and optimizes LLM-based data quality rules.

Note: This class assumes DSPy is already configured with a language model. The configuration should be done externally before instantiating this class.

### \_\_init\_\_[​](#__init__-1 "Direct link to __init__")

```python
def __init__(custom_check_functions: dict[str, Callable] | None = None,
             rule_validator: RuleValidator | None = None,
             optimizer: BootstrapFewShotOptimizer | None = None)

```

Initialize the rule compiler.

Note: DSPy must be configured before creating this instance.

**Arguments**:

* `custom_check_functions` - Optional custom check functions.
* `rule_validator` - Optional rule validator instance.
* `optimizer` - Optional optimizer instance.

### model[​](#model "Direct link to model")

```python
@cached_property
def model() -> dspy.Module

```

Get the optimized DSPy model.

**Returns**:

Optimized DSPy module for generating data quality rules.

### model\_using\_data\_stats[​](#model_using_data_stats "Direct link to model_using_data_stats")

```python
@cached_property
def model_using_data_stats() -> dspy.Module

```

Get the optimized DSPy model for generating rules from data summary statistics.

**Returns**:

Optimized DSPy module for generating data quality rules from data stats.
