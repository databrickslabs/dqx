# databricks.labs.dqx.llm.optimizers

## DspyOptimizerConfig Objects[​](#dspyoptimizerconfig-objects "Direct link to DspyOptimizerConfig Objects")

```python
@dataclass(frozen=True)
class DspyOptimizerConfig()

```

Configuration for DSPy optimizer.

## BootstrapFewShotOptimizer Objects[​](#bootstrapfewshotoptimizer-objects "Direct link to BootstrapFewShotOptimizer Objects")

```python
class BootstrapFewShotOptimizer()

```

Bootstrap few-shot optimizer implementation.

### compile[​](#compile "Direct link to compile")

```python
def compile(module: dspy.Module, trainset: list[dspy.Example],
            metric: Callable) -> dspy.Module

```

Compile and optimize a DSPy module using bootstrap few-shot learning.

**Arguments**:

* `module` - DSPy module to optimize.
* `trainset` - Training examples for optimization.
* `metric` - Metric function for evaluation.

**Returns**:

Optimized DSPy module.
