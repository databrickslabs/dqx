from collections.abc import Callable
from dataclasses import dataclass, field

import dspy  # type: ignore


@dataclass(frozen=True)
class DspyOptimizerConfig:
    """Configuration for DSPy optimizer."""

    max_bootstrapped_demos: int = 2
    max_labeled_demos: int = 5
    teacher_settings: dict = field(default_factory=dict)


class BootstrapFewShotOptimizer:
    """
    Bootstrap few-shot optimizer implementation.
    """

    def __init__(self, config: DspyOptimizerConfig | None = None):
        self._config = config or DspyOptimizerConfig()

    def compile(self, module: dspy.Module, trainset: list[dspy.Example], metric: Callable) -> dspy.Module:
        """
        Compile and optimize a DSPy module using bootstrap few-shot learning.

        Args:
            module: DSPy module to optimize.
            trainset: Training examples for optimization.
            metric: Metric function for evaluation.

        Returns:
            Optimized DSPy module.
        """
        optimizer = dspy.BootstrapFewShot(
            metric=metric,
            max_bootstrapped_demos=self._config.max_bootstrapped_demos,
            max_labeled_demos=self._config.max_labeled_demos,
            teacher_settings=self._config.teacher_settings,
        )

        return optimizer.compile(module, trainset=trainset)
