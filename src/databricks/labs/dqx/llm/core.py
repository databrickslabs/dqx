import dspy
from dspy.teleprompt import BootstrapFewShot
from databricks.labs.dqx.llm.utils import create_optimizer_training_set
import yaml
import logging

logger = logging.getLogger(__name__)


class RuleSignature(dspy.Signature):
    """Generate a yaml configuration based on user input and provided examples."""

    schema_info: str = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    user_input: str = dspy.InputField(desc="Natural language description of the desired data quality requirements")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc="""Generated Data Quality Rules in the yaml format. Each rule must follow this exact structure:
- criticality: error|warn
  check:
    function: <function_name>
    arguments:
      column: <column_name>
      [additional_args: <values>]

rules:
1. Use integers for numeric limits (e.g., min_limit: 1, not min_limit: 0.01)
2. Use correct argument names: min_limit, max_limit
3. For is_in_range: use min_limit and max_limit as integers
4. AVOID regex patterns if possible - use is_in_list for validation instead
5. If regex is absolutely necessary, use simple patterns without special characters
exact examples to refer:
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: customer_id
- criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: first_name
      trim_strings: true
      important: Focus on simple validation rules. Avoid complex regex patterns.
    Do NOT include ```yaml or ``` markers. Return only the YAML content""",
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGen(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(RuleSignature)

    def forward(self, schema_info: str, user_input: str, available_functions: str):
        return self.generator(schema_info=schema_info, user_input=user_input, available_functions=available_functions)


def _configure_dspy_lm(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = None, api_base: str = None
):
    """Configure the Dspy language model.

    :param model: The model to use for the Dspy language model.
    """
    lm = dspy.LM(
        model=model,
        model_type="chat",
        api_key=api_key,
        api_base=api_base,
    )
    dspy.configure(lm=lm)


class AssessDQRules(dspy.Signature):

    schema_info = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description = dspy.InputField(desc="Natural language description of quality requirements")
    expected_rules = dspy.InputField(desc="YAML string of expected quality rules")
    actual_rules = dspy.InputField(desc="YAML string of actual generated quality rules")
    assessment_question = dspy.InputField(desc="Specific question about the quality of the rules")

    assessment_answer: bool = dspy.OutputField(desc="Boolean assessment of the quality")


def validate_generated_rules(expected: str, actual: str) -> float:
    """Validate generated rules against expected rules with better error handling."""
    try:
        # Clean up the actual output - remove markdown code blocks if present
        if "```yaml" in actual:
            actual = actual.split("```yaml")[1].split("```")[0].strip()
        elif "```" in actual:
            actual = actual.split("```")[1].split("```")[0].strip()

        # Fix regex patterns before parsing YAML
        actual = fix_regex_patterns_in_yaml(actual)

        # Parse YAML
        expected_rules = yaml.safe_load(expected)
        actual_rules = yaml.safe_load(actual)

        if not actual_rules:
            return 0.0

        # Basic DQX validation
        from databricks.labs.dqx.engine import DQEngine

        validation_status = DQEngine.validate_checks(actual_rules)

        if validation_status.has_errors:
            print(f"DQX validation errors: {validation_status.errors}")
            # Try to fix common issues in the LLM output
            fixed_rules = fix_common_llm_issues(actual_rules)
            if fixed_rules:
                validation_status = DQEngine.validate_checks(fixed_rules)
                if not validation_status.has_errors:
                    actual_rules = fixed_rules
                    print("Fixed common LLM issues")
                else:
                    return 0.0
            else:
                return 0.0

        # Calculate similarity score
        score = 0.0
        total_checks = 0

        # Compare each rule
        for expected_rule in expected_rules:
            total_checks += 1
            for actual_rule in actual_rules:
                if expected_rule.get('criticality') == actual_rule.get('criticality') and expected_rule.get(
                    'check', {}
                ).get('function') == actual_rule.get('check', {}).get('function'):
                    score += 1.0
                    break

        if total_checks == 0:
            return 0.0

        return score / total_checks

    except Exception as e:
        print(f"Validation error: {e}")
        return 0.0


def fix_regex_patterns_in_yaml(yaml_content):
    """Fix regex patterns in YAML content before parsing."""
    try:
        fixed_content = yaml_content

        # Simple string replacements for common regex issues
        replacements = [
            # Fix email pattern - escape the dot properly
            (
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
            ),
            # Fix phone pattern - escape backslashes
            ('regex: "^\\+?[1-9]\\d{1,14}$"', 'regex: "^\\+?[1-9]\\d{1,14}$"'),
            # Fix zip code pattern - escape backslashes
            ('regex: "^\\d{5}(-\\d{4})?$"', 'regex: "^\\d{5}(-\\d{4})?$"'),
            # Fix common unescaped patterns
            (
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
            ),
            ('regex: "^\\+?[1-9]\\d{1,14}$"', 'regex: "^\\+?[1-9]\\d{1,14}$"'),
            ('regex: "^\\d{5}(-\\d{4})?$"', 'regex: "^\\d{5}(-\\d{4})?$"'),
        ]

        # Apply each replacement
        for old_pattern, new_pattern in replacements:
            fixed_content = fixed_content.replace(old_pattern, new_pattern)

        # Additional fixes for common issues
        # Fix any unescaped dots in email patterns
        if 'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"' in fixed_content:
            fixed_content = fixed_content.replace(
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
                'regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"',
            )

        # Fix any unescaped backslashes in phone patterns
        if 'regex: "^\\+?[1-9]\\d{1,14}$"' in fixed_content:
            fixed_content = fixed_content.replace('regex: "^\\+?[1-9]\\d{1,14}$"', 'regex: "^\\+?[1-9]\\d{1,14}$"')

        # Fix any unescaped backslashes in zip patterns
        if 'regex: "^\\d{5}(-\\d{4})?$"' in fixed_content:
            fixed_content = fixed_content.replace('regex: "^\\d{5}(-\\d{4})?$"', 'regex: "^\\d{5}(-\\d{4})?$"')

        return fixed_content

    except Exception as e:
        print(f"Error fixing regex patterns: {e}")
        return yaml_content


def fix_common_llm_issues(rules):
    """Fix common issues in LLM-generated rules."""
    try:
        fixed_rules = []
        for rule in rules:
            fixed_rule = rule.copy()
            check = fixed_rule.get('check', {})
            arguments = check.get('arguments', {})

            # Fix float to int conversion for limits
            if 'min_limit' in arguments and isinstance(arguments['min_limit'], float):
                arguments['min_limit'] = int(arguments['min_limit'])
            if 'max_limit' in arguments and isinstance(arguments['max_limit'], float):
                arguments['max_limit'] = int(arguments['max_limit'])

            # Fix regex patterns
            if 'regex' in arguments:
                # Ensure proper escaping
                regex = arguments['regex']
                if '\\' in regex and not regex.startswith('\\\\'):
                    # Fix single backslash issues
                    regex = regex.replace('\\', '\\\\')
                    arguments['regex'] = regex

            fixed_rule['check']['arguments'] = arguments
            fixed_rules.append(fixed_rule)

        return fixed_rules
    except Exception as e:
        print(f"Error fixing LLM issues: {e}")
        return None


def get_dspy_compiler(
    api_key: str = None, api_base: str = None, model: str = "databricks/databricks-meta-llama-3-3-70b-instruct"
) -> DQRuleGen:
    """A utility function to get the Dspy compiler.

    :param custom_check_functions: A dictionary of custom check functions.
        If provided, the function will use the custom check functions to resolve the check function.
        If not provided, the function will use only the built-in check functions.
    """

    if not api_key or not api_base:
        raise ValueError("api_key and api_base must be provided")

    _configure_dspy_lm(api_key=api_key, api_base=api_base, model=model)

    model = DQRuleGen()
    trainset = create_optimizer_training_set()

    optimizer = dspy.BootstrapFewShot(
        metric=lambda x, y, trace=None: validate_generated_rules(x.quality_rules, y.quality_rules)
    )
    optimized_model = optimizer.compile(model, trainset=trainset)

    return optimized_model
