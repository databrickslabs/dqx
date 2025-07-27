import logging

from pyspark.sql import Column
from pyspark.sql.functions import call_udf, concat_ws, lit, to_json, udf
from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType, StructType, StructField

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_norm_column_and_expr

try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_analyzer.nlp_engine import NlpEngineProvider
except ImportError as e:
    raise ImportError(
        "Presidio dependencies not found. Install with: pip install 'databricks-labs-dqx[pii-detection]'"
    ) from e

logger = logging.getLogger(__name__)

default_nlp_configuration = {
    "nlp_engine_name": "spacy",
    "models": [{"lang_code": "en", "model_name": "en_core_web_md"}],
}
provider = NlpEngineProvider(nlp_configuration=default_nlp_configuration)
nlp_engine = provider.create_engine()
analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

_detection_result_type = ArrayType(
    StructType(
        [
            StructField("entity_type", StringType(), True),
            StructField("start", IntegerType(), True),
            StructField("end", IntegerType(), True),
            StructField("score", FloatType(), True),
            StructField("text", StringType(), True),
        ]
    )
)


def _detect_named_entities(text: str, threshold: float, entities: list[str], language: str) -> list[dict] | None:
    """
    Detects named entities in the input text using Presidio.

    :param text: Text to analyze for named entities
    :param threshold: Confidence threshold for named entity detection (0.0 to 1.0)
    :param entities: List of entities to detect
    :param language: Language of the text
    :return: JSON string with detected named entities or None if no named entities found
    """
    if not text:
        return None

    results = analyzer.analyze(
        text=text,
        entities=entities,
        language=language,
        score_threshold=threshold,
    )

    qualified_results = [result for result in results if result.score >= threshold]
    if not qualified_results or len(qualified_results) == 0:
        return None

    return [
        {
            "entity_type": result.entity_type,
            "start": int(result.start),
            "end": int(result.end),
            "score": float(result.score),
            "text": text[result.start : result.end],
        }
        for result in qualified_results
    ]


@register_rule("row")
def contains_pii(
    column: str | Column, threshold: float = 0.7, entities: list[str] | None = None, language: str = 'en'
) -> Column:
    """
    Check if a column contains personally-identifying information (PII).

    This function uses Microsoft Presidio to detect various types of PII including:
    - Person names
    - Email addresses
    - Phone numbers
    - Credit card numbers
    - Social Security Numbers
    - Passport numbers
    - IP addresses
    - Dates and times
    - Locations
    - Organizations

    :param column: Column to check; can be a string column name or a column expression
    :param threshold: Confidence threshold for PII detection (0.0 to 1.0, default: 0.7)
                     Higher values = less sensitive, fewer false positives
                     Lower values = more sensitive, more potential false positives
    :param entities: Optional list of entities to detect
    :param language: Optional language of the text (default: 'en')
    :return: Column object for condition that fails when PII is detected
    """
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(f"Threshold must be between 0.0 and 1.0, got {threshold}")

    @udf(returnType=_detection_result_type)
    def _detect_named_entities_udf(value):
        return _detect_named_entities(value, threshold, entities, language)

    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    pii_info = call_udf("_detect_named_entities_udf", col_expr)
    condition = pii_info.isNotNull()
    message = concat_ws(' ', lit(f"Column '{col_expr_str}' contains PII:"), to_json(pii_info))

    return make_condition(condition=condition, message=message, alias=f"{col_str_norm}_contains_pii")
