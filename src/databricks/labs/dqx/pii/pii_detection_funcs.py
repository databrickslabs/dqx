import logging
from collections.abc import Callable

from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

from pyspark.sql import Column
from pyspark.sql.functions import concat_ws, lit, to_json, udf
from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType, StructType, StructField

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_norm_column_and_expr
from databricks.labs.dqx.pii.config import NLPEngineConfig

logger = logging.getLogger(__name__)

_default_nlp_engine_config = NLPEngineConfig.SPACY_MEDIUM
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


@register_rule("row")
def contains_pii(
    column: str | Column,
    language: str = 'en',
    threshold: float = 0.7,
    entities: list[str] | None = None,
    nlp_engine_config: NLPEngineConfig | None = None,
) -> Column:
    """
    Check if a column contains personally-identifying information (PII). Uses Microsoft Presidio to detect various named
    entities (e.g. PERSON, ADDRESS, EMAIL_ADDRESS). If PII is detected, the message includes a JSON string with the
    entity types, location within the string, and confidence score from the model.

    :param column: Column to check; can be a string column name or a column expression
    :param language: Optional language of the text (default: 'en')
    :param threshold: Confidence threshold for PII detection (0.0 to 1.0, default: 0.7)
                     Higher values = less sensitive, fewer false positives
                     Lower values = more sensitive, more potential false positives
    :param entities: Optional list of entities to detect
    :param nlp_engine_config: Optional NLP engine configuration used for PII detection; Can be `dict` or `NLPEngineConfiguration`
    :return: Column object for condition that fails when PII is detected
    """
    if threshold < 0.0 or threshold > 1.0:
        raise ValueError(f"Provided threshold {threshold} must be between 0.0 and 1.0")

    analyzer = _get_analyzer(nlp_engine_config)
    entity_detection_udf = _build_detection_udf(analyzer, language, threshold, entities)
    col_str_norm, _, col_expr = _get_norm_column_and_expr(column)
    entity_info = entity_detection_udf(col_expr)
    condition = entity_info.isNotNull()
    message = concat_ws(" ", lit(f"Column '{col_str_norm}' contains PII:"), to_json(entity_info))

    return make_condition(condition=condition, message=message, alias=f"{col_str_norm}_contains_pii")


def _get_analyzer(nlp_engine_config: NLPEngineConfig | dict | None) -> AnalyzerEngine:
    """
    Gets an `AnalyzerEngine` for use with PII detection checks.

    :param nlp_engine_config: Optional dictionary configuring the NLP engine used for PII detection
    :return: Presidio `AnalyzerEngine`
    """
    if not nlp_engine_config:
        nlp_engine_config = _default_nlp_engine_config

    nlp_engine_config_params = (
        nlp_engine_config.value if isinstance(nlp_engine_config, NLPEngineConfig) else nlp_engine_config
    )

    if not isinstance(nlp_engine_config_params, dict):
        raise TypeError(f"Invalid type for provided 'nlp_engine_config': {type(nlp_engine_config_params)}")

    provider = NlpEngineProvider(nlp_configuration=nlp_engine_config_params)
    nlp_engine = provider.create_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

    return analyzer


def _build_detection_udf(
    analyzer: AnalyzerEngine, language: str, threshold: float, entities: list[str] | None
) -> Callable:
    """
    Builds a UDF with the provided threshold, entities, language, and analyzer.

    :param analyzer: Presidio `AnalyzerEngine` used for named entity detection
    :param language: Language of the text
    :param threshold: Confidence threshold for named entity detection (0.0 to 1.0)
    :param entities: List of entities to detect
    :return: PySpark UDF which can be called to detect PII with the given configuration
    """
    return udf(
        lambda text: _detect_named_entities(text, analyzer, language, threshold, entities),
        returnType=_detection_result_type,
        useArrow=True,
    )


def _detect_named_entities(
    text: str, analyzer: AnalyzerEngine, language: str, threshold: float, entities: list[str] | None
) -> list[dict] | None:
    """
    Detects named entities in the input text using Presidio.

    :param text: Text to analyze for named entities
    :param analyzer: Presidio `AnalyzerEngine` used for named entity detection
    :param language: Language of the text
    :param threshold: Confidence threshold for named entity detection (0.0 to 1.0)
    :param entities: List of entities to detect
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
            "score": float(result.score),
            "text": text[result.start:result.end],
        }
        for result in qualified_results
    ]
