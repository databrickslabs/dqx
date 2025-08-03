import importlib
import logging
import json
import warnings

from collections.abc import Callable
from presidio_analyzer import AnalyzerEngine

from pyspark.sql import Column
from pyspark.sql.functions import concat_ws, lit, pandas_udf, PandasUDFType

from databricks.connect import DatabricksEnv, DatabricksSession
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_normalized_column_and_expr
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig

logging.getLogger("presidio_analyzer").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

_default_nlp_engine_config = NLPEngineConfig.SPACY_SMALL
_required_modules = ["presidio_analyzer", "spacy"]


@register_rule("row")
def contains_pii(
    column: str | Column,
    language: str = 'en',
    threshold: float = 0.7,
    entities: list[str] | None = None,
    nlp_engine_config: NLPEngineConfig | dict | None = None,
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
    warnings.warn(
        "PII detection uses user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running PII detection."
    )

    if threshold < 0.0 or threshold > 1.0:
        raise ValueError(f"Provided threshold {threshold} must be between 0.0 and 1.0")

    if not nlp_engine_config:
        nlp_engine_config = _default_nlp_engine_config

    config_dict = nlp_engine_config if isinstance(nlp_engine_config, dict) else nlp_engine_config.value
    if not isinstance(config_dict, dict):
        raise ValueError(f"Invalid type provided for 'nlp_engine_config': {type(nlp_engine_config)}")

    _get_connect_env(config_dict)
    entity_detection_udf = _build_detection_udf(config_dict, language, threshold, entities)
    col_str_norm, _, col_expr = _get_normalized_column_and_expr(column)
    entity_info = entity_detection_udf(col_expr)
    condition = entity_info.isNotNull()
    message = concat_ws(" ", lit(f"Column '{col_str_norm}' contains PII:"), entity_info)

    return make_condition(condition=condition, message=message, alias=f"{col_str_norm}_contains_pii")


def _get_connect_env(nlp_engine_config: dict) -> None:
    """
    Gets a Databricks Connect environment where PII detection dependencies are pre-loaded on
    the Spark cluster for use by user-defined functions. Modifies the existing `SparkSession`.

    :param nlp_engine_config: Dictionary configuring the NLP engine used for PII detection
    """
    import spacy  # pylint: disable=import-outside-toplevel

    warnings.warn(
        "PII detection checks require installation of the "
        f"following libraries: {_required_modules}; Updating SparkSession"
    )

    modules = _required_modules.copy()
    for model_name in _get_model_names(nlp_engine_config):
        spacy.load(model_name)
        importlib.import_module(model_name)
        modules.append(model_name)

    env = DatabricksEnv().withDependencies(modules)
    DatabricksSession.builder.withEnvironment(env).getOrCreate()


def _get_model_names(nlp_engine_config: dict) -> list[str]:
    """
    Gets a PII detection model name from configuration as `NLPEngineConfig` or Python dictionary.

    :param nlp_engine_config: Dictionary configuring the NLP engine used for PII detection
    :return: Name of the PII detection model as `str`
    """
    if not isinstance(nlp_engine_config, dict):
        raise ValueError(f"Invalid type provided for 'nlp_engine_config': {type(nlp_engine_config)}")

    return [model["model_name"] for model in nlp_engine_config["models"]]


def _get_analyzer(nlp_engine_config: dict) -> AnalyzerEngine:
    """
    Gets an `AnalyzerEngine` for use with PII detection checks.

    :param nlp_engine_config: Optional dictionary configuring the NLP engine used for PII detection
    :return: Presidio `AnalyzerEngine`
    """
    from presidio_analyzer.analyzer_engine import (  # pylint: disable=import-outside-toplevel, redefined-outer-name, reimported
        AnalyzerEngine,
    )
    from presidio_analyzer.nlp_engine import NlpEngineProvider  # pylint: disable=import-outside-toplevel

    provider = NlpEngineProvider(nlp_configuration=nlp_engine_config)
    nlp_engine = provider.create_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

    return analyzer


def _build_detection_udf(
    nlp_engine_config: dict, language: str, threshold: float, entities: list[str] | None
) -> Callable:
    """
    Builds a UDF with the provided threshold, entities, language, and analyzer.

    :param nlp_engine_config: Dictionary configuring the NLP engine used for PII detection
    :param language: Language of the text
    :param threshold: Confidence threshold for named entity detection (0.0 to 1.0)
    :param entities: List of entities to detect
    :return: PySpark UDF which can be called to detect PII with the given configuration
    """

    @pandas_udf("string", PandasUDFType.SCALAR)
    def handler(batch):
        analyzer = _get_analyzer(nlp_engine_config)
        return batch.map(lambda text: _detect_named_entities(text, analyzer, language, threshold, entities))

    return handler


def _detect_named_entities(
    text: str, analyzer: AnalyzerEngine, language: str, threshold: float, entities: list[str] | None
) -> str | None:
    """
    Detects named entities in the input text using a Presidio analyzer.

    :param text: Input text to analyze for named entities
    :param analyzer: Presidio `AnalyzerEngine` used for named entity detection
    :param language: Language of the text
    :param threshold: Confidence threshold for named entity detection (0.0 to 1.0)
    :param entities: List of entities to detect
    :return: PySpark UDF which can be called to detect PII with the given configuration
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

    return json.dumps(
        [
            {
                "entity_type": result.entity_type,
                "score": float(result.score),
                "text": text[result.start : result.end],
            }
            for result in qualified_results
        ]
    )
