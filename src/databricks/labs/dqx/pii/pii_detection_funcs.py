import contextlib
import io
import logging
import json
import os
import re
import warnings
from collections.abc import Callable
import pandas as pd  # type: ignore[import-untyped]

import pyspark.sql.connect.session
import spacy
from spacy.cli import download
from spacy.util import is_package
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

from pyspark.sql import Column
from pyspark.sql.functions import concat_ws, lit, pandas_udf

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, get_normalized_column_and_expr
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig
from databricks.labs.dqx.errors import MissingParameterError, InvalidParameterError

logging.getLogger("presidio_analyzer").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

_default_nlp_engine_config = NLPEngineConfig.SPACY_SMALL


@register_rule("row")
def does_not_contain_pii(
    column: str | Column,
    language: str = "en",
    threshold: float = 0.7,
    entities: list[str] | None = None,
    nlp_engine_config: NLPEngineConfig | dict | None = None,
) -> Column:
    """
    Check if a column contains personally-identifying information (PII). Uses Microsoft Presidio to detect various named
    entities (e.g. PERSON, ADDRESS, EMAIL_ADDRESS). If PII is detected, the message includes a JSON string with the
    entity types, location within the string, and confidence score from the model.

    Args:
        column: Column to check; can be a string column name or a column expression
        language: Optional language of the text (default: 'en')
        threshold: Confidence threshold for PII detection (0.0 to 1.0, default: 0.7)
            Higher values = less sensitive, fewer false positives
            Lower values = more sensitive, more potential false positives
        entities: Optional list of entities to detect
        nlp_engine_config: Optional NLP engine configuration used for PII detection;
        Can be *NLPEngineConfiguration* or *dict* in the format:
        ```
        {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
        }
        ```

    Returns:
        Column object for condition that fails when PII is detected

    Raises:
        InvalidParameterError: if `threshold` is not between 0.0 and 1.0, or if `nlp_engine_config` is not a
            valid *NLPEngineConfig* or *dict*.
        MissingParameterError: if `nlp_engine_config` is missing the required `nlp_engine_name` key.
        ImportError: if the environment is not supported (e.g., running with Databricks Connect).
    """
    warnings.warn(
        "PII detection uses pandas user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running PII detection.",
        UserWarning,
    )

    if threshold < 0.0 or threshold > 1.0:
        raise InvalidParameterError(f"Provided threshold {threshold} must be between 0.0 and 1.0")

    if not nlp_engine_config:
        nlp_engine_config = _default_nlp_engine_config

    if not isinstance(nlp_engine_config, dict | NLPEngineConfig):
        raise InvalidParameterError(f"Invalid type provided for 'nlp_engine_config': {type(nlp_engine_config)}")

    nlp_engine_config_dict = nlp_engine_config if isinstance(nlp_engine_config, dict) else nlp_engine_config.value
    _ensure_nlp_models_available(nlp_engine_config_dict)
    _validate_environment()

    entity_detection_udf = _build_detection_udf(nlp_engine_config_dict, language, threshold, entities)
    col_str_norm, _, col_expr = get_normalized_column_and_expr(column)
    entity_info = entity_detection_udf(col_expr)
    condition = entity_info.isNotNull()
    message = concat_ws(" ", lit(f"Column '{col_str_norm}' contains PII:"), entity_info)

    return make_condition(condition=condition, message=message, alias=f"{col_str_norm}_contains_pii")


def _validate_environment() -> None:
    """
    Validates that the environment can run PII detection checks which use Python dependencies.

    As of Databricks Connect 17.1, strict limits are imposed on the size of dependencies for
    user-defined functions. UDFs will fail with out-of-memory errors if these limits are exceeded.

    Because of this limitation, we limit use of PII detection checks to local Spark or a Databricks
    workspace. Databricks Connect uses a *pyspark.sql.connect.session.SparkSession* with an external
    host (e.g. 'https://hostname.cloud.databricks.com'). To raise a clear error message, we check
    the session and intentionally fail if *does_not_contain_pii* is called using Databricks Connect.
    """
    connect_session_pattern = re.compile(r"127.0.0.1|.*grpc.sock")
    session = pyspark.sql.SparkSession.builder.getOrCreate()
    if isinstance(session, pyspark.sql.connect.session.SparkSession) and not connect_session_pattern.search(
        session.client.host
    ):
        raise ImportError("'does_not_contain_pii' is not supported when running checks with Databricks Connect")


def _build_detection_udf(
    nlp_engine_config: dict, language: str, threshold: float, entities: list[str] | None
) -> Callable:
    """
    Builds a UDF with the provided threshold, entities, language, and analyzer.

    Args:
        nlp_engine_config: Dictionary configuring the NLP engine used for PII detection
        language: Language of the text
        threshold: Confidence threshold for named entity detection (0.0 to 1.0)
        entities: List of entities to detect

    Returns:
        PySpark UDF which can be called to detect PII with the given configuration
    """

    @pandas_udf("string")  # type: ignore[call-overload]
    def handler(batch: pd.Series) -> pd.Series:
        def _get_analyzer() -> AnalyzerEngine:
            """
            Gets an *AnalyzerEngine* for use with PII detection checks.

            Returns:
                Presidio *AnalyzerEngine*
            """
            provider = NlpEngineProvider(nlp_configuration=nlp_engine_config)
            nlp_engine = provider.create_engine()
            return AnalyzerEngine(nlp_engine=nlp_engine)

        analyzer = _get_analyzer()

        def _detect_named_entities(text: str) -> str | None:
            """
            Detects named entities in the input text using a Presidio analyzer.

            Args:
                text: Input text to analyze for named entities

            Returns:
                JSON string with detected entities, or *None* if no entities are found
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
            if not qualified_results:
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

        return batch.map(_detect_named_entities)

    return handler


def _load_nlp_spacy_model(name: str):
    """
    Lazily loads a spaCy model and download if not available.

    This has to be used carefully when loading larger models to avoid out-of-memory issues
    due to memory limitation of Databricks Connect. For larger models, it is recommended to pre-install them
    via pip instead of relying on DQX to download them at runtime.

    Args:
        name: spaCy model package name (e.g., en_core_web_sm)

    Returns:
        Loaded spaCy Language instance
    """
    # Silence pip version check to avoid unnecessary warnings.
    original_pip_check = os.environ.get("PIP_DISABLE_PIP_VERSION_CHECK")
    os.environ["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"

    try:
        if not is_package(name):
            # Silence stdout/stderr during download to avoid cluttering logs.
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                download(name)  # Download the model if not available
        return spacy.load(name)
    finally:
        if original_pip_check is None:
            os.environ.pop("PIP_DISABLE_PIP_VERSION_CHECK", None)
        else:
            os.environ["PIP_DISABLE_PIP_VERSION_CHECK"] = original_pip_check


def _ensure_nlp_models_available(nlp_engine_config: dict) -> None:
    """
    Ensures all nlp models referenced by the provided NLP engine configuration are available locally.

    Args:
        nlp_engine_config: Dictionary with "models" list entries containing model_name.

    Raises:
        MissingParameterError: if `nlp_engine_config` is missing the required `nlp_engine_name` key.
    """
    nlp_engine_name = nlp_engine_config.get("nlp_engine_name")
    if not nlp_engine_name:
        raise MissingParameterError(f"Missing 'nlp_engine_name' key in nlp_engine_config: {nlp_engine_config}")

    models = nlp_engine_config.get("models") or []
    for model in models:
        model_name = model.get("model_name")
        if model_name is not None:
            if nlp_engine_name == "spacy":
                _load_nlp_spacy_model(model_name)
