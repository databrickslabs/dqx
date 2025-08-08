from enum import Enum


class NLPEngineConfig(Enum):
    """Enum class defining various NLP engine configurations for PII detection."""

    SPACY_SMALL = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
    }

    SPACY_MEDIUM = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_md"}],
    }

    SPACY_LARGE = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
