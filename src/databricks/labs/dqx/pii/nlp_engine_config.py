from enum import Enum


class NLPEngineConfig(Enum):
    """
    Enum class defining various NLP engine configurations for PII detection.

    Note that DQX automatically installs the built-in entity recognition models at runtime if they are not already available.
    However, for better performance and to avoid potential out-of-memory issues, it is recommended to pre-install models using pip install.

    **Members**:
    * `SPACY_SMALL`: Uses spaCy's [en_core_web_sm](https://spacy.io/models/en#en_core_web_sm) for entity recognition
    * `SPACY_SMALL`: Uses spaCy's [en_core_web_md](https://spacy.io/models/en#en_core_web_md) for entity recognition
    * `SPACY_SMALL`: Uses spaCy's [en_core_web_lg](https://spacy.io/models/en#en_core_web_lg) for entity recognition
    """

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
