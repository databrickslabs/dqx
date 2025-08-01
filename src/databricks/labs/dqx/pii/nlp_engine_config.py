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

    STANFORD_DEIDENTIFIER_BASE = {
        'nlp_engine_name': 'transformers_stanford_deidentifier_base',
        'models': [
            {
                'lang_code': 'en',
                'model_name': {'spacy': 'en_core_web_sm', 'transformers': 'StanfordAIMI/stanford-deidentifier-base'},
            }
        ],
        'ner_model_configuration': {
            'labels_to_ignore': ['O'],
            'aggregation_strategy': 'max',
            'stride': 16,
            'alignment_mode': 'expand',
            'model_to_presidio_entity_mapping': {
                'PER': 'PERSON',
                'LOC': 'LOCATION',
                'ORG': 'ORGANIZATION',
                'AGE': 'AGE',
                'ID': 'ID',
                'EMAIL': 'EMAIL',
                'PATIENT': 'PERSON',
                'STAFF': 'PERSON',
                'HOSP': 'ORGANIZATION',
                'PATORG': 'ORGANIZATION',
                'DATE': 'DATE_TIME',
                'PHONE': 'PHONE_NUMBER',
                'HCW': 'PERSON',
                'HOSPITAL': 'LOCATION',
                'VENDOR': 'ORGANIZATION',
            },
            'low_confidence_score_multiplier': 0.4,
            'low_score_entity_names': ['ID'],
        },
    }
