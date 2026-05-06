# databricks.labs.dqx.pii.nlp\_engine\_config

## NLPEngineConfig Objects[​](#nlpengineconfig-objects "Direct link to NLPEngineConfig Objects")

```python
class NLPEngineConfig(Enum)

```

Enum class defining various NLP engine configurations for PII detection.

Note that DQX automatically installs the built-in entity recognition models at runtime if they are not already available. However, for better performance and to avoid potential out-of-memory issues, it is recommended to pre-install models using pip install.

**Members**:

* `SPACY_SMALL`: Uses spaCy's [en\_core\_web\_sm](https://spacy.io/models/en#en_core_web_sm) for entity recognition
* `SPACY_SMALL`: Uses spaCy's [en\_core\_web\_md](https://spacy.io/models/en#en_core_web_md) for entity recognition
* `SPACY_SMALL`: Uses spaCy's [en\_core\_web\_lg](https://spacy.io/models/en#en_core_web_lg) for entity recognition
