---
sidebar_label: nlp_engine_config
title: databricks.labs.dqx.pii.nlp_engine_config
---

## NLPEngineConfig Objects

```python
class NLPEngineConfig(Enum)
```

Enum class defining various NLP engine configurations for PII detection.

Note that DQX automatically installs the built-in entity recognition models at runtime if they are not already available.
However, for better performance and to avoid potential out-of-memory issues, it is recommended to pre-install models using pip install.

**Members**:
* `SPACY_SMALL`: Uses spaCy&#x27;s [en_core_web_sm](https://spacy.io/models/en#en_core_web_sm) for entity recognition
* `SPACY_SMALL`: Uses spaCy&#x27;s [en_core_web_md](https://spacy.io/models/en#en_core_web_md) for entity recognition
* `SPACY_SMALL`: Uses spaCy&#x27;s [en_core_web_lg](https://spacy.io/models/en#en_core_web_lg) for entity recognition

