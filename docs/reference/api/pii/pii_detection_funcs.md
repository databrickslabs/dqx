# databricks.labs.dqx.pii.pii\_detection\_funcs

### does\_not\_contain\_pii[​](#does_not_contain_pii "Direct link to does_not_contain_pii")

```python
@register_rule("row")
def does_not_contain_pii(
        column: str | Column,
        language: str = "en",
        threshold: float = 0.7,
        entities: list[str] | None = None,
        nlp_engine_config: NLPEngineConfig | dict | None = None) -> Column

```

Check if a column contains personally-identifying information (PII). Uses Microsoft Presidio to detect various named entities (e.g. PERSON, ADDRESS, EMAIL\_ADDRESS). If PII is detected, the message includes a JSON string with the entity types, location within the string, and confidence score from the model.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `language` - Optional language of the text (default: 'en')
* `threshold` - Confidence threshold for PII detection (0.0 to 1.0, default: 0.7) Higher values = less sensitive, fewer false positives Lower values = more sensitive, more potential false positives
* `entities` - Optional list of entities to detect
* `nlp_engine_config` - Optional NLP engine configuration used for PII detection; Can be *NLPEngineConfiguration* or *dict* in the format:
  <!-- -->
  ```text
  {
      "nlp_engine_name": "spacy",
      "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
  }

  ```

**Returns**:

Column object for condition that fails when PII is detected

**Raises**:

* `InvalidParameterError` - if `threshold` is not between 0.0 and 1.0, or if `nlp_engine_config` is not a valid *NLPEngineConfig* or *dict*.
* `MissingParameterError` - if `nlp_engine_config` is missing the required `language`1 key.
* `language`2 - if the environment is not supported (e.g., running with Databricks Connect).
