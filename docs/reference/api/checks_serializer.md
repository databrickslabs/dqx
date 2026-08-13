# databricks.labs.dqx.checks\_serializer

### project\_to\_check\_schema[​](#project_to_check_schema "Direct link to project_to_check_schema")

```python
def project_to_check_schema(check: dict) -> dict

```

Return a copy of *check* containing only the logical check-metadata keys.

Storage backends persist columns alongside the check that are not part of the check metadata accepted by *apply\_checks\_by\_metadata* (e.g. *run\_config\_name*, *created\_at*, *rule\_fingerprint*, *rule\_set\_fingerprint*). Loading a check therefore yields those extra keys. This helper drops them so a loaded check round-trips cleanly.

Both load paths funnel their assembled check dict through this single projection so they agree on the loaded shape: the Lakebase path (*LakebaseChecksStorageHandler.\_load\_checks\_from\_lakebase*) and the Delta path (*DataFrameConverter.from\_dataframe*). The allowed keys are derived from *CheckSpec.model\_fields*, so a new logical field added to the schema is retained by both paths automatically — provided the backing store also carries it (adding a *CheckSpec* field still requires a matching Delta table column and Lakebase column for the value to survive a round-trip).

**Arguments**:

* `check` - A check dict that may carry storage-only keys.

**Returns**:

A new dict with only the keys defined by *CheckSpec*.

## ChecksNormalizer Objects[​](#checksnormalizer-objects "Direct link to ChecksNormalizer Objects")

```python
class ChecksNormalizer()

```

Handles normalization and denormalization of check dictionaries. E.g. responsible for converting Decimal values to/from serializable format.

### normalize[​](#normalize "Direct link to normalize")

```python
@staticmethod
def normalize(checks: list[dict]) -> list[dict]

```

Recursively normalize checks dictionary to make it JSON/YAML serializable.

**Arguments**:

* `checks` - List of check dictionaries that may contain non-serializable values.

**Returns**:

List of normalized check dictionaries.

### denormalize\_value[​](#denormalize_value "Direct link to denormalize_value")

```python
@staticmethod
def denormalize_value(val: Any) -> Any

```

Recursively convert special markers (e.g. Decimal) back to original objects.

### denormalize[​](#denormalize "Direct link to denormalize")

```python
@staticmethod
def denormalize(checks: list[dict]) -> list[dict]

```

Recursively convert special markers back to objects after deserialization. Converts special markers (e.g., **decimal** format) back to Decimal objects.

**Arguments**:

* `checks` - List of check dictionaries that may contain special markers.

**Returns**:

List of check dictionaries with special markers converted to objects.

## FileFormatSerializer Objects[​](#fileformatserializer-objects "Direct link to FileFormatSerializer Objects")

```python
class FileFormatSerializer(ABC)

```

Abstract base class for file format serializers.

### serialize[​](#serialize "Direct link to serialize")

```python
@abstractmethod
def serialize(data: list[dict]) -> str

```

Serialize data to string format.

### deserialize[​](#deserialize "Direct link to deserialize")

```python
@abstractmethod
def deserialize(file_like: TextIO) -> list[dict]

```

Deserialize data from file-like object.

## JsonSerializer Objects[​](#jsonserializer-objects "Direct link to JsonSerializer Objects")

```python
class JsonSerializer(FileFormatSerializer)

```

JSON format serializer implementation.

### serialize[​](#serialize-1 "Direct link to serialize")

```python
def serialize(data: list[dict]) -> str

```

Serialize data to JSON string.

### deserialize[​](#deserialize-1 "Direct link to deserialize")

```python
def deserialize(file_like: TextIO) -> list[dict]

```

Deserialize data from JSON file.

## YamlSerializer Objects[​](#yamlserializer-objects "Direct link to YamlSerializer Objects")

```python
class YamlSerializer(FileFormatSerializer)

```

YAML format serializer implementation.

### serialize[​](#serialize-2 "Direct link to serialize")

```python
def serialize(data: list[dict]) -> str

```

Serialize data to YAML string.

### deserialize[​](#deserialize-2 "Direct link to deserialize")

```python
def deserialize(file_like: TextIO) -> list[dict]

```

Deserialize data from YAML file.

## SerializerFactory Objects[​](#serializerfactory-objects "Direct link to SerializerFactory Objects")

```python
class SerializerFactory()

```

Factory for creating appropriate serializers based on file extension.

### get\_supported\_extensions[​](#get_supported_extensions "Direct link to get_supported_extensions")

```python
@classmethod
def get_supported_extensions(cls) -> tuple[str, ...]

```

Get tuple of supported file extensions.

**Returns**:

Tuple of supported file extensions (e.g., (".json", ".yaml", ".yml")).

### create\_serializer[​](#create_serializer "Direct link to create_serializer")

```python
@classmethod
def create_serializer(cls,
                      extension: str | None = None) -> FileFormatSerializer

```

Create a serializer based on file extension.

**Arguments**:

* `extension` - File extension (e.g., ".json", ".yaml", ".yml"). If None or empty, defaults to YAML.

**Returns**:

Appropriate serializer instance. Defaults to YAML if extension not recognized or not provided.

### register\_format[​](#register_format "Direct link to register_format")

```python
@classmethod
def register_format(cls, extension: str,
                    serializer_class: type[FileFormatSerializer]) -> None

```

Register a new file format serializer.

**Arguments**:

* `extension` - File extension
* `serializer_class` - Serializer class implementing FileFormatSerializer interface.

## ChecksSerializer Objects[​](#checksserializer-objects "Direct link to ChecksSerializer Objects")

```python
class ChecksSerializer()

```

Handles serialization of DQRule objects to dictionaries and file formats.

### serialize[​](#serialize-3 "Direct link to serialize")

```python
@staticmethod
def serialize(checks: list[DQRule]) -> list[dict]

```

Converts a list of quality checks defined as *DQRule* objects to a list of quality checks defined as Python dictionaries.

**Arguments**:

* `checks` - List of DQRule instances to convert.

**Returns**:

List of dictionaries representing the DQRule instances.

**Raises**:

* `InvalidCheckError` - If any item in the list is not a DQRule instance.

### serialize\_to\_bytes[​](#serialize_to_bytes "Direct link to serialize_to_bytes")

```python
@staticmethod
def serialize_to_bytes(checks: list[dict], extension: str) -> bytes

```

Serializes a list of checks to bytes in json or yaml (default) format.

**Arguments**:

* `checks` - List of checks to serialize.
* `extension` - File extension (e.g., ".json", ".yaml", ".yml").

**Returns**:

Serialized checks as bytes.

## ChecksDeserializer Objects[​](#checksdeserializer-objects "Direct link to ChecksDeserializer Objects")

```python
class ChecksDeserializer()

```

Handles deserialization of dictionaries to DQRule objects and from file formats.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(custom_checks: dict[str, Callable] | None = None)

```

Initialize the deserializer.

**Arguments**:

* `custom_checks` - Dictionary with custom check functions.

### deserialize[​](#deserialize-3 "Direct link to deserialize")

```python
def deserialize(checks: list[dict]) -> list[DQRule]

```

Converts a list of quality checks defined as Python dictionaries to a list of *DQRule* objects.

**Arguments**:

* `checks` - list of dictionaries describing checks. Each check is a dictionary consisting of following fields:

  <!-- -->

  * *check* - Column expression to evaluate. This expression should return string value if it's evaluated to true or *null* if it's evaluated to *false*
  * *name* - name that will be given to a resulting column. Autogenerated if not provided
  * *criticality* (optional) - possible values are *error* (data going only into "bad" dataframe), and *warn* (data is going into both dataframes)
  * *filter* (optional) - Expression for filtering data quality checks
  * *user\_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.

**Returns**:

list of data quality check rules

**Raises**:

* `InvalidCheckError` - If any dictionary is invalid or unsupported.

### deserialize\_from\_file[​](#deserialize_from_file "Direct link to deserialize_from_file")

```python
@staticmethod
def deserialize_from_file(extension: str, file_like: TextIO) -> list[dict]

```

Deserialize checks from a file-like object based on file extension. Automatically denormalizes special markers back to objects.

**Arguments**:

* `extension` - File extension (e.g., ".json", ".yaml", ".yml").
* `file_like` - File-like object to read from.

**Returns**:

List of check dictionaries with special markers converted to objects.

### serialize\_checks[​](#serialize_checks "Direct link to serialize_checks")

```python
def serialize_checks(checks: list[DQRule]) -> list[dict]

```

Converts a list of quality checks defined as *DQRule* objects to a list of quality checks defined as Python dictionaries.

This is a convenience user-friendly function that wraps ChecksSerializer.serialize.

**Arguments**:

* `checks` - List of DQRule instances to convert.

**Returns**:

List of dictionaries representing the DQRule instances.

**Raises**:

* `InvalidCheckError` - If any item in the list is not a DQRule instance.

### deserialize\_checks[​](#deserialize_checks "Direct link to deserialize_checks")

```python
def deserialize_checks(
        checks: list[dict],
        custom_checks: dict[str, Callable] | None = None) -> list[DQRule]

```

Converts a list of quality checks defined as Python dictionaries to a list of DQRule objects.

This is a convenience user-friendly function that wraps ChecksDeserializer.deserialize.

**Arguments**:

* `checks` - list of dictionaries describing checks. Each check is a dictionary consisting of following fields:

  <!-- -->

  * *check* - Column expression to evaluate. This expression should return string value if it's evaluated to true or *null* if it's evaluated to *false*
  * *name* - name that will be given to a resulting column. Autogenerated if not provided
  * *criticality* (optional) - possible values are *error* (data going only into "bad" dataframe), and *warn* (data is going into both dataframes)
  * *filter* (optional) - Expression for filtering data quality checks
  * *user\_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.

* `custom_checks` - Dictionary with custom check functions.

**Returns**:

list of data quality check rules

**Raises**:

* `InvalidCheckError` - If any dictionary is invalid or unsupported.
