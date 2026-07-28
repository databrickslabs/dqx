---
sidebar_label: checks_serializer
title: databricks.labs.dqx.checks_serializer
---

## ChecksNormalizer Objects

```python
class ChecksNormalizer()
```

Handles normalization and denormalization of check dictionaries.
E.g. responsible for converting Decimal values to/from serializable format.

#### normalize

```python
@staticmethod
def normalize(checks: list[dict]) -> list[dict]
```

Recursively normalize checks dictionary to make it JSON/YAML serializable.

**Arguments**:

- `checks` - List of check dictionaries that may contain non-serializable values.
  

**Returns**:

  List of normalized check dictionaries.

#### denormalize\_value

```python
@staticmethod
def denormalize_value(val: Any) -> Any
```

Recursively convert special markers (e.g. Decimal) back to original objects.

#### denormalize

```python
@staticmethod
def denormalize(checks: list[dict]) -> list[dict]
```

Recursively convert special markers back to objects after deserialization.
Converts special markers (e.g., __decimal__ format) back to Decimal objects.

**Arguments**:

- `checks` - List of check dictionaries that may contain special markers.
  

**Returns**:

  List of check dictionaries with special markers converted to objects.

## FileFormatSerializer Objects

```python
class FileFormatSerializer(ABC)
```

Abstract base class for file format serializers.

#### serialize

```python
@abstractmethod
def serialize(data: list[dict]) -> str
```

Serialize data to string format.

#### deserialize

```python
@abstractmethod
def deserialize(file_like: TextIO) -> list[dict]
```

Deserialize data from file-like object.

## JsonSerializer Objects

```python
class JsonSerializer(FileFormatSerializer)
```

JSON format serializer implementation.

#### serialize

```python
def serialize(data: list[dict]) -> str
```

Serialize data to JSON string.

#### deserialize

```python
def deserialize(file_like: TextIO) -> list[dict]
```

Deserialize data from JSON file.

## YamlSerializer Objects

```python
class YamlSerializer(FileFormatSerializer)
```

YAML format serializer implementation.

#### serialize

```python
def serialize(data: list[dict]) -> str
```

Serialize data to YAML string.

#### deserialize

```python
def deserialize(file_like: TextIO) -> list[dict]
```

Deserialize data from YAML file.

## SerializerFactory Objects

```python
class SerializerFactory()
```

Factory for creating appropriate serializers based on file extension.

#### get\_supported\_extensions

```python
@classmethod
def get_supported_extensions(cls) -> tuple[str, ...]
```

Get tuple of supported file extensions.

**Returns**:

  Tuple of supported file extensions (e.g., (&quot;.json&quot;, &quot;.yaml&quot;, &quot;.yml&quot;)).

#### create\_serializer

```python
@classmethod
def create_serializer(cls,
                      extension: str | None = None) -> FileFormatSerializer
```

Create a serializer based on file extension.

**Arguments**:

- `extension` - File extension (e.g., &quot;.json&quot;, &quot;.yaml&quot;, &quot;.yml&quot;).
  If None or empty, defaults to YAML.
  

**Returns**:

  Appropriate serializer instance. Defaults to YAML if extension not recognized or not provided.

#### register\_format

```python
@classmethod
def register_format(cls, extension: str,
                    serializer_class: type[FileFormatSerializer]) -> None
```

Register a new file format serializer.

**Arguments**:

- `extension` - File extension
- `serializer_class` - Serializer class implementing FileFormatSerializer interface.

## ChecksSerializer Objects

```python
class ChecksSerializer()
```

Handles serialization of DQRule objects to dictionaries and file formats.

#### serialize

```python
@staticmethod
def serialize(checks: list[DQRule]) -> list[dict]
```

Converts a list of quality checks defined as *DQRule* objects to a list of quality checks
defined as Python dictionaries.

**Arguments**:

- `checks` - List of DQRule instances to convert.
  

**Returns**:

  List of dictionaries representing the DQRule instances.
  

**Raises**:

- `InvalidCheckError` - If any item in the list is not a DQRule instance.

#### serialize\_to\_bytes

```python
@staticmethod
def serialize_to_bytes(checks: list[dict], extension: str) -> bytes
```

Serializes a list of checks to bytes in json or yaml (default) format.

**Arguments**:

- `checks` - List of checks to serialize.
- `extension` - File extension (e.g., &quot;.json&quot;, &quot;.yaml&quot;, &quot;.yml&quot;).

**Returns**:

  Serialized checks as bytes.

## ChecksDeserializer Objects

```python
class ChecksDeserializer()
```

Handles deserialization of dictionaries to DQRule objects and from file formats.

#### \_\_init\_\_

```python
def __init__(custom_checks: dict[str, Callable] | None = None)
```

Initialize the deserializer.

**Arguments**:

- `custom_checks` - Dictionary with custom check functions.

#### deserialize

```python
def deserialize(checks: list[dict]) -> list[DQRule]
```

Converts a list of quality checks defined as Python dictionaries to a list of `DQRule` objects.

**Arguments**:

- `checks` - list of dictionaries describing checks. Each check is a dictionary
  consisting of following fields:
  - *check* - Column expression to evaluate. This expression should return string value if it&#x27;s evaluated to true
  or *null* if it&#x27;s evaluated to *false*
  - *name* - name that will be given to a resulting column. Autogenerated if not provided
  - *criticality* (optional) - possible values are *error* (data going only into &quot;bad&quot; dataframe),
  and *warn* (data is going into both dataframes)
  - *filter* (optional) - Expression for filtering data quality checks
  - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
  

**Returns**:

  list of data quality check rules
  

**Raises**:

- `InvalidCheckError` - If any dictionary is invalid or unsupported.

#### deserialize\_from\_file

```python
@staticmethod
def deserialize_from_file(extension: str, file_like: TextIO) -> list[dict]
```

Deserialize checks from a file-like object based on file extension.
Automatically denormalizes special markers back to objects.

**Arguments**:

- `extension` - File extension (e.g., &quot;.json&quot;, &quot;.yaml&quot;, &quot;.yml&quot;).
- `file_like` - File-like object to read from.
  

**Returns**:

  List of check dictionaries with special markers converted to objects.

#### serialize\_checks

```python
def serialize_checks(checks: list[DQRule]) -> list[dict]
```

Converts a list of quality checks defined as *DQRule* objects to a list of quality checks
defined as Python dictionaries.

This is a convenience user-friendly function that wraps ChecksSerializer.serialize.

**Arguments**:

- `checks` - List of DQRule instances to convert.
  

**Returns**:

  List of dictionaries representing the DQRule instances.
  

**Raises**:

- `InvalidCheckError` - If any item in the list is not a DQRule instance.

#### deserialize\_checks

```python
def deserialize_checks(
        checks: list[dict],
        custom_checks: dict[str, Callable] | None = None) -> list[DQRule]
```

Converts a list of quality checks defined as Python dictionaries to a list of DQRule objects.

This is a convenience user-friendly function that wraps ChecksDeserializer.deserialize.

**Arguments**:

- `checks` - list of dictionaries describing checks. Each check is a dictionary
  consisting of following fields:
  - *check* - Column expression to evaluate. This expression should return string value if it&#x27;s evaluated to true
  or *null* if it&#x27;s evaluated to *false*
  - *name* - name that will be given to a resulting column. Autogenerated if not provided
  - *criticality* (optional) - possible values are *error* (data going only into &quot;bad&quot; dataframe),
  and *warn* (data is going into both dataframes)
  - *filter* (optional) - Expression for filtering data quality checks
  - *user_metadata* (optional) - User-defined key-value pairs added to metadata generated by the check.
- `custom_checks` - Dictionary with custom check functions.
  

**Returns**:

  list of data quality check rules
  

**Raises**:

- `InvalidCheckError` - If any dictionary is invalid or unsupported.

