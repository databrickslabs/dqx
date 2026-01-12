import hashlib
import json


def rule_fingerprint(dicionario) -> str:
    return hashlib.md5(json.dumps(dicionario, sort_keys=True).encode("utf-8")).hexdigest()


dicionario = {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"]}}

print(rule_fingerprint(dicionario))
