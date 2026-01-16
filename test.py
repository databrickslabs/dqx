# import hashlib
# import json
# from datetime import datetime

# def rule_fingerprint(dicionario) -> str:
#     return hashlib.md5(json.dumps(dicionario, sort_keys=True).encode("utf-8")).hexdigest()


# dicionario = {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"]}}

# print(rule_fingerprint(dicionario))

# def generate_rule_set_fingerprint_from_dict(checks: list[dict]) -> tuple[list[dict], str]:
#     rule_set = []
#     for check in checks:
#         rule_fingerprint = hashlib.md5(json.dumps(check, sort_keys=True).encode("utf-8")).hexdigest()
#         check['rule_fingerprint'] = rule_fingerprint
#         rule_set.append(rule_fingerprint)
#     rule_set_fingerprint = str(hash(tuple(sorted(rule_set))))
#     created_at = datetime.now()
#     for check in checks:
#         check['rule_set_fingerprint'] = rule_set_fingerprint
#         check['created_at'] = created_at
#     return checks, rule_set_fingerprint

# checks=[{
#         "name": "column_not_less_than",
#         "criticality": "warn",
#         "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1}},
#         "filter": "Col_3 >1",
#         "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
#     },
#     {
#         "name": "column_in_list",
#         "criticality": "warn",
#         "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
#     },] 

# new_checks, rule_set_fingerprint = generate_rule_set_fingerprint_from_dict(checks)
# print(new_checks, rule_set_fingerprint)
from tests.conftest import compare_checks,_sort_key
INPUT_CHECKS = [
    {
        "name": "col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    }
   
]

EXPECTED_CHECKS = [
    {
        "name": "col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        "rule_fingerprint": "3e2f5f3f4e2e1e8e4f5c6d7a8b9c0d1e",
        "rule_set_fingerprint": "1234567890abcdef",
        "created_at": "2024-06-01T12:00:00",
    },
    {
        "name": "col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        "rule_fingerprint": "4f5e6d7c8b9a0b1c2d3e4f5a6b7c8d9e",
        "rule_set_fingerprint": "1234567890abcdef",
        "created_at": "2024-06-01T12:00:00",
    }
   
]
