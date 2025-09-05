# Databricks notebook source

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
from databricks.labs.dqx.ipaddress.ipaddress_funcs import is_ipv6_address_in_cidr, is_valid_ipv6_address

from chispa import assert_df_equality

# COMMAND ----------
# DBTITLE 1,test_does_not_contain_pii_basic

def test_does_not_contain_ipv6_basic():
    schema_ipv6 = "col1: string"

    test_df = spark.createDataFrame(
        [
            ["192.170.01.1"],
            ["0.0.0.0"],
            ["abc.def.ghi.jkl"],
            [None],
            ["255255155255"],
            ["192.168.1.1.1"],
            [""],
            [" "],
            ["192.168.1.0/"],
            # Valid IPv6 addresses - basic formats
            ["::1"],  # Loopback
            ["12345"],  # Invalid - hextet too long
            ["::"],  # Unspecified
            ["2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64"],  # malformed IPv4-embedded suffix
            ["001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Leading zeros (valid)
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # All F's
            ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Full form
            ["fe80:0000:0000:0000:0202:b3ff:fe1e:8329"],  # Link-local full
            ["2001:0db8:0000:0000:0000:ff00:0042:8329"],  # Valid full
            ["2606:4700:4700:0000:0000:0000:0000:1111"],  # Cloudflare DNS
            ["2a03:2880:f12f:83:FACE:b00c:0000:25DE"],  # Mixed case
            ["2001:4860:4860:0000:0000:0000:0000:8888"],  # Google DNS
            ["2002:c0a8:0101:0000:0000:0000:c0a8:0101"],  # 6to4
            # Invalid formats - various malformed cases
            ["zFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:ZZZFFF"],  # Invalid hex chars
            ["2001:0018:0194:0c02:0001:02ff:fe03:0405"],  # Valid
            ["2000:0018:0194:0c02:0001:02ff:fe03:0405"],  # Valid
            ["FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"],  # All uppercase F's
            ["f:f:a:d:g:1:2:3"],  # Invalid hex char 'g'
            ["f:: "],  # Trailing space
            [" :: "],  # Leading/trailing spaces
            [" ::"],  # Leading space
            ["FF FF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"],  # Space in address
            ["2000:00 8:0194:0c02:0001:02ff:fe03:0405"],  # Space in hextet
            [":::"],  # Too many colons
            ["0:0::d::"],  # Multiple compressions
            ["aaaa:"],  # Incomplete address
            ["aaaa"],  # Single hextet
            [":abcd"],  # Leading colon without compression
            ["::abcd"],  # Valid compression
            ["::abcg"],  # Invalid hex in compression
            ["::1bcg"],  # Invalid hex in compression
            ["::1bcf"],  # Valid compression
            ["1b::cf"],  # Valid compression
            ["1b::cf_"],  # Invalid character
            ["2000:0:0194:0c02:00+1:02ff:fe03:_10"],  # Invalid characters
            [".::"],  # Invalid format
            ["0::"],  # Valid compression
            ["0::0"],  # Valid compression
            ["0::z"],  # Invalid hex char
            ["::0:0194:0c02:1:02ff:fe03:10"],  # Valid compression
            ["1:2::3:4"],  # Valid compression
            ["1234::5678::abcd"],  # Multiple compressions (invalid)
            [":1234:5678:9abc:def0:1234:5678:9abc"],  # Leading colon
            ["1234:5678:9abc:def0:1234:5678:9abc:"],  # Trailing colon
            ["::1"],  # Loopback (duplicate for completeness)
            ["::"],  # Unspecified (duplicate for completeness)
            ["1::"],  # Valid compression
            ["::1:2:3:4:5:6"],  # Valid compression
            ["2001:db8::1"],  # Valid compression
            ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Full form (duplicate)
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # All F's (duplicate)
            ["2001:DB8::1"],  # Mixed case
            ["fe80::1%eth0"],  # Zone ID (invalid in pure IPv6)
            ["fe80::%12"],  # Zone ID (invalid in pure IPv6)
            ["1:2:3:4:5:6:7:8"],  # Valid full form
            ["2001:db8::192.168.0.1"],  # IPv4-embedded
            ["::ffff:192.0.2.128"],  # IPv4-mapped
            ["2001:db8:abcd:0012::"],  # Valid compression
            # ADDITIONAL EDGE CASES - Link-local addresses
            ["fe80::1"],  # Valid link-local
            ["fe80::0000:0000:0000:1"],  # Link-local full form
            ["fe81::1"],  # Valid link-local variant
            ["fec0::1"],  # Site-local (deprecated but valid format)
            # Unique Local Addresses (ULA)
            ["fc00::1"],  # Valid ULA
            ["fd00::1"],  # Valid ULA (locally assigned)
            ["fb00::1"],  # Valid ULA
            ["fe00::1"],  # Outside ULA range
            # Multicast addresses
            ["ff00::1"],  # Valid multicast
            ["ff02::1"],  # All-nodes multicast
            ["ff05::1:3"],  # Valid multicast with scope
            ["fe02::1"],  # Just outside multicast range
            # Documentation addresses (RFC 3849)
            ["2001:db8::"],  # Valid documentation range
            ["2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # Max in doc range
            ["2001:db7::1"],  # Just outside documentation range
            ["2001:db9::1"],  # Just outside documentation range
            # Extreme compression edge cases
            ["1::"],  # Minimal compression at end (duplicate)
            ["::1:0:0:0:0:0:0"],  # Compression at start with trailing
            ["1:0:0:0::1"],  # Compression in middle
            ["::0:0:0:0:0:0:1"],  # Compression with zeros
            # Zero padding edge cases
            ["2001:0db8:0000:0000:0000:0000:0000:0001"],  # Excessive zeros
            ["2001:db8:0:0:0:0:0:1"],  # Minimal zeros
            ["0001:0002:0003:0004:0005:0006:0007:0008"],  # All with leading zeros
            # IPv4-mapped additional cases
            ["::ffff:0:0"],  # IPv4-mapped all zeros
            ["::ffff:255.255.255.255"],  # IPv4-mapped max values
            ["::ffff:127.0.0.1"],  # IPv4-mapped loopback
            ["::ffff:192.168.1.1"],  # IPv4-mapped private
            ["2001:db8::10.0.0.1"],  # IPv4-embedded in documentation range
            # Invalid edge cases
            ["12345::1"],  # Hextet too long (5 digits)
            ["1:2:3:4:5:6:7:8:9"],  # Too many hextets
            ["1::2::3"],  # Multiple compressions
            ["::1::"],  # Compression with extra colon
            ["1:2:3:4:5:6:7:"],  # Trailing colon without compression
            ["1:2:3:4:5:6:7"],  # Too few hextets without compression
            [":1:2:3:4:5:6:7:8"],  # Leading colon without compression
            ["g::1"],  # Invalid hexadecimal character
            ["1:2:3:4:5:6:7:8:"],  # Trailing colon with full address
            ["1:2:3:4:5:6:7::8"],  # Invalid compression position
            ["::g"],  # Invalid hex in compression
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff"],  # Hextet too long
            ["1:2:3:4:5:6:7:8:9:a"],  # Way too many hextets
            # Boundary cases
            ["0:0:0:0:0:0:0:0"],  # All zeros (should be ::)
            ["0000:0000:0000:0000:0000:0000:0000:0000"],  # All zeros with padding
            # Case sensitivity tests
            ["ABCD:EFAB:CDEF:1234:5678:9ABC:DEF0:1234"],  # All uppercase
            ["abcd:efab:cdef:1234:5678:9abc:def0:1234"],  # All lowercase
            ["AbCd:EfAb:CdEf:1234:5678:9aBc:DeF0:1234"],  # Mixed case
        ],
        schema_ipv6
    )

    actual = test_df.select(
        is_valid_ipv6_address("col1")
    )

    checked_schema = "a_does_not_match_pattern_ipv6_address: string"
    expected = spark.createDataFrame(
        [
            # Invalid formats - IPv4 and malformed addresses
            ["Value '192.170.01.1' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0.0.0.0' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'abc.def.ghi.jkl' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # NULL values should pass (return None)
            ["Value '255255155255' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.1.1' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.0/' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            # Valid IPv6 addresses - basic formats
            [None],  # ::1 - Valid loopback
            ["Value '12345' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # :: - Valid unspecified
            [
                "Value '2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"
            ],  # malformed IPv4-embedded suffix
            [None],  # Leading zeros valid
            [None],  # All F's valid
            [None],  # Full form valid
            [None],  # Link-local full valid
            [None],  # Valid full
            [None],  # Cloudflare DNS valid
            [None],  # Mixed case valid
            [None],  # Google DNS valid
            [None],  # 6to4 valid
            # Invalid formats - various malformed cases
            ["Value 'zFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:ZZZFFF' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid
            [None],  # Valid
            [None],  # All uppercase F's valid
            ["Value 'f:f:a:d:g:1:2:3' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'f:: ' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' :: ' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ::' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'FF FF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:00 8:0194:0c02:0001:02ff:fe03:0405' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':::' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0:0::d::' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa:' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':abcd' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::abcd valid
            ["Value '::abcg' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1bcg' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1bcf valid
            [None],  # 1b::cf valid
            ["Value '1b::cf_' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:0:0194:0c02:00+1:02ff:fe03:_10' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '.::' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # 0:: valid
            [None],  # 0::0 valid
            ["Value '0::z' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid compression
            [None],  # Valid compression
            ["Value '1234::5678::abcd' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1234:5678:9abc:def0:1234:5678:9abc' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1234:5678:9abc:def0:1234:5678:9abc:' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1 valid (duplicate)
            [None],  # :: valid (duplicate)
            [None],  # 1:: valid
            [None],  # Valid compression
            [None],  # Valid compression
            [None],  # Full form valid (duplicate)
            [None],  # All F's valid (duplicate)
            [None],  # Mixed case valid
            ["Value 'fe80::1%eth0' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'fe80::%12' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid full form
            [None],  # IPv4-embedded valid
            [None],  # IPv4-mapped valid
            [None],  # Valid compression
            # ADDITIONAL EDGE CASES - Link-local addresses
            [None],  # fe80::1 valid
            [None],  # Link-local full form valid
            [None],  # fe81::1 valid
            [None],  # Site-local valid format
            # Unique Local Addresses (ULA)
            [None],  # fc00::1 valid
            [None],  # fd00::1 valid
            [None],  # Valid ULA
            [None],  # fe00::1 valid format (though not in ULA range)
            # Multicast addresses
            [None],  # ff00::1 valid multicast
            [None],  # ff02::1 valid multicast
            [None],  # ff05::1:3 valid multicast
            [None],  # fe02::1 valid format
            # Documentation addresses
            [None],  # 2001:db8:: valid
            [None],  # Max in doc range valid
            [None],  # 2001:db7::1 valid
            [None],  # 2001:db9::1 valid
            # Extreme compression edge cases
            [None],  # 1:: valid (duplicate)
            [None],  # Compression with trailing valid
            [None],  # Compression in middle valid
            [None],  # Compression with zeros valid
            # Zero padding edge cases
            [None],  # Excessive zeros valid
            [None],  # Minimal zeros valid
            [None],  # All with leading zeros valid
            # IPv4-mapped additional cases
            [None],  # IPv4-mapped zeros valid
            [None],  # IPv4-mapped max valid
            [None],  # IPv4-mapped loopback valid
            [None],  # IPv4-mapped private valid
            [None],  # IPv4-embedded in doc range valid
            # Invalid edge cases
            ["Value '12345::1' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1::2::3' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1::' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1:2:3:4:5:6:7:8' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'g::1' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7::8' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::g' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9:a' in Column 'col1' does not match pattern 'IPV6_ADDRESS'"],
            # Boundary cases
            [None],  # All zeros valid (0:0:0:0:0:0:0:0)
            [None],  # All zeros with padding valid
            # Case sensitivity tests
            [None],  # All uppercase valid
            [None],  # All lowercase valid
            [None],  # Mixed case valid
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


# COMMAND ----------
# DBTITLE 1,test_does_not_contain_pii_with_builtin_nlp_engine_config

def test_does_not_contain_pii_with_builtin_nlp_engine_config():
    schema_pii = "col1: string"
    test_df = spark.createDataFrame(
        [
            ["Dr. Jane Smith works at Memorial Hospital"],
            ["Patient ID: 12345, DOB: 1990-01-01"],
            ["Regular text without PII"],
            [None],
        ],
        schema_pii,
    )

    actual = test_df.select(does_not_contain_pii("col1", entities=["PERSON", "DATE_TIME"], nlp_engine_config=NLPEngineConfig.SPACY_MEDIUM))

    checked_schema = "col1_contains_pii: string"
    expected = spark.createDataFrame(
        [
            ["""Column 'col1' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "Jane Smith"}]"""],
            ["""Column 'col1' contains PII: [{"entity_type": "DATE_TIME", "score": 1.0, "text": "1990-01-01"}]"""],
            [None],
            [None],
        ],
        checked_schema,
    )
    transforms = [
        lambda df: df.select(
            F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
        )
    ]
    assert_df_equality(actual, expected, transforms=transforms)

# # COMMAND ----------
# # DBTITLE 1,test_does_not_contain_pii_with_custom_nlp_config_dict

# def test_does_not_contain_pii_with_custom_nlp_config_dict():
#     schema_pii = "col1: string"
#     test_df = spark.createDataFrame(
#         [
#             ["Dr. Jane Smith treated patient John Doe at City Hospital"],
#             ["Lorem ipsum dolor sit amet"],
#             [None],
#         ],
#         schema_pii,
#     )
#     custom_nlp_engine_config = {
#         "nlp_engine_name": "spacy",
#         "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
#     }
#     actual = test_df.select(does_not_contain_pii("col1", entities=["PERSON"], nlp_engine_config=custom_nlp_engine_config))

#     checked_schema = "col1_contains_pii: string"
#     expected = spark.createDataFrame(
#         [
#             [
#                 """Column 'col1' contains PII: [{"entity_type": "PERSON", "score": 1.0, "text": "Jane Smith"},{"entity_type": "PERSON", "score": 1.0, "text": "John Doe"}]"""
#             ],
#             [None],
#             [None],
#         ],
#         checked_schema,
#     )
#     transforms = [
#         lambda df: df.select(
#             F.ilike("col1_contains_pii", F.lit("Column 'col1' contains PII: %")).alias("col1_contains_pii"),
#         )
#     ]
#     assert_df_equality(actual, expected, transforms=transforms)

# test_does_not_contain_pii_with_custom_nlp_config_dict()
