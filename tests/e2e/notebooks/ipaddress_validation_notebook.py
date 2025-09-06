# Databricks notebook source

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

# COMMAND ----------

%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' pytest chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.ipaddress.ipaddress_funcs import is_ipv6_address_in_cidr, is_valid_ipv6_address
from chispa import assert_df_equality

# COMMAND ----------
# DBTITLE 1,test_is_valid_ipv6_address

def test_is_valid_ipv6_address():
    schema_ipv6 = "a: string"

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
        is_valid_ipv6_address("a")
    )

    checked_schema = "a_does_not_match_pattern_ipv6_address: string"
    expected = spark.createDataFrame(
        [
            # Invalid formats - IPv4 and malformed addresses
            ["Value '192.170.01.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0.0.0.0' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'abc.def.ghi.jkl' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # NULL values should pass (return None)
            ["Value '255255155255' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.1.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.0/' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            # Valid IPv6 addresses - basic formats
            [None],  # ::1 - Valid loopback
            ["Value '12345' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # :: - Valid unspecified
            [
                "Value '2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64' in Column 'a' does not match pattern 'IPV6_ADDRESS'"
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
            ["Value 'zFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:ZZZFFF' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid
            [None],  # Valid
            [None],  # All uppercase F's valid
            ["Value 'f:f:a:d:g:1:2:3' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'f:: ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' :: ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'FF FF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:00 8:0194:0c02:0001:02ff:fe03:0405' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0:0::d::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':abcd' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::abcd valid
            ["Value '::abcg' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1bcg' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1bcf valid
            [None],  # 1b::cf valid
            ["Value '1b::cf_' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:0:0194:0c02:00+1:02ff:fe03:_10' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '.::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # 0:: valid
            [None],  # 0::0 valid
            ["Value '0::z' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid compression
            [None],  # Valid compression
            ["Value '1234::5678::abcd' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1234:5678:9abc:def0:1234:5678:9abc' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1234:5678:9abc:def0:1234:5678:9abc:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1 valid (duplicate)
            [None],  # :: valid (duplicate)
            [None],  # 1:: valid
            [None],  # Valid compression
            [None],  # Valid compression
            [None],  # Full form valid (duplicate)
            [None],  # All F's valid (duplicate)
            [None],  # Mixed case valid
            [None], # IPv6 link-local address
            [None], # IPv6 link-local address
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
            ["Value '12345::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1::2::3' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1:2:3:4:5:6:7:8' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'g::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7::8' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::g' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9:a' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
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

test_is_valid_ipv6_address()

# COMMAND ----------
# DBTITLE 1,test_is_ipv6_address_in_cidr_basic

def test_is_ipv6_address_in_cidr_basic():
    schema_ipv6 = "a: string, b: string"

    test_df = spark.createDataFrame(
        [
            ["2001:db8:abcd:0012", None],
            ["::1", "2002:c0a8:0101::1"],
            ["192.1", "1.01"],
            ["2001:db8:abcd:0012:0000:0000:0000:0001", "2001:db8:1234:5600::1"],
            ["2001:db8:abcd:0012::1", "2001:db8:1234:56ff::1"],
            ["2001:db8:ffff:0012::1", "2001:db9::1"],
            [None, None],
            ["", ""],
            ["::ffff:192.168.1.1", "2001:db8::192.168.1.1"],
            ["2001:db8:abcd:12::", "2001:db8:1234:56aa::1"],
            ["2001:DB8:ABCD:0012::FFFF", "2001:db8:1234:5700::1"],
            ["2001:db8:abcd:0013::1", "::ffff:192.0.2.128"],
            ["[2001:db8:abcd:0012::1]", "fe80::1%eth0"],
            ["2001:db8:abcd:0012:0:0:0:0", "2001:db8:1234:5600::192.0.2.128"],
            ["::", "2001:db8::192.168.1.1"],
            ["2001:db8:abcd:0012:ffff:ffff:ffff:ffff", "2001:db8:1234:56ff::ffff"],
            ["2001:db8:abcd:0012::dead", "2001:db8:1234:56ab::"],
            ["2001:DB8:ABCD:0012:0:0:BeEf:1", "2001:db8:1234:56ab::192.168.10.20"],
            ["2001:db8:abcd:0011::", "2001:db8:1234:55ff::1"],
            ["2001:db8:abgd::1", "2001:db8:1234:5800::"],
            ["2001:db8:abcd:0012::1", "2001:db8:1234:5600::"],
            ["2001:db8:abcd:12:0::1", "2001:db8:1234:56ff:ffff:ffff::"],
            ["::1", "::ffff:203.0.113.10"],
            ["::", "2001:db8:1234:5700::192.0.2.128"],
            ["2001:db8:abcd:0012:FFFF:ffff:FFFF:ffff", "2001:DB8:1234:56AA::"],
            ["2002::1", "2001:db8:1234:56:0:0:0:0:1"],
            ["2001:db8:abcd:0012::", "2001:db8:1234:56aa::10.0.0.1"],
            # ADDITIONAL EDGE CASES FOR CIDR TESTING
            # Boundary testing - first/last addresses in ranges
            ["2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # First and last in /32
            ["2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "2001:db9::"],  # Just before/after range
            # Different prefix lengths
            ["2001:db8::1", "2001:db8::2"],  # Single host testing
            ["::1", "::2"],  # Loopback testing
            ["2001:db8::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Max address testing
            # Zero prefix length edge case (/0 matches everything)
            ["::", "2001:db8::"],  # All addresses should match /0
            # Link-local ranges
            ["fe80::1", "fec0::1"],  # Link-local testing
            # Multicast ranges
            ["ff02::1", "fe02::1"],  # Multicast testing
            # ULA ranges
            ["fc00::1", "fd00::1"],  # ULA testing
            ["fe00::1", "fb00::1"],  # Outside ULA testing
            # IPv4-embedded in CIDR
            ["::ffff:192.168.1.1", "::ffff:192.168.2.1"],  # IPv4-mapped testing
            # Case sensitivity in addresses
            ["2001:DB8::1", "2001:db8::1"],  # Mixed case testing
            # Compression variations
            ["2001:0:0:0:0:0:0:1", "2001::2"],  # Different compression styles
            # Invalid addresses for error testing
            ["invalid::address", "2001:db8::invalid"],  # Invalid formats
            ["12345::1", "g::1"],  # Invalid hex chars
        ],
        schema_ipv6,
    )

    # Test with multiple different CIDR blocks to cover various edge cases
    actual = test_df.select(
        is_ipv6_address_in_cidr("a", "2001:db8:abcd:0012::/64"),
        is_ipv6_address_in_cidr("b", "2001:db8:1234:5600::192.0.2.128/56"),
    )

    checked_schema = "a_is_not_ipv6_in_cidr: string, b_is_not_ipv6_in_cidr: string"
    expected = spark.createDataFrame(
        [
            ["Value '2001:db8:abcd:0012' in Column 'a' does not match pattern 'IPV6_ADDRESS'", None],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2002:c0a8:0101::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '192.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '1.01' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [None, None],
            [None, None],
            [
                "Value '2001:db8:ffff:0012::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db9::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                "Value '' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [
                "Value '::ffff:192.168.1.1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::192.168.1.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                None,
                "Value '2001:db8:1234:5700::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8:abcd:0013::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:192.0.2.128' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '[2001:db8:abcd:0012::1]' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value 'fe80::1%eth0' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [None, None],
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::192.168.1.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [None, None],
            [None, None],
            [
                "Value '2001:db8:abcd:0011::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:55ff::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8:abgd::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '2001:db8:1234:5800::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [None, None],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:203.0.113.10' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:5700::192.0.2.128' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                "Value '2002::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:56:0:0:0:0:1' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [None, None],
            # ADDITIONAL EDGE CASE RESULTS
            # Boundary testing - first/last addresses in ranges
            [
                "Value '2001:db8::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db9::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Different prefix lengths
            [
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Zero prefix length edge case
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Link-local ranges
            [
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fec0::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Multicast ranges
            [
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fe02::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # ULA ranges
            [
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fd00::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fb00::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # IPv4-embedded in CIDR
            [
                "Value '::ffff:192.168.1.1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:192.168.2.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Case sensitivity in addresses (should still work)
            [
                "Value '2001:DB8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Compression variations
            [
                "Value '2001:0:0:0:0:0:0:1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Invalid addresses for error testing
            [
                "Value 'invalid::address' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '2001:db8::invalid' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [
                "Value '12345::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value 'g:1' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)

test_is_ipv6_address_in_cidr_basic()

# COMMAND ----------
# DBTITLE 1,test_ipv6_address_cidr_edge_cases

def test_ipv6_address_cidr_edge_cases():
    """Test comprehensive IPv6 CIDR edge cases including different prefix lengths."""
    schema_ipv6 = "a: string"

    test_df = spark.createDataFrame(
        [
            # Boundary testing for different prefix lengths
            ["2001:db8::"],  # First in /32
            ["2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # Last in /32
            ["2001:db7:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before /32
            ["2001:db9::"],  # Just after /32
            # Single host testing (/128)
            ["2001:db8::1"],  # Exact match for /128
            ["2001:db8::2"],  # Different host
            # Loopback testing
            ["::1"],  # Exact loopback
            ["::2"],  # Different loopback
            # Zero prefix testing (/0 - should match everything)
            ["::"],  # Unspecified
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Maximum address
            ["2001:db8::1"],  # Any address
            # Link-local prefix testing
            ["fe80::1"],  # Valid link-local
            ["fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before link-local
            ["fec0::1"],  # Just after link-local range
            # Multicast prefix testing
            ["ff00::1"],  # First multicast
            ["ff02::1"],  # All-nodes multicast
            ["feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before multicast
            # ULA prefix testing
            ["fc00::1"],  # First ULA
            ["fd00::1"],  # Local ULA
            ["fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Last ULA
            ["fe00::1"],  # Just after ULA
        ],
        schema_ipv6,
    )

    actual = test_df.select(
        # Test various prefix lengths
        is_ipv6_address_in_cidr("a", "2001:db8::/32"),  # /32 - 96 bits of network
        is_ipv6_address_in_cidr("a", "2001:db8::1/128"),  # /128 - single host
        is_ipv6_address_in_cidr("a", "::/0"),  # /0 - match everything
        is_ipv6_address_in_cidr("a", "fe80::/10"),  # /10 - link-local
        is_ipv6_address_in_cidr("a", "ff00::/8"),  # /8 - multicast
        is_ipv6_address_in_cidr("a", "fc00::/7"),  # /7 - ULA range
    )

    checked_schema = (
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string"
    )

    expected = spark.createDataFrame(
        [
            # Test /32 range (2001:db8::/32)
            [
                None,
                "Value '2001:db8::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '2001:db9::' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Single host testing (/128)
            [
                None,
                None,
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Loopback testing
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '::2' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::2' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::2' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::2' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::2' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Zero prefix testing (/0 - should match everything)
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Link-local prefix testing (/10)
            [
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                None,
                "Value 'fe80::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fec0::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fec0::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                None,
                "Value 'fec0::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            # Multicast prefix testing (/8)
            [
                "Value 'ff00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ff00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ff00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ff00::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ff02::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ff02::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            # ULA prefix testing (/7)
            [
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fc00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fc00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fd00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fd00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fd00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fd00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)

test_ipv6_address_cidr_edge_cases()
