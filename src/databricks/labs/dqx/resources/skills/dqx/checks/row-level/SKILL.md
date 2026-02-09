---
name: dqx-row-level-checks
description: Row-level DQX quality check functions. Each check validates individual row values (nulls, ranges, patterns, dates, etc.) and adds results to _errors or _warnings columns.
---

# Row-Level Check Functions

Row-level checks validate each row independently. Use `column` in `arguments` to specify which column to check.

## Parameter Reference

**IMPORTANT:** Parameters listed as **required** MUST be provided -- omitting them causes a runtime error.
Use `is_not_less_than` (not `is_in_range`) when you only have a minimum.
Use `is_not_greater_than` (not `is_in_range`) when you only have a maximum.

### Null and Empty

| Function | Required | Optional |
|----------|----------|----------|
| `is_not_null` | `column` | -- |
| `is_not_empty` | `column` | `trim_strings` (default: false) |
| `is_not_null_and_not_empty` | `column` | `trim_strings` (default: false) |
| `is_not_null_and_not_empty_array` | `column` | -- |
| `is_null` | `column` | -- |
| `is_empty` | `column` | `trim_strings` (default: false) |
| `is_null_or_empty` | `column` | `trim_strings` (default: false) |

### List Membership

| Function | Required | Optional |
|----------|----------|----------|
| `is_in_list` | `column`, `allowed` (list) | `case_sensitive` (default: true) |
| `is_not_null_and_is_in_list` | `column`, `allowed` (list) | `case_sensitive` (default: true) |
| `is_not_in_list` | `column`, `forbidden` (list) | `case_sensitive` (default: true) |

### Comparison

| Function | Required | Optional |
|----------|----------|----------|
| `is_not_less_than` | `column`, `limit` | -- |
| `is_not_greater_than` | `column`, `limit` | -- |
| `is_equal_to` | `column`, `value` | `abs_tolerance`, `rel_tolerance` |
| `is_not_equal_to` | `column`, `value` | `abs_tolerance`, `rel_tolerance` |

`limit` / `value` can be a number, date, timestamp, column name, or SQL expression.

### Range

| Function | Required | Optional |
|----------|----------|----------|
| `is_in_range` | `column`, **`min_limit`**, **`max_limit`** | -- |
| `is_not_in_range` | `column`, **`min_limit`**, **`max_limit`** | -- |

**Both `min_limit` AND `max_limit` are required.** If you only need one bound, use `is_not_less_than` (min only) or `is_not_greater_than` (max only).

### Pattern

| Function | Required | Optional |
|----------|----------|----------|
| `regex_match` | `column`, `regex` | `negate` (default: false) |

### Date and Time

| Function | Required | Optional |
|----------|----------|----------|
| `is_valid_date` | `column` | `date_format` |
| `is_valid_timestamp` | `column` | `timestamp_format` |
| `is_not_in_future` | `column` | `offset` (seconds, default: 0) |
| `is_not_in_near_future` | `column` | `offset` (seconds, default: 0) |
| `is_older_than_n_days` | `column`, `days` | `negate` (default: false) |
| `is_older_than_col2_for_n_days` | `column1`, `column2` | `days` (default: 0), `negate` (default: false) |
| `is_data_fresh` | `column`, `max_age_minutes` | `base_timestamp` |

### Network

| Function | Required | Optional |
|----------|----------|----------|
| `is_valid_ipv4_address` | `column` | -- |
| `is_ipv4_address_in_cidr` | `column`, `cidr_block` | -- |
| `is_valid_ipv6_address` | `column` | -- |
| `is_ipv6_address_in_cidr` | `column`, `cidr_block` | -- |

### JSON

| Function | Required | Optional |
|----------|----------|----------|
| `is_valid_json` | `column` | -- |
| `has_json_keys` | `column`, `keys` (list) | `require_all` (default: true) |
| `has_valid_json_schema` | `column`, `schema` | -- |

### SQL Expression

| Function | Required | Optional |
|----------|----------|----------|
| `sql_expression` | `expression` | `msg`, `name`, `negate` (default: false), `columns` |

---

## Examples

### Null and Empty Checks

```yaml
# Single column
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: customer_id

# Multiple columns with for_each_column
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
      - first_name
      - last_name
      - email

# Trim whitespace before checking emptiness
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: address
      trim_strings: true

# Array column: not null and has at least one element
- criticality: error
  check:
    function: is_not_null_and_not_empty_array
    arguments:
      column: tags
```

### List Membership

```yaml
# Value must be in allowed list (nulls pass)
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: status
      allowed: [active, inactive, pending]

# Not null AND must be in list
- criticality: error
  check:
    function: is_not_null_and_is_in_list
    arguments:
      column: country_code
      allowed: [US, GB, DE, FR, JP]
```

### Comparison

```yaml
# Numeric: value >= 0  (use is_not_less_than, NOT is_in_range)
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: quantity
      limit: 0                       # REQUIRED

# Column comparison: end_date >= start_date
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: end_date
      limit: start_date              # can be a column name

# String literal (must be single-quoted inside double quotes)
- criticality: warn
  check:
    function: is_not_equal_to
    arguments:
      column: status
      value: "'unknown'"             # REQUIRED

# SQL expression as limit
- criticality: warn
  check:
    function: is_not_greater_than
    arguments:
      column: discount
      limit: price * 0.5            # REQUIRED
```

### Range

**Both `min_limit` AND `max_limit` are required for `is_in_range` / `is_not_in_range`.**
If you only need one bound, use `is_not_less_than` or `is_not_greater_than` instead.

```yaml
# Numeric range -- BOTH limits required
- criticality: warn
  check:
    function: is_in_range
    arguments:
      column: age
      min_limit: 0                   # REQUIRED
      max_limit: 150                 # REQUIRED

# Date range -- BOTH limits required
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: order_date
      min_limit: 2020-01-01          # REQUIRED
      max_limit: 2030-12-31          # REQUIRED

# Column-based range -- BOTH limits required
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: sale_price
      min_limit: cost_price          # REQUIRED
      max_limit: cost_price * 3      # REQUIRED
```

### Pattern and Regex

```yaml
# Match email pattern
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: email
      regex: '^[^@]+@[^@]+\.[^@]+$'  # REQUIRED

# Negate: fail if pattern DOES match (e.g. detect test data)
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: name
      regex: '^TEST_'                 # REQUIRED
      negate: true
```

### Date and Time Validation

```yaml
# Validate date format
- criticality: error
  check:
    function: is_valid_date
    arguments:
      column: birth_date

# Custom date format
- criticality: error
  check:
    function: is_valid_date
    arguments:
      column: date_str
      date_format: yyyy-MM-dd

# Not in the future (with 1-day tolerance in seconds)
- criticality: error
  check:
    function: is_not_in_future
    arguments:
      column: created_at
      offset: 86400

# Data freshness: column must be within N minutes of now
- criticality: warn
  check:
    function: is_data_fresh
    arguments:
      column: last_updated
      max_age_minutes: 60            # REQUIRED

# Compare two date columns: col1 must be at least N days older than col2
- criticality: warn
  check:
    function: is_older_than_col2_for_n_days
    arguments:
      column1: start_date            # REQUIRED
      column2: end_date              # REQUIRED
      days: 1
```

### Network Validation

```yaml
- criticality: warn
  check:
    function: is_valid_ipv4_address
    arguments:
      column: ip_address

- criticality: error
  check:
    function: is_ipv4_address_in_cidr
    arguments:
      column: ip_address
      cidr_block: '10.0.0.0/8'      # REQUIRED
```

### SQL Expression (Catch-All)

Use for any custom row-level logic. See `custom/SKILL.md` for advanced patterns (window functions, etc.).

```yaml
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "end_date >= start_date"   # REQUIRED
      msg: "end_date is before start_date"
```

### Complex Column Types

Use dot notation for structs, `try_element_at()` for maps/arrays:

```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: address.city             # struct field

- criticality: error
  check:
    function: is_not_null
    arguments:
      column: "try_element_at(metadata, 'source')"  # map element
```

## Runnable Examples

| Example | File |
|---------|------|
| Null and empty checks | `examples/01_row_null_empty.py` |
| List membership | `examples/02_row_list.py` |
| Comparison checks | `examples/03_row_comparison.py` |
| Range checks | `examples/04_row_range.py` |
| Regex / pattern matching | `examples/05_row_regex.py` |
| Date and time validation | `examples/06_row_datetime.py` |
| Network validation (IPv4/IPv6) | `examples/07_row_network.py` |
| SQL expression (catch-all) | `examples/08_row_sql_expression.py` |
| Complex column types | `examples/09_row_complex.py` |

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#row-level-checks-reference
