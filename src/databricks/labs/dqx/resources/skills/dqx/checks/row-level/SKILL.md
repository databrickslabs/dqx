---
name: dqx-row-level-checks
description: Row-level DQX quality check functions. Each check validates individual row values (nulls, ranges, patterns, dates, etc.) and adds results to _errors or _warnings columns.
---

# Row-Level Check Functions

Row-level checks validate each row independently. Use `column` in `arguments` to specify which column to check.

## Null and Empty Checks

Functions: `is_not_null`, `is_not_empty`, `is_not_null_and_not_empty`, `is_not_null_and_not_empty_array`

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

## List Membership

Functions: `is_in_list`, `is_not_null_and_is_in_list`

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

## Comparison

Functions: `is_equal_to`, `is_not_equal_to`, `is_not_less_than`, `is_not_greater_than`

Values can be literals, column names, or SQL expressions.

```yaml
# Numeric: value >= 0
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: quantity
      limit: 0

# Column comparison: end_date >= start_date
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: end_date
      limit: start_date

# String literal (must be single-quoted inside double quotes)
- criticality: warn
  check:
    function: is_not_equal_to
    arguments:
      column: status
      value: "'unknown'"

# SQL expression as limit
- criticality: warn
  check:
    function: is_not_greater_than
    arguments:
      column: discount
      limit: price * 0.5
```

## Range

Functions: `is_in_range`, `is_not_in_range`

Both boundaries are inclusive. Supports numbers, dates, timestamps, column refs.

```yaml
# Numeric range
- criticality: warn
  check:
    function: is_in_range
    arguments:
      column: age
      min_limit: 0
      max_limit: 150

# Date range
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: order_date
      min_limit: 2020-01-01
      max_limit: 2030-12-31

# Column-based range
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: sale_price
      min_limit: cost_price
      max_limit: cost_price * 3
```

## Pattern and Regex

Function: `regex_match`

```yaml
# Match email pattern
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: email
      regex: '^[^@]+@[^@]+\.[^@]+$'

# Negate: fail if pattern DOES match (e.g. detect test data)
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: name
      regex: '^TEST_'
      negate: true
```

## Date and Time Validation

Functions: `is_valid_date`, `is_valid_timestamp`, `is_not_in_future`, `is_not_in_near_future`, `is_older_than_n_days`, `is_older_than_col2_for_n_days`, `is_data_fresh`

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
      max_age_minutes: 60

# Compare two date columns: col1 must be at least N days older than col2
- criticality: warn
  check:
    function: is_older_than_col2_for_n_days
    arguments:
      column1: start_date
      column2: end_date
      days: 1
```

## Network Validation

Functions: `is_valid_ipv4_address`, `is_ipv4_address_in_cidr`, `is_valid_ipv6_address`, `is_ipv6_address_in_cidr`

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
      cidr_block: '10.0.0.0/8'
```

## SQL Expression (Catch-All)

Function: `sql_expression` -- use for any custom row-level logic. See `custom/SKILL.md` for advanced patterns (window functions, etc.).

```yaml
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "end_date >= start_date"
      msg: "end_date is before start_date"
```

## Complex Column Types

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

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#row-level-checks-reference
