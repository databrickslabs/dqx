# DQX Studio — Marketplace content packs

Each `*.yaml` file in this directory is a **content pack**: a themed bundle of
reusable data-quality rules that appears in the DQX Studio **Marketplace**
(admin-only), where an admin can preview and import individual rules into the
Rules Registry as reusable templates.

**Repo:** https://github.com/databrickslabs/dqx

## Contributing a pack or a rule

Add a rule to an existing pack, or drop in a new `<domain>.yaml` file here.
Rules import as reusable templates, so column arguments use `{{slot}}`
placeholders rather than real column names.

```yaml
id: pricing-and-money            # stable, kebab-case; unique across packs
title: Pricing & Money           # shown on the pack card
icon: DollarSign                 # any lucide-react icon name
rules:
  - name: Amount must be non-zero
    description: Monetary amount must not equal zero.   # one sentence, one period
    industries: [banking, retail] # omit / [] => general (shows everywhere)
    regions: [global]             # omit / [] => global  (shows everywhere)
    criticality: warn             # DQX execution field (warn | error)
    user_metadata:
      dimension: Validity         # Validity|Completeness|Accuracy|Consistency|Uniqueness|Timeliness
      severity: Medium            # Low|Medium|High|Critical
    check:
      function: is_not_equal_to   # any registered DQX row-level check, or sql_expression
      arguments:
        column: "{{amount}}"
        value: 0
```

### Rules that must hold

- **Reusable only** — no rule that bakes in a table-specific allow-list or
  arbitrary bounds, and no bare `is_not_null` / uniqueness duplicate of the
  Standard checks pack.
- **`sql_expression` must be true-when-good** — DQX flags rows where the
  expression is `false`, so a "bad pattern" rule is written as `not (<bad>)`.
- **Lookup lists, not shape regexes**, for closed vocabularies (ISO country /
  currency / language codes use `is_in_list`).
- Every rule is validated at load time against `DQEngine.validate_checks`; a
  pack with any invalid rule is skipped (with a warning) rather than served.
  Run `pytest app/tests/test_marketplace_packs.py` to check your pack locally.
