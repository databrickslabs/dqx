"""Tag identifiers shared by the Studio tag filters and the Results tag breakdowns.

A tag id is ``<source>:<key>``. *source* says where the tag lives: on a rule
(free-text Studio tags, frozen with each run), on a Unity Catalog catalog,
schema or table, or on a column. A column's tags are the Unity Catalog tags its
rule ties to the column it checks (the rule's slot tags), never the column's
own Unity Catalog tags. The source keeps tags with the same key apart.
"""

RULE_TAG_SOURCE = "rule"
COLUMN_TAG_SOURCE = "column"
UC_TAG_SOURCES: tuple[str, ...] = ("catalog", "schema", "table")
TAG_SOURCES: tuple[str, ...] = (*UC_TAG_SOURCES, COLUMN_TAG_SOURCE, RULE_TAG_SOURCE)


def tag_id(source: str, key: str) -> str:
    """Return the tag id of *key* on *source*."""
    return f"{source}:{key}"


def split_tag_id(value: str) -> tuple[str, str] | None:
    """Split a tag id into (source, key), or None when it has no known source or no key."""
    source, sep, key = value.partition(":")
    if not sep or not key or source not in TAG_SOURCES:
        return None
    return source, key
