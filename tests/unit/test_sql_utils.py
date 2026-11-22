import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.sql_utils import quote_table_name


@pytest.mark.parametrize(
    "name, expected",
    [
        ("view", "`view`"),
        ("schema.table", "`schema`.`table`"),
        ("catalog.schema.table", "`catalog`.`schema`.`table`"),
        ("1abc.schema.table", "`1abc`.`schema`.`table`"),
        ("`catalog`.`schema`.`table`", "`catalog`.`schema`.`table`"),
        ("`my-catalog`.schema.table", "`my-catalog`.`schema`.`table`"),
        ("catalog.schema.`weird.name`", "`catalog`.`schema`.`weird.name`"),
        ("`tick``name`.schema.table", "`tick``name`.`schema`.`table`"),
    ],
)
def test_quote_table_name_quotes_every_part(name: str, expected: str):
    assert quote_table_name(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        "",
        "catalog.schema.table; DROP TABLE catalog.schema.other",
        "catalog.schema.table`",
        "`catalog`schema`.table",
        "catalog..table",
        "catalog.schema.table.extra",
        "my-catalog.schema.table",
        "catalog schema.table",
        "catalog.schema.table\n",
        "``.schema.table",
    ],
)
def test_quote_table_name_rejects_invalid_names(name: str):
    with pytest.raises(InvalidParameterError, match="Invalid table or view name"):
        quote_table_name(name)
