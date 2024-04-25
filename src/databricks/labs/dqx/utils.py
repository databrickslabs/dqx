from pyspark.sql import Column


def get_column_name(col: Column) -> str:
    """
    PySpark doesn't allow to directly access the column name with respect to aliases from an unbound column.
    It is necessary to parse this out from the string representation.

    This works on columns with one or more aliases as well as not aliased columns.

    :param col: Column
    :return: Col name alias as str
    """
    return str(col).removeprefix("Column<'").removesuffix("'>").split(" AS ")[-1]
