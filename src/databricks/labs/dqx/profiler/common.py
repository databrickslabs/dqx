import datetime


def val_to_str(value, include_sql_quotes=True):
    quote = "'" if include_sql_quotes else ""
    if isinstance(value, datetime.datetime):
        return f"{quote}{value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(value, datetime.date):
        return f"{quote}{value.isoformat()}{quote}"

    if isinstance(value, (int, float)):
        return str(value)

    # TODO: do correct escaping
    return f"{quote}{value}{quote}"


def val_maybe_to_str(value, include_sql_quotes=True):
    quote = "'" if include_sql_quotes else ""
    if isinstance(value, datetime.datetime):
        return f"{quote}{value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(value, datetime.date):
        return f"{quote}{value.isoformat()}{quote}"

    return value
