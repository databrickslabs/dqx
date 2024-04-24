import datetime


def val_to_str(v, include_sql_quotes=True):
    quote = "'" if include_sql_quotes else ""
    if isinstance(v, datetime.datetime):
        return f"{quote}{v.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(v, datetime.date):
        return f"{quote}{v.isoformat()}{quote}"

    if isinstance(v, int) or isinstance(v, float):
        return str(v)

    # TODO: do correct escaping
    return f"{quote}{v}{quote}"


def val_maybe_to_str(v, include_sql_quotes=True):
    quote = "'" if include_sql_quotes else ""
    if isinstance(v, datetime.datetime):
        return f"{quote}{v.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(v, datetime.date):
        return f"{quote}{v.isoformat()}{quote}"

    return v
