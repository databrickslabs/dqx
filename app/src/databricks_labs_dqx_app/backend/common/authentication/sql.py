class SQLAuthentication:
    """Auth resolver for SQL Warehouse connections using OBO bearer tokens."""

    def __init__(self, bearer: str | None = None) -> None:
        self._bearer = bearer

    @property
    def access_token(self) -> str:
        if self._bearer:
            return self._bearer

        raise ValueError(
            "No SQL authentication token available. "
            "An OBO bearer token is required (X-Forwarded-Access-Token header)."
        )
