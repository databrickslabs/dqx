"""Sanitized failures for required Studio setup actions."""


class RequiredViewSetupError(RuntimeError):
    """Required score or entitlement objects could not be created."""

    def __init__(self) -> None:
        super().__init__("Could not create required Studio views.")
