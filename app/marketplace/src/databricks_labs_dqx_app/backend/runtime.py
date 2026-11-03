from .config import AppConfig, conf
from .setup.resources import ActiveResources


class Runtime:
    def __init__(self) -> None:
        self.config: AppConfig = conf
        self.resources: ActiveResources | None = None

    def activate(self, resources: ActiveResources) -> None:
        """Publish resources that passed setup readiness checks."""
        self.resources = resources

    def deactivate(self) -> None:
        """Clear resources when the owning application lifespan stops."""
        self.resources = None

    def require_resources(self) -> ActiveResources:
        """Return active resources or fail while setup is incomplete."""
        if self.resources is None:
            raise RuntimeError("DQX Studio resources are not ready")
        return self.resources


rt = Runtime()
