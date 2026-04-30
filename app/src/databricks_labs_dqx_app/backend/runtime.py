from .config import AppConfig, conf


class Runtime:
    def __init__(self) -> None:
        self.config: AppConfig = conf


rt = Runtime()
