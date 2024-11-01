import re

import databricks.sdk.useragent as ua
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.dqx.__about__ import __version__

install_logger()

ua.semver_pattern = re.compile(
    r"^"
    r"(?P<major>0|[1-9]\d*)\.(?P<minor>x|0|[1-9]\d*)(\.(?P<patch>x|0|[1-9x]\d*))?"
    r"(?:-(?P<pre_release>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

# Add ucx/<version> for projects depending on ucx as a library
ua.with_extra("dqx", __version__)

# Add ucx/<version> for re-packaging of ucx, where product name is omitted
ua.with_product("dqx", __version__)
