"""Unity Gateway model services — the transport behind every AI call in Studio.

Databricks retires the workspace-bound Foundation Model API serving endpoints
(``databricks-*``) on March 30, 2027, and the *Enforce Unity Gateway* workspace
setting disables them sooner. Models are now Unity Catalog securables called
model services (for example ``system.ai.claude-sonnet-4-5``), queried through the
workspace ``/ai-gateway/mlflow/v1`` routes and governed by Unity Catalog grants:
callers need ``EXECUTE`` on the model service plus ``USE_CATALOG`` and
``USE_SCHEMA`` on its catalog and schema.

The pinned Databricks SDK has no typed client for these routes, so
:class:`ModelServices` calls them through the client's authenticated
``api_client``. Whoever owns the injected client is the caller — the signed-in
user (OBO, which needs the app's ``ai-gateway`` user API scope) or the app's
service principal.
"""

from typing import Any
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

SYSTEM_MODEL_SCHEMA = "system.ai"
CHAT_COMPLETIONS_API = "mlflow/v1/chat/completions"
EMBEDDINGS_API = "mlflow/v1/embeddings"

_LEGACY_ENDPOINT_PREFIX = "databricks-"
_NAME_PREFIX = "model-services/"
_CHAT_PATH = "/ai-gateway/mlflow/v1/chat/completions"
_EMBEDDINGS_PATH = "/ai-gateway/mlflow/v1/embeddings"
_MODEL_SERVICES_PATH = "/api/2.1/unity-catalog/model-services"


def model_service_name(name: str) -> str:
    """Return the Unity Gateway model service name for a configured model.

    Legacy pay-per-token endpoint names map onto their ``system.ai`` model service
    (``databricks-claude-sonnet-4-5`` → ``system.ai.claude-sonnet-4-5``), so settings
    saved before the migration keep working. Any other value is returned trimmed.

    Args:
        name: Configured model name, either a model service or a legacy endpoint name.

    Returns:
        The model service name, or ``""`` when *name* is blank.
    """
    cleaned = (name or "").strip()
    if "." not in cleaned and cleaned.startswith(_LEGACY_ENDPOINT_PREFIX):
        return f"{SYSTEM_MODEL_SCHEMA}.{cleaned[len(_LEGACY_ENDPOINT_PREFIX):]}"
    return cleaned


class ModelServices:
    """Query and look up Unity Gateway model services as the injected client's identity.

    Args:
        ws: Workspace client whose identity makes the calls.
    """

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    def chat(
        self,
        *,
        model: str,
        messages: list[dict[str, str]],
        max_tokens: int,
        temperature: float | None = None,
    ) -> dict[str, Any]:
        """Send an OpenAI-compatible chat completion request to *model*.

        Args:
            model: Model service name, e.g. ``system.ai.claude-sonnet-4-5``.
            messages: Chat messages as ``{"role": ..., "content": ...}``.
            max_tokens: Output token cap.
            temperature: Optional sampling temperature, omitted when ``None``.

        Returns:
            The raw chat completion response.
        """
        body: dict[str, Any] = {"model": model, "messages": messages, "max_tokens": max_tokens}
        if temperature is not None:
            body["temperature"] = temperature
        return self._post(_CHAT_PATH, body)

    def embed(self, *, model: str, inputs: list[str]) -> dict[str, Any]:
        """Embed *inputs* with *model*; the response holds one ``data`` element per input.

        Args:
            model: Embedding model service name, e.g. ``system.ai.gte-large-en``.
            inputs: Texts to embed.

        Returns:
            The raw embeddings response.
        """
        return self._post(_EMBEDDINGS_PATH, {"model": model, "input": inputs})

    def exists(self, name: str) -> bool:
        """Return whether model service *name* exists and is visible to the caller.

        Each dot-separated part of *name* is URL-encoded, so an admin-configured name can't
        redirect the lookup to another API path (``/``, ``?``, ``#``, ``..``).

        Raises:
            DatabricksError: The lookup failed for a reason other than the model service not existing.
        """
        encoded = ".".join(quote(part, safe="") for part in name.split("."))
        try:
            self._ws.api_client.do("GET", f"{_MODEL_SERVICES_PATH}/{encoded}")
        except NotFound:
            return False
        return True

    def list_names(self, *, api_type: str, schema: str = SYSTEM_MODEL_SCHEMA) -> list[str]:
        """List the model services in *schema* that support *api_type*, sorted by name.

        Args:
            api_type: Required API, e.g. :data:`CHAT_COMPLETIONS_API`.
            schema: Schema to list, as ``catalog.schema``.

        Returns:
            Fully qualified model service names.
        """
        names: set[str] = set()
        query: dict[str, str] = {"parent": f"schemas/{schema}"}
        while True:
            response = self._ws.api_client.do("GET", _MODEL_SERVICES_PATH, query=query)
            if not isinstance(response, dict):
                break
            for service in response.get("model_services") or []:
                name = str(service.get("name") or "").removeprefix(_NAME_PREFIX)
                if name and api_type in (service.get("supported_api_types") or []):
                    names.add(name)
            token = response.get("next_page_token")
            if not token:
                break
            query = {**query, "page_token": str(token)}
        return sorted(names)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        response = self._ws.api_client.do("POST", path, body=body)
        return response if isinstance(response, dict) else {}
