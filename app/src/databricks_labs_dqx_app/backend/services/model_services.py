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

import logging
import re
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters

JsonObject = dict[str, object]

SYSTEM_MODEL_SCHEMA = "system.ai"
CHAT_COMPLETIONS_API = "mlflow/v1/chat/completions"
EMBEDDINGS_API = "mlflow/v1/embeddings"

_LEGACY_ENDPOINT_PREFIX = "databricks-"
_NAME_PREFIX = "model-services/"
_CHAT_PATH = "/ai-gateway/mlflow/v1/chat/completions"
_EMBEDDINGS_PATH = "/ai-gateway/mlflow/v1/embeddings"
_MODEL_SERVICES_PATH = "/api/2.1/unity-catalog/model-services"
# Databricks reports a token missing an OAuth scope as "... does not have required scopes: <scope>".
# The SDK maps it to the same PermissionDenied (error code PERMISSION_DENIED) as a missing grant,
# so the wording is the only signal; match the phrase, not any mention of "scope".
_MISSING_SCOPE_RE = re.compile(r"required scopes?\b", re.IGNORECASE)

logger = logging.getLogger(__name__)


class ModelServiceUnavailableError(Exception):
    """Raised when a model service can't be called: it doesn't exist, or the caller lacks access.

    :attr:`reason` names the fix and is safe to show to the user.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


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
    ) -> JsonObject:
        """Send an OpenAI-compatible chat completion request to *model*.

        Args:
            model: Model service name, e.g. ``system.ai.claude-sonnet-4-5``.
            messages: Chat messages as ``{"role": ..., "content": ...}``.
            max_tokens: Output token cap.
            temperature: Optional sampling temperature, omitted when ``None``.

        Returns:
            The raw chat completion response.

        Raises:
            ModelServiceUnavailableError: The model doesn't exist or the caller lacks access.
        """
        body: JsonObject = {"model": model, "messages": messages, "max_tokens": max_tokens}
        if temperature is not None:
            body["temperature"] = temperature
        return self._post(_CHAT_PATH, model, body)

    def embed(self, *, model: str, inputs: list[str]) -> JsonObject:
        """Embed *inputs* with *model*; the response holds one ``data`` element per input.

        Args:
            model: Embedding model service name, e.g. ``system.ai.gte-large-en``.
            inputs: Texts to embed.

        Returns:
            The raw embeddings response.

        Raises:
            ModelServiceUnavailableError: The model doesn't exist or the caller lacks access.
        """
        return self._post(_EMBEDDINGS_PATH, model, {"model": model, "input": inputs})

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
        seen_tokens: set[str] = set()
        while True:
            response = self._ws.api_client.do("GET", _MODEL_SERVICES_PATH, query=query)
            if not isinstance(response, dict):
                break
            services = response.get("model_services")
            for service in services if isinstance(services, list) else []:
                if not isinstance(service, dict):
                    continue
                name = str(service.get("name") or "").removeprefix(_NAME_PREFIX)
                api_types = service.get("supported_api_types")
                if name and isinstance(api_types, list) and api_type in api_types:
                    names.add(name)
            token = str(response.get("next_page_token") or "")
            # A repeated token would page forever; stop instead.
            if not token or token in seen_tokens:
                break
            seen_tokens.add(token)
            query = {**query, "page_token": token}
        return sorted(names)

    def _post(self, path: str, model: str, body: JsonObject) -> JsonObject:
        try:
            response = self._ws.api_client.do("POST", path, body=body)
        except NotFound as e:
            logger.warning(f"model_service_not_found model={replace_control_characters(model)}")
            raise ModelServiceUnavailableError(
                f"AI model {model} doesn't exist in this workspace. An administrator can check the AI "
                "settings in Settings > AI."
            ) from e
        except PermissionDenied as e:
            # Databricks' own error text (never prompt content) says which scope or grant is missing.
            logger.warning(
                f"model_service_permission_denied model={replace_control_characters(model)} "
                f"error={replace_control_characters(str(e))[:300]}"
            )
            if _MISSING_SCOPE_RE.search(str(e)):
                # The user's token lacks the app's ``ai-gateway`` user API scope, not a grant.
                raise ModelServiceUnavailableError(
                    "Studio isn't allowed to call AI models for you yet. An administrator needs to add the "
                    "ai-gateway user API scope to the app (redeploying the bundle does this), then you "
                    "need to sign in again."
                ) from e
            raise ModelServiceUnavailableError(
                f"You don't have access to AI model {model}. Ask a metastore admin to grant you "
                f"EXECUTE on model service {model}, plus USE_CATALOG and USE_SCHEMA on its "
                "catalog and schema. If you signed in before Studio was last upgraded, sign in again first."
            ) from e
        return response if isinstance(response, dict) else {}
