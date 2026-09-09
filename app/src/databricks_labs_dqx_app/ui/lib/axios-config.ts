import axios from "axios";

/**
 * Configure axios defaults and interceptors for DQX Studio.
 *
 * Note: Initial authentication is handled by AuthGuard component.
 * This configuration only provides error logging for debugging.
 * DO NOT add retry logic here - it interferes with AuthGuard.
 */

// Serialize ARRAY query params as repeated keys (`dimension=a&dimension=b`),
// never axios's bracketed default (`dimension[]=a`). FastAPI's
// `Annotated[list[str] | None, Query()]` parameters only collect REPEATED
// bare keys — a bracketed `dimension[]` is a different parameter name, so
// the backend silently received None for every list param and the results
// drilldown facet filters (dimension/severity/rule/column) never filtered
// anything server-side. dqlake's hand-rolled client appends repeated keys
// (`searchParams.append(...)` per value); this default restores that
// contract for the whole orval/axios client. `indexes: null` = no brackets.
axios.defaults.paramsSerializer = { indexes: null };

// Response interceptor for logging only (no retries)
axios.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    // Only log non-401 errors to avoid noise during initial auth
    // 401 errors during initial load are expected and handled by AuthGuard
    if (error.response?.status === 403) {
      console.error(
        "[DQX] 403 Forbidden - You don't have permission to access this resource."
      );
    } else if (error.response?.status >= 500) {
      console.error(
        `[DQX] Server error (${error.response.status}): ${error.response.statusText}`
      );
    } else if (error.response?.status !== 401) {
      // Log other errors except 401 (handled by AuthGuard)
      console.error(`[DQX] HTTP ${error.response?.status || 'error'}:`, error.message);
    }

    return Promise.reject(error);
  }
);

export { axios };

