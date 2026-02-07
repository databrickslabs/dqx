import axios from "axios";

/**
 * Configure axios defaults and interceptors for the DQX app.
 * 
 * Note: Initial authentication is handled by AuthGuard component.
 * This configuration only provides error logging for debugging.
 * DO NOT add retry logic here - it interferes with AuthGuard.
 */

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

