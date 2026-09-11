interface StructuredApiErrorDetail {
  summary?: unknown;
}

/** Return a render-safe message from string or structured FastAPI error details. */
export function extractApiError(error: unknown, fallback: string): string {
  const response = error as { response?: { data?: { detail?: unknown } } } | null | undefined;
  const detail = response?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const summary = (detail as StructuredApiErrorDetail).summary;
    if (typeof summary === "string") return summary;
  }
  return fallback;
}
