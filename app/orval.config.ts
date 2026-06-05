// Orval config — generates the typed React Query / Axios client in
// ``src/databricks_labs_dqx_app/ui/lib/api.ts`` from the FastAPI
// app's OpenAPI schema. ``scripts/build_app.py`` is responsible for
// producing ``.build/openapi.json`` (via ``app.openapi()``) before
// invoking ``orval``, so the input is always in sync with the
// backend's current routes / pydantic models.
//
// Replaces the previously apx-generated ``.apx/orval.config.ts``.
// Paths are relative to ``app/`` (orval's cwd when invoked from the
// ``Makefile`` / ``scripts/build_app.py``).
import { defineConfig } from "orval";

export default defineConfig({
  api: {
    input: ".build/openapi.json",
    output: {
      target: "src/databricks_labs_dqx_app/ui/lib/api.ts",
      client: "react-query",
      httpClient: "axios",
      prettier: true,
      override: {
        query: {
          useQuery: true,
          useSuspenseQuery: true,
        },
      },
    },
  },
});
