// orval.config.ts
import type { Config } from "orval";

export default {
  fastapi: {
    input: "http://127.0.0.1:8000/api/openapi.json",
    output: {
      target: "./src/lib/api.ts",
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
} satisfies Config;
