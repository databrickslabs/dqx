import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";

import axios from "axios";

if (import.meta.env.DEV) {
  axios.defaults.baseURL = "http://localhost:8000/";
}

import { RouterProvider, createRouter } from "@tanstack/react-router";
// Import the generated route tree
import { routeTree } from "./routeTree.gen";
// Import the query client for data fetching

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// Create a new query client instance, and use http://localhost:8000/api as the base URL for the API
const queryClient = new QueryClient();

const router = createRouter({
  routeTree,
  context: {
    queryClient,
  },
  defaultPreload: "intent",
  // Since we're using React Query, we don't want loader calls to ever be stale
  // This will ensure that the loader is always called when the route is preloaded or visited
  defaultPreloadStaleTime: 0,
  scrollRestoration: true,
});

// Register the router instance for type safety
declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

// Register things for typesafety
declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const rootElement = document.getElementById("root")!;

if (!rootElement.innerHTML) {
  const root = createRoot(rootElement);
  root.render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </StrictMode>,
  );
}
