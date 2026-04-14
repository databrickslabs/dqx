import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "@/styles/globals.css";
import { routeTree } from "@/types/routeTree.gen";

import { RouterProvider, createRouter } from "@tanstack/react-router";
import { MutationCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AuthGuard } from "@/components/AuthGuard";
import { toast } from "sonner";

const mutationCache = new MutationCache({
  onSuccess: (_data, _vars, _ctx, mutation) => {
    const meta = mutation.meta as { successMessage?: string } | undefined;
    if (meta?.successMessage) toast.success(meta.successMessage);
  },
  onError: (_error, _vars, _ctx, mutation) => {
    const meta = mutation.meta as { errorMessage?: string } | undefined;
    if (meta?.errorMessage) toast.error(meta.errorMessage);
  },
});

// Create a new query client instance
const queryClient = new QueryClient({
  mutationCache,
  defaultOptions: {
    queries: {
      // Don't retry by default - AuthGuard handles initial auth flow
      retry: false,
    },
  },
});

const router = createRouter({
  routeTree,
  context: {
    queryClient,
  },
  // Disable preloading to prevent API calls before AuthGuard confirms auth is ready
  defaultPreload: false,
  // Since we're using React Query, we don't want loader calls to ever be stale
  // This will ensure that the loader is always called when the route is preloaded or visited
  defaultPreloadStaleTime: 0,
  scrollRestoration: true,
});

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
      <AuthGuard>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AuthGuard>
    </StrictMode>,
  );
}
