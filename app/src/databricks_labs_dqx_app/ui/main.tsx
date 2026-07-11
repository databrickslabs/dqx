import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "@/styles/globals.css";
// Side-effect import: installs the axios defaults the generated client
// relies on — notably the repeated-key array-param serializer that the
// FastAPI list query params (results drilldown facet filters) require.
import "@/lib/axios-config";
import { i18nReady } from "@/lib/i18n";
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
  // Restore scroll position on back/forward navigation everywhere except
  // the Rules Registry, which is a filterable list — persisting scroll
  // there means re-opening it (e.g. after approving/rejecting a rule)
  // lands you mid-list instead of at the top, which reads as broken.
  scrollRestoration: ({ location }) => !location.pathname.startsWith("/registry-rules"),
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
  const renderApp = () => {
    root.render(
      <StrictMode>
        <AuthGuard>
          <QueryClientProvider client={queryClient}>
            <RouterProvider router={router} />
          </QueryClientProvider>
        </AuthGuard>
      </StrictMode>,
    );
  };

  void i18nReady.then(renderApp).catch((err) => {
    console.error("i18n init failed; rendering with English fallback", err);
    renderApp();
  });
}
