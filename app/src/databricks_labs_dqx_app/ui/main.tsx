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
import { errorToast } from "@/lib/toast";
import { getCurrentUserQueryKey } from "@/lib/api";

const mutationCache = new MutationCache({
  onSuccess: (_data, _vars, _ctx, mutation) => {
    const meta = mutation.meta as { successMessage?: string } | undefined;
    if (meta?.successMessage) toast.success(meta.successMessage);
  },
  onError: (_error, _vars, _ctx, mutation) => {
    const meta = mutation.meta as { errorMessage?: string } | undefined;
    // B2-30: error toasts carry a "Copy" action (copies the message text) in
    // addition to the global dismiss X on the Toaster.
    if (meta?.errorMessage) errorToast(meta.errorMessage);
  },
});

// Create a new query client instance
const queryClient = new QueryClient({
  mutationCache,
  defaultOptions: {
    queries: {
      // Don't retry by default - AuthGuard handles initial auth flow
      retry: false,
      // B2-22: keep query results fresh for 5 minutes so app-wide data
      // (role, version, approvals mode, global-results flag, and everything
      // else) is served from cache instead of refetching on every route
      // mount / tab switch. Run-completion invalidation and explicit
      // invalidateQueries still refresh what needs to change.
      staleTime: 5 * 60 * 1000,
    },
  },
});

// Session-stable identity: the SCIM current-user doesn't change within a
// session. The role, version, approvals-mode and global-results queries are
// already pinned to staleTime: Infinity at their hooks, but current-user was
// still riding the 5-minute default and re-fetching whenever it lapsed —
// firing repeatedly across a session (once per component that mounts it after
// the window expires). Pin it here centrally, by query key, so the keyed
// hooks (suspense + non-suspense) all inherit it without per-call-site churn.
// (Timezone is pinned at its own hook in api-custom.ts, alongside the same
// reasoning, because that hand-written hook passes staleTime directly into
// useQuery — which would override a query-default set here.)
queryClient.setQueryDefaults(getCurrentUserQueryKey(), { staleTime: Infinity });

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
