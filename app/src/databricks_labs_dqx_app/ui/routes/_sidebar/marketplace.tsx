import { useEffect, Suspense } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Skeleton } from "@/components/ui/skeleton";
import { usePermissions } from "@/hooks/use-permissions";
import { MarketplacePage } from "@/components/marketplace/MarketplacePage";

export const Route = createFileRoute("/_sidebar/marketplace")({
  component: () => <MarketplaceRoute />,
});

function MarketplaceRoute() {
  const { isAdmin } = usePermissions();
  const navigate = useNavigate();
  useEffect(() => {
    if (!isAdmin) navigate({ to: "/rules/active", replace: true });
  }, [isAdmin, navigate]);
  if (!isAdmin) return null;
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={() => null}>
          <Suspense fallback={<Skeleton className="h-96 w-full" />}>
            <MarketplacePage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
