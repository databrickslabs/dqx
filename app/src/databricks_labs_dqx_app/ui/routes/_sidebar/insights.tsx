import { createFileRoute, Link } from "@tanstack/react-router";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Suspense, useMemo } from "react";
import { AlertCircle, ExternalLink, LayoutDashboard, Settings } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { ShinyText } from "@/components/anim/ShinyText";
import { FadeIn } from "@/components/anim/FadeIn";
import { usePermissions } from "@/hooks/use-permissions";
import { useEmbeddedDashboard } from "@/lib/api-custom";

export const Route = createFileRoute("/_sidebar/insights")({
  component: () => <InsightsPage />,
});

function SectionError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> Failed to load Insights
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        Retry
      </Button>
    </div>
  );
}

function EmptyState({ isAdmin }: { isAdmin: boolean }) {
  return (
    <div className="flex h-[calc(100vh-220px)] items-center justify-center">
      <div className="flex flex-col items-center gap-4 max-w-md text-center px-6">
        <div className="h-14 w-14 rounded-full bg-muted flex items-center justify-center">
          <LayoutDashboard className="h-7 w-7 text-muted-foreground" />
        </div>
        <div className="space-y-1">
          <h2 className="text-lg font-semibold">No dashboard configured</h2>
          <p className="text-sm text-muted-foreground">
            {isAdmin
              ? "Pin a Databricks AI/BI dashboard ID in Configuration to render it here."
              : "Ask an administrator to pin a dashboard in Configuration."}
          </p>
        </div>
        {isAdmin && (
          <Button asChild variant="default" size="sm" className="gap-1.5">
            <Link to="/config">
              <Settings className="h-3.5 w-3.5" />
              Go to Configuration
            </Link>
          </Button>
        )}
      </div>
    </div>
  );
}

function DashboardFrame() {
  const { data, isLoading } = useEmbeddedDashboard();
  const { isAdmin } = usePermissions();

  const embedUrl = useMemo(() => {
    if (!data?.dashboard_id || !data.workspace_host) return null;
    return `${data.workspace_host}/embed/dashboardsv3/${data.dashboard_id}`;
  }, [data]);

  const openUrl = useMemo(() => {
    if (!data?.dashboard_id || !data.workspace_host) return null;
    return `${data.workspace_host}/dashboardsv3/${data.dashboard_id}`;
  }, [data]);

  if (isLoading) {
    return <Skeleton className="h-[calc(100vh-220px)] w-full" />;
  }

  if (!data || !embedUrl) {
    return <EmptyState isAdmin={isAdmin} />;
  }

  const title = data.title || "Quality dashboard";

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <h2 className="text-base font-medium">{title}</h2>
          {data.is_default && !data.is_set && (
            <span className="text-[10px] uppercase tracking-wide text-muted-foreground border rounded-full px-2 py-0.5">
              Default
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          {openUrl && (
            <Button asChild variant="ghost" size="sm" className="gap-1.5 text-xs text-muted-foreground">
              <a href={openUrl} target="_blank" rel="noopener noreferrer" title="Open the full dashboard in a new tab">
                <ExternalLink className="h-3.5 w-3.5" />
                Open in Databricks
              </a>
            </Button>
          )}
          {isAdmin && (
            <Button asChild variant="ghost" size="sm" className="gap-1.5 text-xs text-muted-foreground">
              <Link to="/config" title="Change the dashboard pinned to this page">
                <Settings className="h-3.5 w-3.5" />
                Configure
              </Link>
            </Button>
          )}
        </div>
      </div>
      {/* Same-workspace iframes inherit Databricks Apps' user-token-passthrough
          cookies, so the dashboard sees each viewer's identity and UC enforces
          row-level visibility on the data behind it. We deliberately don't
          inject tokens or proxy the response — keeps PII off the app server. */}
      <iframe
        key={embedUrl}
        src={embedUrl}
        title={title}
        className="w-full rounded-md border bg-background min-h-[calc(100vh-220px)]"
        referrerPolicy="strict-origin"
        sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads"
      />
    </div>
  );
}

function InsightsPage() {
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <PageBreadcrumb page="Insights" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Insights" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            A Databricks dashboard pinned to your data quality metrics. Customise the dashboard in
            Databricks and Insights picks up your changes immediately.
          </p>
        </div>
      </div>

      <QueryErrorResetBoundary>
        {({ reset }) => (
          <FadeIn delay={0.05}>
            <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
              <Suspense fallback={<Skeleton className="h-[calc(100vh-220px)] w-full" />}>
                <DashboardFrame />
              </Suspense>
            </ErrorBoundary>
          </FadeIn>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}
