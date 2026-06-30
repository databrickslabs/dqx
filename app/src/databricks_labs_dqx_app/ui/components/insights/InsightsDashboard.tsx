import { Link, useLocation } from "@tanstack/react-router";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Suspense, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";
import { AlertCircle, ExternalLink, LayoutDashboard, Settings } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { ShinyText } from "@/components/anim/ShinyText";
import { FadeIn } from "@/components/anim/FadeIn";
import { cn } from "@/lib/utils";
import { usePermissions } from "@/hooks/use-permissions";
import { useEmbeddedDashboard } from "@/lib/api-custom";

function SectionError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> {t("insights.failedToLoad")}
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        {t("common.retry")}
      </Button>
    </div>
  );
}

function EmptyState({ isAdmin }: { isAdmin: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="flex h-[calc(100vh-220px)] items-center justify-center">
      <div className="flex flex-col items-center gap-4 max-w-md text-center px-6">
        <div className="h-14 w-14 rounded-full bg-muted flex items-center justify-center">
          <LayoutDashboard className="h-7 w-7 text-muted-foreground" />
        </div>
        <div className="space-y-1">
          <h2 className="text-lg font-semibold">{t("insights.emptyTitle")}</h2>
          <p className="text-sm text-muted-foreground">
            {isAdmin ? t("insights.emptyAdminHint") : t("insights.emptyViewerHint")}
          </p>
        </div>
        {isAdmin && (
          <Button asChild variant="default" size="sm" className="gap-1.5">
            <Link to="/config">
              <Settings className="h-3.5 w-3.5" />
              {t("insights.goToConfig")}
            </Link>
          </Button>
        )}
      </div>
    </div>
  );
}

function DashboardFrame() {
  const { t } = useTranslation();
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

  const title = data.title || t("insights.defaultTitle");

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <h2 className="text-base font-medium">{title}</h2>
          {data.is_default && !data.is_set && (
            <span className="text-[10px] uppercase tracking-wide text-muted-foreground border rounded-full px-2 py-0.5">
              {t("insights.badgeDefault")}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          {openUrl && (
            <Button asChild variant="ghost" size="sm" className="gap-1.5 text-xs text-muted-foreground">
              <a href={openUrl} target="_blank" rel="noopener noreferrer" title={t("insights.openInDbxTooltip")}>
                <ExternalLink className="h-3.5 w-3.5" />
                {t("insights.openInDbx")}
              </a>
            </Button>
          )}
          {isAdmin && (
            <Button asChild variant="ghost" size="sm" className="gap-1.5 text-xs text-muted-foreground">
              <Link to="/config" title={t("insights.configureTooltip")}>
                <Settings className="h-3.5 w-3.5" />
                {t("insights.configure")}
              </Link>
            </Button>
          )}
        </div>
      </div>
      {/* Same-workspace iframes inherit Databricks Apps' user-token-passthrough
          cookies, so the dashboard sees each viewer's identity and UC enforces
          row-level visibility on the data behind it. We deliberately don't
          inject tokens or proxy the response — keeps PII off the app server.

          ``allow-storage-access-by-user-activation`` lets the Lakeview embed
          use the Storage Access API to read its first-party cookies from
          inside the sandbox; without it the embed logs "requestStorageAccess:
          Refused ... the 'allow-storage-access-by-user-activation' keyword is
          not set" and can fall back to a degraded/unauthenticated state. */}
      <iframe
        key={embedUrl}
        src={embedUrl}
        title={title}
        className="w-full rounded-md border bg-background min-h-[calc(100vh-220px)]"
        referrerPolicy="strict-origin"
        sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads allow-storage-access-by-user-activation"
      />
    </div>
  );
}

function InsightsContent() {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <PageBreadcrumb page={t("insights.title")} />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text={t("insights.title")} speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">{t("insights.subtitle")}</p>
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

/**
 * Persistent host for the Insights dashboard.
 *
 * The embedded Lakeview dashboard lives in an ``<iframe>``, and a browser
 * reloads an iframe whenever it is removed from / re-attached to the DOM. If
 * we rendered it inside the ``/insights`` route component it would unmount on
 * every navigation away and re-fetch + re-authenticate + re-render on the way
 * back — the "reloads every time I jump back" behaviour.
 *
 * Instead this host is rendered once inside the persistent ``_sidebar`` layout
 * (which stays mounted while only the ``<Outlet/>`` swaps between pages). We:
 *   1. Lazily mount the iframe the first time the user opens Insights (so users
 *      who never visit it don't pay the dashboard load), via a latch ref that
 *      never flips back to false.
 *   2. Keep it mounted forever after, toggling ``display`` with the route.
 *      ``display:none`` keeps the iframe's document alive — it is NOT reparented
 *      — so returning to Insights shows the already-loaded dashboard instantly.
 *
 * The ``/insights`` route renders nothing; the layout hides the empty
 * ``<Outlet/>`` wrapper while this host is visible.
 */
export function InsightsDashboardHost() {
  const location = useLocation();
  const onInsights = location.pathname.startsWith("/insights");

  // Latch: once the user has visited Insights, keep the iframe mounted for the
  // rest of the session. Writing a ref during render is safe and avoids an
  // extra state update — the location change already re-renders this host.
  const hasVisited = useRef(false);
  if (onInsights) hasVisited.current = true;
  if (!hasVisited.current) return null;

  return (
    <div className={cn(onInsights ? "block" : "hidden")} aria-hidden={!onInsights}>
      <div className="flex flex-col gap-4 p-6 pt-0 max-w-7xl mx-auto">
        <InsightsContent />
      </div>
    </div>
  );
}
