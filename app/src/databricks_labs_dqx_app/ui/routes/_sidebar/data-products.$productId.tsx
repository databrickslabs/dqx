import { createFileRoute, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle, RotateCcw } from "lucide-react";
import { useGetDataProductSuspense, type DataProductOut } from "@/lib/api";
import selector from "@/lib/selector";
import { usePermissions } from "@/hooks/use-permissions";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { ProductTabsShell, PRODUCT_TAB_KEYS, type ProductTabKey } from "@/components/data-products/ProductTabsShell";
import { ProductHeader } from "@/components/data-products/ProductHeader";
import { ProductAboutTab } from "@/components/data-products/ProductAboutTab";
import { ProductSharingTab } from "@/components/data-products/ProductSharingTab";
import { useEditProductState } from "@/components/data-products/useEditProductState";

export const Route = createFileRoute("/_sidebar/data-products/$productId")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={DataProductDetailError}>
          <Suspense fallback={<DataProductDetailSkeleton />}>
            <DataProductDetailPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DataProductDetailError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("common.loadFailed")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">{t("common.retryHint")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

function DataProductDetailSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-48" />
      </div>
      <Skeleton className="h-96 w-full max-w-5xl" />
    </div>
  );
}

/** Per-tab boundary so one tab's data failure doesn't blow up the whole
 *  page — ported from dqlake's `TabBoundary`. About/Sharing don't fetch
 *  anything beyond the already-suspended product, but Tables/Runs/Schedule
 *  (Tasks 8-10) will, so the shell is wired for it now. */
function TabBoundary({ label, children }: { label: string; children: React.ReactNode }) {
  const { t } = useTranslation();
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary
          onReset={reset}
          fallbackRender={({ resetErrorBoundary }) => (
            <div className="flex flex-col items-center justify-center py-12 text-center max-w-5xl">
              <AlertCircle className="h-8 w-8 text-destructive/30 mb-2" />
              <p className="text-sm text-muted-foreground mb-1">{t("dataProducts.tabLoadFailed", { tab: label })}</p>
              <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
                <RotateCcw className="h-3 w-3" />
                {t("common.retry")}
              </Button>
            </div>
          )}
        >
          <Suspense fallback={<Skeleton className="h-48 max-w-5xl mt-4" />}>{children}</Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}

/** Simple centered placeholder for tabs whose real content lands in a later
 *  task (Tables: Task 8, Schedule: Task 9, Runs: Task 10). */
function ComingSoonTab() {
  const { t } = useTranslation();
  return (
    <div className="flex items-center justify-center py-16 max-w-5xl">
      <p className="text-sm text-muted-foreground">{t("dataProducts.tabPlaceholder")}</p>
    </div>
  );
}

function DataProductDetailPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const { productId } = useParams({ from: "/_sidebar/data-products/$productId" });
  const { tab } = useSearch({ from: "/_sidebar/data-products/$productId" });

  const { data: product } = useGetDataProductSuspense(productId, selector<DataProductOut>());

  // canEdit gating is client-side UX only (hides controls the backend would
  // 403 on anyway) — RULE_AUTHOR+ mirrors the route's real RBAC gate.
  const canEdit = perms.canEditRules;

  const editState = useEditProductState(product);
  const { blocker } = useUnsavedGuard({ hasUnsavedChanges: editState.isDirty });

  // The URL is the single source of truth for the active tab — no local
  // state. An absent/invalid ?tab= falls back to "about".
  const activeTab: ProductTabKey =
    tab && (PRODUCT_TAB_KEYS as string[]).includes(tab) ? (tab as ProductTabKey) : "about";
  const setActiveTab = (next: ProductTabKey) =>
    void navigate({
      to: "/data-products/$productId",
      params: { productId },
      search: (prev) => ({ ...prev, tab: next }),
    });

  return (
    <FadeIn>
      <div className="space-y-4">
        <PageBreadcrumb items={[{ label: t("dataProducts.title"), to: "/data-products" }]} page={product.name} />

        <ProductHeader product={product} canEdit={canEdit} editState={editState} />

        <ProductTabsShell activeTab={activeTab} onTabChange={setActiveTab}>
          {{
            about: (
              <TabBoundary label={t("dataProducts.tabAbout")}>
                <ProductAboutTab editState={editState} canEdit={canEdit} />
              </TabBoundary>
            ),
            sharing: (
              <TabBoundary label={t("dataProducts.tabSharing")}>
                <ProductSharingTab editState={editState} canEdit={canEdit} />
              </TabBoundary>
            ),
            tables: (
              <TabBoundary label={t("dataProducts.tabTables")}>
                <ComingSoonTab />
              </TabBoundary>
            ),
            runs: (
              <TabBoundary label={t("dataProducts.tabRuns")}>
                <ComingSoonTab />
              </TabBoundary>
            ),
            scheduling: (
              <TabBoundary label={t("dataProducts.tabSchedule")}>
                <ComingSoonTab />
              </TabBoundary>
            ),
          }}
        </ProductTabsShell>
      </div>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("common.unsavedChanges")}</AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.unsavedChangesDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("common.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-white hover:bg-destructive/90"
              onClick={() => blocker.proceed?.()}
            >
              {t("dataProducts.discardAndLeave")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
