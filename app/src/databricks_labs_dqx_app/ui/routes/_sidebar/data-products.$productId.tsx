import { createFileRoute, Link, useParams } from "@tanstack/react-router";
import { Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle, ArrowLeft, RotateCcw } from "lucide-react";
import { useGetDataProduct } from "@/lib/api";
import selector from "@/lib/selector";
import type { DataProductOut } from "@/lib/api";

// Minimal placeholder detail page — Task 7 replaces this with the full
// product shell (tabs, header actions, edit state). Kept just detailed
// enough that the list page's row-click has somewhere real to land: the
// product name plus a back link to the list.
export const Route = createFileRoute("/_sidebar/data-products/$productId")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} fallbackRender={DataProductDetailError}>
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
      <Skeleton className="h-32 w-full max-w-xl" />
    </div>
  );
}

function DataProductDetailPage() {
  const { t } = useTranslation();
  const { productId } = useParams({ from: "/_sidebar/data-products/$productId" });
  const { data: product } = useGetDataProduct(productId, selector<DataProductOut>());

  return (
    <FadeIn>
      <div className="space-y-6 max-w-xl">
        <PageBreadcrumb
          items={[{ label: t("dataProducts.title"), to: "/data-products" }]}
          page={product?.name ?? t("dataProducts.detailFallbackTitle")}
        />

        <div className="border-b pb-4 space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">
            {product?.name ?? t("dataProducts.detailFallbackTitle")}
          </h1>
          <Link
            to="/data-products"
            className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="h-4 w-4 mr-1" /> {t("dataProducts.backToList")}
          </Link>
        </div>

        <p className="text-sm text-muted-foreground">{t("dataProducts.detailPlaceholder")}</p>
      </div>
    </FadeIn>
  );
}
