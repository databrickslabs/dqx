import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo, useState, Suspense } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Pagination } from "@/components/Pagination";
import {
  DataProductsTable,
  getDataProductsSortValue,
  type DataProductsSortKey,
} from "@/components/data-products/DataProductsTable";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { AlertCircle, Boxes, Loader2, Plus, RotateCcw, Search } from "lucide-react";
import { useListDataProducts, type DataProductOut } from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";

const PAGE_SIZE = 50;
const ALL = "all";

export const Route = createFileRoute("/_sidebar/data-products/")({
  component: () => (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary onReset={reset} FallbackComponent={DataProductsError}>
          <Suspense fallback={<DataProductsSkeleton />}>
            <DataProductsPage />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  ),
});

function DataProductsError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
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

function DataProductsSkeleton() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-6 w-24" />
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-4 w-64" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

function DataProductsPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();

  // Refetch on remount (e.g. navigating back from the create/detail page)
  // so a newly created product appears without a manual page reload —
  // ported from dqlake's `DataProductsTable`, which needs the same
  // override for the same navigation pattern.
  const { data, isLoading, isError, refetch } = useListDataProducts({ query: { refetchOnMount: "always" } });
  const products = useMemo(() => data?.data ?? [], [data]);

  const [stewardFilter, setStewardFilter] = useState<string>(ALL);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  const stewardOptions = useMemo(
    () => Array.from(new Set(products.map((p) => p.steward).filter((s): s is string => !!s))).sort(),
    [products],
  );

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return products.filter((p) => {
      if (stewardFilter !== ALL && (p.steward ?? "") !== stewardFilter) return false;
      if (!q) return true;
      const hay = `${p.name} ${p.description ?? ""} ${p.steward ?? ""}`.toLowerCase();
      return hay.includes(q);
    });
  }, [products, stewardFilter, search]);

  const [sortKey, setSortKey] = useState<DataProductsSortKey | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const handleHeaderClick = useCallback(
    (key: DataProductsSortKey) => {
      if (sortKey !== key) {
        setSortKey(key);
        setSortDir("asc");
        return;
      }
      if (sortDir === "asc") {
        setSortDir("desc");
        return;
      }
      setSortKey(null);
    },
    [sortKey, sortDir],
  );

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = getDataProductsSortValue(sortKey, a);
      const bv = getDataProductsSortValue(sortKey, b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return copy;
  }, [filtered, sortKey, sortDir]);

  const paged = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sorted.slice(start, start + PAGE_SIZE);
  }, [sorted, page]);

  const hasActiveFilters = stewardFilter !== ALL || search.trim() !== "";

  const applyFilter = useCallback(
    <T,>(setter: (v: T) => void) =>
      (v: T) => {
        setter(v);
        setPage(1);
      },
    [],
  );

  const openProduct = (product: DataProductOut) => {
    navigate({ to: "/data-products/$productId", params: { productId: product.product_id } });
  };

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb page={t("dataProducts.title")} />

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{t("dataProducts.title")}</h1>
            <p className="text-sm text-muted-foreground mt-1">{t("dataProducts.subtitle")}</p>
          </div>
          {perms.canCreateRules && (
            <Button onClick={() => navigate({ to: "/data-products/new" })} className="gap-2">
              <Plus className="h-4 w-4" />
              {t("dataProducts.newProduct")}
            </Button>
          )}
        </div>

        <DataProductsTable
          rows={paged}
          sortKey={sortKey}
          sortDir={sortDir}
          onHeaderClick={handleHeaderClick}
          onRowClick={openProduct}
          toolbarExtra={
            <>
              <div className="relative w-56">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder={t("dataProducts.searchPlaceholder")}
                  value={search}
                  onChange={(e) => applyFilter(setSearch)(e.target.value)}
                  className="h-8 text-xs pl-7"
                />
              </div>
              {stewardOptions.length > 0 && (
                <Select value={stewardFilter} onValueChange={applyFilter(setStewardFilter)}>
                  <SelectTrigger className="h-8 w-40 text-xs" aria-label={t("dataProducts.colSteward")}>
                    <SelectValue placeholder={t("dataProducts.stewardPlaceholder")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL} className="text-xs">{t("dataProducts.allStewards")}</SelectItem>
                    {stewardOptions.map((s) => (
                      <SelectItem key={s} value={s} className="text-xs">{s}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </>
          }
          emptyState={
            isLoading ? (
              <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" />
                {t("common.loading")}
              </div>
            ) : isError ? (
              <div className="flex flex-col items-center justify-center text-center">
                <AlertCircle className="h-10 w-10 text-destructive/30 mb-3" />
                <p className="text-sm text-muted-foreground mb-3">{t("common.loadFailed")}</p>
                <Button variant="outline" size="sm" onClick={() => refetch()} className="gap-2">
                  <RotateCcw className="h-3 w-3" />
                  {t("common.retry")}
                </Button>
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center text-center">
                <Boxes className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">
                  {hasActiveFilters
                    ? t("dataProducts.emptyState")
                    : perms.canCreateRules
                      ? t("dataProducts.emptyStateNoProductsCta")
                      : t("dataProducts.emptyStateNoProducts")}
                </p>
              </div>
            )
          }
        />

        {filtered.length > 0 && (
          <Pagination page={page} totalItems={filtered.length} pageSize={PAGE_SIZE} onPageChange={setPage} />
        )}
      </div>
    </FadeIn>
  );
}
