import { ChevronLeft, ChevronRight } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";

interface PaginationProps {
  /** 1-indexed current page. */
  page: number;
  totalItems: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  className?: string;
}

/**
 * Client-side pagination footer — shows an item range, page-number buttons
 * (with ellipsis for large page counts), and prev/next controls. Shared by
 * any list page that paginates an already-fetched array (Rules Registry,
 * and Monitored Tables in a future phase).
 */
export function Pagination({ page, totalItems, pageSize, onPageChange, className }: PaginationProps) {
  const { t } = useTranslation();
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  if (totalPages <= 1) return null;

  const rangeStart = (page - 1) * pageSize + 1;
  const rangeEnd = Math.min(page * pageSize, totalItems);
  const pageNumbers = Array.from({ length: totalPages }, (_, i) => i + 1)
    .filter((p) => p === 1 || p === totalPages || Math.abs(p - page) <= 2)
    .reduce<(number | "ellipsis")[]>((acc, p, i, arr) => {
      if (i > 0 && p - arr[i - 1] > 1) acc.push("ellipsis");
      acc.push(p);
      return acc;
    }, []);

  return (
    <div className={className ?? "flex items-center justify-between px-4 py-3"}>
      <p className="text-xs text-muted-foreground">
        {t("common.showing")} {rangeStart}–{rangeEnd} {t("common.of")} {totalItems}
      </p>
      <div className="flex items-center gap-1">
        <Button
          variant="outline"
          size="sm"
          className="h-8 w-8 p-0"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        {pageNumbers.map((p, i) =>
          p === "ellipsis" ? (
            <span key={`ellipsis-${i}`} className="px-1 text-xs text-muted-foreground">
              …
            </span>
          ) : (
            <Button
              key={p}
              variant={p === page ? "default" : "outline"}
              size="sm"
              className="h-8 w-8 p-0 text-xs"
              onClick={() => onPageChange(p)}
            >
              {p}
            </Button>
          ),
        )}
        <Button
          variant="outline"
          size="sm"
          className="h-8 w-8 p-0"
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
