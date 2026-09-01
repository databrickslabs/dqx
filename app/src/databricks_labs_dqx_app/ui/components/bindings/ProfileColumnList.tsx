import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Pagination } from "@/components/Pagination";
import { ProfileColumnCard } from "./ProfileColumnCard";

interface Props {
  columnStats: Record<string, Record<string, unknown>>;
  columnTypes: Record<string, string>;
  rowCount: number | null | undefined;
}

// Match the About-tab schema table's page size so the two column lists on the
// same detail page feel consistent.
const PROFILE_PAGE_SIZE = 6;

export function ProfileColumnList({ columnStats, columnTypes, rowCount }: Props) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  const [openColumn, setOpenColumn] = useState<string | null>(null);
  const [page, setPage] = useState(1);

  // Click outside any card collapses the open one.
  useEffect(() => {
    if (openColumn == null) return;
    const onMouseDown = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (!target.closest("[data-profile-card]")) {
        setOpenColumn(null);
      }
    };
    document.addEventListener("mousedown", onMouseDown);
    return () => document.removeEventListener("mousedown", onMouseDown);
  }, [openColumn]);

  const allNames = useMemo(() => Object.keys(columnStats).sort(), [columnStats]);
  const visibleNames = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return allNames;
    return allNames.filter((n) => n.toLowerCase().includes(q));
  }, [allNames, filter]);

  // Reset to the first page whenever the filter or the underlying data set
  // changes (e.g. the version switcher selects a different run) so the current
  // page can never point past the end of the list.
  useEffect(() => {
    setPage(1);
  }, [filter, allNames]);

  const pagedNames = useMemo(
    () => visibleNames.slice((page - 1) * PROFILE_PAGE_SIZE, page * PROFILE_PAGE_SIZE),
    [visibleNames, page],
  );

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-3">
        <Input
          placeholder={t("monitoredTables.profileFilterPlaceholder")}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="max-w-xs h-8 text-xs"
        />
        <span className="text-[11px] text-muted-foreground">
          {visibleNames.length === allNames.length
            ? t("monitoredTables.profileColumnsCount", { count: allNames.length })
            : t("monitoredTables.profileColumnsCountOf", { visible: visibleNames.length, total: allNames.length })}
        </span>
      </div>

      {visibleNames.length === 0 ? (
        <div className="py-8 text-center text-xs text-muted-foreground border border-dashed rounded-md">
          {t("monitoredTables.profileNoColumnsMatch")}
        </div>
      ) : (
        <>
          <div className="space-y-2">
            {pagedNames.map((name) => (
              <ProfileColumnCard
                key={name}
                name={name}
                sparkType={columnTypes[name]}
                stats={columnStats[name] ?? {}}
                rowCount={rowCount}
                open={openColumn === name}
                onToggle={() => setOpenColumn((c) => (c === name ? null : name))}
              />
            ))}
          </div>
          <Pagination
            page={page}
            totalItems={visibleNames.length}
            pageSize={PROFILE_PAGE_SIZE}
            onPageChange={setPage}
            className="flex items-center justify-between pt-2"
          />
        </>
      )}
    </div>
  );
}
