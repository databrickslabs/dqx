import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { ProfileColumnCard } from "./ProfileColumnCard";

interface Props {
  columnStats: Record<string, Record<string, unknown>>;
  columnTypes: Record<string, string>;
  rowCount: number | null | undefined;
}

export function ProfileColumnList({ columnStats, columnTypes, rowCount }: Props) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  const [openColumn, setOpenColumn] = useState<string | null>(null);

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
        <div className="space-y-2">
          {visibleNames.map((name) => (
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
      )}
    </div>
  );
}
