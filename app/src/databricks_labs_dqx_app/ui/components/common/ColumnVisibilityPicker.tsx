import { useTranslation } from "react-i18next";
import { Columns3 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

/** A "Columns (shown/total)" menu that hides or shows a table's columns. The caller owns the hidden set. */
export function ColumnVisibilityPicker({
  columns,
  hiddenColumns,
  onHiddenColumnsChange,
  label = (column) => column,
}: {
  columns: string[];
  hiddenColumns: ReadonlySet<string>;
  onHiddenColumnsChange: (hidden: Set<string>) => void;
  label?: (column: string) => string;
}) {
  const { t } = useTranslation();
  if (columns.length === 0) return null;
  const shown = columns.filter((c) => !hiddenColumns.has(c)).length;
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-7 gap-1.5 select-none">
          <Columns3 className="h-3.5 w-3.5" aria-hidden="true" />
          {t("resultsUi.columnsPicker", { shown, total: columns.length })}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="max-h-80 overflow-y-auto">
        <DropdownMenuItem
          onSelect={(e) => {
            e.preventDefault();
            onHiddenColumnsChange(new Set());
          }}
        >
          {t("resultsUi.columnsShowAll")}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        {columns.map((col) => (
          <DropdownMenuCheckboxItem
            key={col}
            checked={!hiddenColumns.has(col)}
            onSelect={(e) => e.preventDefault()}
            onCheckedChange={(checked) => {
              const next = new Set(hiddenColumns);
              if (checked) next.delete(col);
              else next.add(col);
              onHiddenColumnsChange(next);
            }}
            className="font-mono text-xs"
          >
            {label(col)}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
