import { ChevronDown } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { TableCell, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

export interface GroupHeaderRowProps {
  label: string;
  count: number;
  expanded: boolean;
  onToggle: () => void;
  /** Number of table columns the header spans. */
  colSpan: number;
}

/**
 * Collapsible group header for a grouped overview table (Group by): chevron,
 * group label and row count, spanning the whole row. Shared by the Rules and
 * Tables overviews so grouped views look the same.
 */
export function GroupHeaderRow({ label, count, expanded, onToggle, colSpan }: GroupHeaderRowProps) {
  return (
    <TableRow className="hover:bg-muted/30">
      <TableCell colSpan={colSpan} className="bg-muted/30 p-0 text-xs font-semibold text-foreground">
        <button
          type="button"
          className="flex w-full items-center gap-2 px-3 py-2.5 text-left transition-colors hover:bg-muted"
          aria-expanded={expanded}
          onClick={onToggle}
        >
          <ChevronDown
            className={cn("h-4 w-4 shrink-0 text-muted-foreground transition-transform", !expanded && "-rotate-90")}
            aria-hidden
          />
          <span>{label}</span>
          <Badge variant="secondary" className="min-w-6 justify-center text-[10px]">
            {count}
          </Badge>
        </button>
      </TableCell>
    </TableRow>
  );
}

/** Adds *key* to, or removes it from, a set of expanded group keys. */
export function toggleGroupKey(prev: ReadonlySet<string>, key: string): Set<string> {
  const next = new Set(prev);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

/** Row count per group key, for group header badges. */
export function countByGroup(groups: readonly ({ key: string } | undefined)[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const group of groups) {
    if (group) counts.set(group.key, (counts.get(group.key) ?? 0) + 1);
  }
  return counts;
}
