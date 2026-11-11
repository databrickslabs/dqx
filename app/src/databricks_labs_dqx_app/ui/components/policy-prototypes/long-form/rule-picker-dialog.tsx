// THROWAWAY prototype — mock lookalike of apply-rules/AddRulesDialog + RulesPicker.
import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { RULES } from "./model";
import { FamilyBadge, StatusBadge } from "./primitives";

export function RulePickerDialog({
  open,
  onOpenChange,
  selectedRuleIds,
  excludeRuleId,
  onAdd,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedRuleIds: string[];
  /** The policy's own rule; not offered. */
  excludeRuleId: string;
  onAdd: (ruleIds: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const already = useMemo(() => new Set(selectedRuleIds), [selectedRuleIds]);

  const rows = RULES.filter((r) => r.rule_id !== excludeRuleId).filter((r) => {
    const q = search.trim().toLowerCase();
    return !q || r.name.toLowerCase().includes(q) || r.dimension.includes(q) || r.rule_id.includes(q);
  });

  const close = (next: boolean) => {
    if (!next) {
      setPicked(new Set());
      setSearch("");
    }
    onOpenChange(next);
  };

  const toggle = (id: string) => {
    if (already.has(id)) return;
    setPicked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="w-[min(92vw,900px)] !max-w-[min(92vw,900px)]">
        <DialogHeader>
          <DialogTitle>Also create for other rules</DialogTitle>
          <DialogDescription>
            Each selected rule gets its own copy of this policy.
          </DialogDescription>
        </DialogHeader>

        <div className="min-w-0 space-y-3 px-2">
          <div className="relative max-w-xs">
            <Search className="absolute top-1/2 left-2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search rules"
              className="h-8 pl-7 text-xs"
              autoFocus
            />
          </div>
          <div className="max-h-[26rem] min-h-[20rem] overflow-y-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-9 px-2" />
                  <TableHead className="px-2">Name</TableHead>
                  <TableHead className="px-2">Slots</TableHead>
                  <TableHead className="px-2">Dimension</TableHead>
                  <TableHead className="px-2">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="py-10 text-center text-sm text-muted-foreground">
                      No rules match "{search}"
                    </TableCell>
                  </TableRow>
                )}
                {rows.map((r) => {
                  const locked = already.has(r.rule_id);
                  const checked = locked || picked.has(r.rule_id);
                  return (
                    <TableRow
                      key={r.rule_id}
                      onClick={() => toggle(r.rule_id)}
                      data-state={checked ? "selected" : undefined}
                      className={cn(locked ? "cursor-default opacity-70" : "cursor-pointer")}
                    >
                      <TableCell className="px-2">
                        <Checkbox
                          checked={checked}
                          disabled={locked}
                          onCheckedChange={() => toggle(r.rule_id)}
                          onClick={(e) => e.stopPropagation()}
                          aria-label={`Select ${r.name}`}
                        />
                      </TableCell>
                      <TableCell className="max-w-[22rem] px-2">
                        <div className="flex items-center gap-2">
                          <span className="truncate text-sm font-medium">{r.name}</span>
                          {locked && (
                            <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                              Already added
                            </span>
                          )}
                        </div>
                        <div className="truncate text-xs text-muted-foreground">{r.description}</div>
                      </TableCell>
                      <TableCell className="px-2">
                        {r.slots.length === 0 ? (
                          <span className="text-xs text-muted-foreground">Table-level</span>
                        ) : (
                          <div className="flex flex-wrap gap-1">
                            {r.slots.map((s) => (
                              <FamilyBadge key={s.name} family={s.family} />
                            ))}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="px-2 text-xs capitalize text-muted-foreground">{r.dimension}</TableCell>
                      <TableCell className="px-2">
                        <div className="flex flex-col items-start gap-1">
                          <StatusBadge status={r.status} />
                          {r.status !== "approved" && (
                            <span className="text-[10px] text-muted-foreground">Applies once approved</span>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => close(false)}>
            Cancel
          </Button>
          <Button
            disabled={picked.size === 0}
            onClick={() => {
              onAdd([...picked]);
              close(false);
            }}
          >
            {picked.size === 0 ? "Add rules" : `Add ${picked.size} ${picked.size === 1 ? "rule" : "rules"}`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
