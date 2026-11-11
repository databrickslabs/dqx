// THROWAWAY prototype — lookalike of apply-rules/AddRulesDialog + RulesPicker
// (search + multiselect table) fed by MOCK_RULES. Rules already in the policy
// render checked + disabled, like already-applied rules in the real picker.
import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { MOCK_RULES, type MockRule } from "../mock";
import { FamilyBadge, Pill } from "./shared";

function StatusPill({ status }: { status: MockRule["status"] }) {
  if (status === "approved") return <Pill tone="emerald">Approved</Pill>;
  if (status === "pending_approval") return <Pill tone="amber">Pending approval</Pill>;
  return <Pill tone="muted">Draft</Pill>;
}

export function RulePickerDialog({
  open,
  onOpenChange,
  existing,
  onAdd,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  existing: string[];
  onAdd: (ids: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const inPolicy = useMemo(() => new Set(existing), [existing]);

  const rows = useMemo(() => {
    const q = search.trim().toLowerCase();
    return q ? MOCK_RULES.filter((r) => r.name.toLowerCase().includes(q) || r.dimension.includes(q)) : MOCK_RULES;
  }, [search]);

  const close = (o: boolean) => {
    if (!o) {
      setSelected(new Set());
      setSearch("");
    }
    onOpenChange(o);
  };
  const toggle = (id: string) =>
    setSelected((s) => {
      const n = new Set(s);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  const pickedDrafts = MOCK_RULES.filter((r) => selected.has(r.rule_id) && r.status !== "approved").length;

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="w-[min(92vw,860px)] !max-w-[min(92vw,860px)]">
        <DialogHeader>
          <DialogTitle>Add rules to policy</DialogTitle>
          <DialogDescription>Pick one or more rules. Only approved rules are attached; drafts apply once approved.</DialogDescription>
        </DialogHeader>
        <div className="min-w-0 space-y-3 px-2">
          <div className="relative max-w-xs">
            <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search rules" className="h-8 pl-7 text-xs" />
          </div>
          <div className="max-h-[26rem] min-h-[16rem] overflow-y-auto rounded-md border">
            <Table className="table-fixed">
              <colgroup>
                <col style={{ width: 36 }} />
                <col style={{ width: 130 }} />
                <col style={{ width: 210 }} />
                <col />
                <col style={{ width: 120 }} />
                <col style={{ width: 150 }} />
              </colgroup>
              <TableHeader>
                <TableRow>
                  <TableHead className="px-2" />
                  <TableHead className="px-2">Slots</TableHead>
                  <TableHead className="px-2">Name</TableHead>
                  <TableHead className="px-2">Description</TableHead>
                  <TableHead className="px-2">Dimension</TableHead>
                  <TableHead className="px-2">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((r) => {
                  const locked = inPolicy.has(r.rule_id);
                  const checked = locked || selected.has(r.rule_id);
                  return (
                    <TableRow
                      key={r.rule_id}
                      data-state={checked ? "selected" : undefined}
                      onClick={locked ? undefined : () => toggle(r.rule_id)}
                      className={cn(locked ? "cursor-not-allowed opacity-60" : "cursor-pointer")}
                      aria-disabled={locked || undefined}
                    >
                      <TableCell className="px-2" onClick={(e) => e.stopPropagation()}>
                        <Checkbox checked={checked} disabled={locked} onCheckedChange={() => toggle(r.rule_id)} aria-label={`Select ${r.name}`} />
                      </TableCell>
                      <TableCell className="p-2">
                        {r.slots.length === 0 ? (
                          <span className="text-muted-foreground">—</span>
                        ) : (
                          <div className="flex flex-wrap gap-1">
                            {r.slots.map((s) => (
                              <FamilyBadge key={s.name} family={s.family} />
                            ))}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="truncate p-2 text-sm font-medium">{r.name}</TableCell>
                      <TableCell className="truncate p-2 text-muted-foreground">{r.description}</TableCell>
                      <TableCell className="p-2 text-xs capitalize text-muted-foreground">{r.dimension}</TableCell>
                      <TableCell className="p-2">
                        <div className="flex flex-col items-start gap-0.5">
                          <StatusPill status={r.status} />
                          {r.status !== "approved" && <span className="text-[10px] text-muted-foreground">Applies once approved</span>}
                          {locked && <span className="text-[10px] text-muted-foreground">In this policy</span>}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
                {rows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="py-8 text-center text-muted-foreground">
                      No rules match.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
          {pickedDrafts > 0 && (
            <p className="text-xs text-muted-foreground">
              {pickedDrafts} selected rule{pickedDrafts === 1 ? " isn't" : "s aren't"} approved yet. The policy will attach{" "}
              {pickedDrafts === 1 ? "it" : "them"} once approved.
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => close(false)}>
            Cancel
          </Button>
          <Button
            disabled={selected.size === 0}
            onClick={() => {
              onAdd([...selected]);
              close(false);
            }}
          >
            Add {selected.size || ""} rule{selected.size === 1 ? "" : "s"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
