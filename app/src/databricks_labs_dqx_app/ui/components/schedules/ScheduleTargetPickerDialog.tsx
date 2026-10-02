import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { CalendarClock, Check, Loader2, Search } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { matchesTargetSearch, type ScheduleTarget } from "@/components/schedules/schedule-target";
import { cronHint } from "@/lib/cron";
import { cn } from "@/lib/utils";

interface ScheduleTargetPickerDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: string;
  searchPlaceholder: string;
  /** Label of the Name column ("Table" / "Collection"). */
  nameLabel: string;
  emptyText: string;
  targets: ScheduleTarget[];
  isLoading: boolean;
  /** Currently chosen target id, marked with a check. */
  selectedId: string | null;
  onSelect: (target: ScheduleTarget) => void;
}

/**
 * Compact single-select list of tables or collections for the New schedule
 * page — a mini version of the Tables / Collections overviews (search box +
 * the same table markup). Clicking a row picks it and closes the dialog.
 * Targets that already have a schedule show it, so it's clear saving
 * replaces that schedule.
 */
export function ScheduleTargetPickerDialog({
  open,
  onOpenChange,
  title,
  description,
  searchPlaceholder,
  nameLabel,
  emptyText,
  targets,
  isLoading,
  selectedId,
  onSelect,
}: ScheduleTargetPickerDialogProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const filtered = useMemo(() => targets.filter((target) => matchesTargetSearch(target, search)), [targets, search]);

  const handleOpenChange = (next: boolean) => {
    if (!next) setSearch("");
    onOpenChange(next);
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="flex max-h-[80vh] flex-col sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>

        <div className="relative w-56">
          <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder={searchPlaceholder}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-7 text-xs"
            autoFocus
          />
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto rounded-md border">
          <Table className="table-fixed">
            <colgroup>
              <col />
              <col style={{ width: 180 }} />
              <col style={{ width: 220 }} />
            </colgroup>
            <TableHeader>
              <TableRow className="bg-muted/50 hover:bg-muted/50">
                <TableHead className="px-2 text-xs font-medium">{nameLabel}</TableHead>
                <TableHead className="px-2 text-xs font-medium">{t("schedules.owner")}</TableHead>
                <TableHead className="px-2 text-xs font-medium">{t("schedules.currentSchedule")}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((target) => {
                const selected = target.id === selectedId;
                const hint = target.existing ? cronHint(target.existing.cron, target.existing.timezone, t) : null;
                return (
                  <TableRow
                    key={target.id}
                    className={cn("cursor-pointer", selected && "bg-muted/50")}
                    onClick={() => {
                      onSelect(target);
                      handleOpenChange(false);
                    }}
                    aria-selected={selected}
                  >
                    <TableCell className="overflow-hidden p-2 align-middle">
                      <div className="flex min-w-0 items-center gap-2">
                        <Check className={cn("h-3.5 w-3.5 shrink-0", selected ? "opacity-100" : "opacity-0")} />
                        <div className="min-w-0">
                          <span className="block truncate font-medium" title={target.name}>
                            {target.name}
                          </span>
                          <span className="block truncate text-xs text-muted-foreground" title={target.detail}>
                            {target.detail}
                          </span>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell className="overflow-hidden p-2 align-middle">
                      {target.owner ? (
                        <span className="block truncate" title={target.owner}>
                          {target.owner}
                        </span>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell className="overflow-hidden p-2 align-middle">
                      {hint ? (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="flex min-w-0 items-center gap-1.5 text-xs text-amber-700 dark:text-amber-400">
                              <CalendarClock className="h-3.5 w-3.5 shrink-0" />
                              <span className="truncate">{hint}</span>
                            </span>
                          </TooltipTrigger>
                          <TooltipContent>{t("schedules.pickerReplacesHint")}</TooltipContent>
                        </Tooltip>
                      ) : (
                        <span className="text-xs text-muted-foreground">{t("schedules.onDemandOnly")}</span>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {isLoading ? (
            <div className="flex items-center justify-center gap-2 py-10 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              {t("common.loading")}
            </div>
          ) : (
            filtered.length === 0 && <p className="py-10 text-center text-sm text-muted-foreground">{emptyText}</p>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
