// ThresholdPill — a reusable ⚠ <pct>% pill that opens a popover with a
// number input. Mirrors the SeverityDropdown badge-trigger pattern in
// RuleConfigCard.tsx so both controls look identical.
//
// Props:
//   value           — current per-rule/per-column override (null = no override)
//   effectiveDefault — resolved default to display when value is null
//   onChange         — called with the new value (number, always non-null)
//   readonly        — when true renders a non-interactive badge
//
// Per-column mode (mixed):
//   columns              — list of mapped columns + each column's current override
//   columnEffectiveDefault — the effective default to show as placeholder per column
//   onColumnChange       — called with (columnName, newValue) for per-column edits

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

export interface ThresholdPillColumnEntry {
  name: string;
  value: number | null;
}

export interface ThresholdPillProps {
  /** Explicit per-rule/per-column override, or null when following default. */
  value: number | null;
  /** Resolved default to display when value is null (rule ?? registry ?? admin). */
  effectiveDefault: number;
  onChange: (v: number | null) => void;
  readonly?: boolean;
  /** Override the popover hint text (defaults to the per-rule i18n string). */
  hintOverride?: string;
  /** When true, the pill label shows "Mixed" instead of a specific percentage,
   *  indicating that per-column overrides differ from the effective rule value.
   *  When columns + onColumnChange are provided, the popover renders per-column
   *  inputs instead of the single rule-level input. */
  mixed?: boolean;
  /** Per-column editor mode: list of mapped columns and their current override values.
   *  When provided alongside mixed=true, the popover shows one input per column. */
  columns?: ThresholdPillColumnEntry[];
  /** The effective default for each per-column input placeholder (the rule-level
   *  effective threshold: per-rule override ?? admin default). */
  columnEffectiveDefault?: number;
  /** Called when the user edits a single column's threshold in per-column mode. */
  onColumnChange?: (column: string, value: number | null) => void;
}

export function ThresholdPill({
  value,
  effectiveDefault,
  onChange,
  readonly = false,
  hintOverride,
  mixed = false,
  columns,
  columnEffectiveDefault,
  onColumnChange,
}: ThresholdPillProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  // Local draft string for the single-value mode — committed on blur/Enter.
  const [draft, setDraft] = useState<string>("");
  // Per-column draft map for mixed mode — keyed by column name.
  const [columnDrafts, setColumnDrafts] = useState<Record<string, string>>({});

  const displayed = value ?? effectiveDefault;
  // Only a value that actually DIFFERS from the effective default counts as an
  // override for the "*" marker: an explicit value equal to the default reads
  // as "following the default", so flagging it with "*" is misleading (the pill
  // then shows e.g. "< 70% *" identical to the un-flagged default).
  const isOverridden = value !== null && value !== effectiveDefault;

  // Per-column mode: mixed + columns provided + onColumnChange wired
  const isPerColumnMode = mixed && columns !== undefined && columns.length > 0 && onColumnChange !== undefined;

  // Screen readers hear "Mixed …" when per-column overrides diverge, matching
  // the visible label instead of announcing a single rule-level percentage.
  const ariaLabel = mixed
    ? t("monitoredTables.thresholdPillMixedAria")
    : t("monitoredTables.thresholdPillAria", { pct: displayed });

  // The label span carries a fixed min-width sized to the widest label
  // ("< 100%" / "Mixed") so the pill stays the SAME width whether it shows a
  // percentage or "Mixed" (no shrink on state change), and the text is
  // left-aligned so a row of pills lines up on the left edge.
  const badgeContent = (
    <>
      <AlertTriangle className="h-3 w-3 text-amber-500 shrink-0" aria-hidden />
      <span className="min-w-[3rem] text-left">
        {mixed
          ? t("monitoredTables.thresholdPillMixed")
          : t("monitoredTables.thresholdPillLabel", { pct: displayed })}
      </span>
      {/* Fixed-width "*" slot (always present, empty when not overridden) so the
          pill width doesn't jump when the marker appears/disappears. */}
      <span className="w-2 text-center text-muted-foreground">{isOverridden || mixed ? "*" : ""}</span>
    </>
  );

  if (readonly) {
    return (
      <Badge
        variant="outline"
        className={cn("text-[10px] gap-1 px-1.5 py-0.5 shrink-0", !isOverridden && "text-muted-foreground")}
        aria-label={ariaLabel}
      >
        {badgeContent}
      </Badge>
    );
  }

  const commitDraft = (raw: string) => {
    const trimmed = raw.trim();
    const n = Number.parseInt(trimmed, 10);
    if (!Number.isNaN(n)) {
      onChange(Math.max(0, Math.min(100, n)));
    }
    // empty or non-numeric → no-op, retain current value
  };

  const commitColumnDraft = (column: string, raw: string) => {
    const trimmed = raw.trim();
    const n = Number.parseInt(trimmed, 10);
    if (!Number.isNaN(n)) {
      onColumnChange!(column, Math.max(0, Math.min(100, n)));
    }
    // empty or non-numeric → no-op, retain current column value
  };

  const colDefault = columnEffectiveDefault ?? effectiveDefault;

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) {
          if (isPerColumnMode) {
            // Seed per-column drafts from current column values when opening
            const drafts: Record<string, string> = {};
            for (const col of columns!) {
              drafts[col.name] = String(col.value ?? colDefault);
            }
            setColumnDrafts(drafts);
          } else {
            // Seed the draft from the current value when opening
            setDraft(String(value ?? effectiveDefault));
          }
        }
        setOpen(nextOpen);
      }}
    >
      <PopoverTrigger asChild>
        <button
          type="button"
          onClick={(e) => e.stopPropagation()}
          aria-label={ariaLabel}
          className="focus:outline-none"
        >
          <Badge
            variant="outline"
            className={cn(
              "text-[10px] gap-1 px-1.5 py-0.5 shrink-0 cursor-pointer hover:bg-muted/60",
              !isOverridden && "text-muted-foreground",
            )}
          >
            {badgeContent}
            {" "}&#x25BE;
          </Badge>
        </button>
      </PopoverTrigger>
      {isPerColumnMode ? (
        <PopoverContent
          className="w-64 p-3 space-y-3"
          align="end"
          onClick={(e) => e.stopPropagation()}
          // Don't auto-focus (and thus select-all) the first column's input on
          // open — the popover should read as a calm list, not land mid-edit
          // with a highlighted value.
          onOpenAutoFocus={(e) => e.preventDefault()}
        >
          <p className="text-xs font-medium">{t("monitoredTables.thresholdMixedPopoverTitle")}</p>
          <div className="space-y-2">
            {columns!.map((col) => (
              <div key={col.name} className="flex items-center gap-2">
                <span className="text-[11px] text-muted-foreground flex-1 min-w-0 truncate" title={col.name}>
                  {col.name}
                </span>
                <div className="relative w-20 shrink-0">
                  <Input
                    type="number"
                    min={0}
                    max={100}
                    step={1}
                    value={columnDrafts[col.name] ?? String(col.value ?? colDefault)}
                    placeholder={String(colDefault)}
                    className="h-7 text-xs w-20 pr-6"
                    onChange={(e) => setColumnDrafts((prev) => ({ ...prev, [col.name]: e.target.value }))}
                    onBlur={(e) => {
                      commitColumnDraft(col.name, e.target.value);
                      setColumnDrafts((prev) => ({ ...prev, [col.name]: String(col.value ?? colDefault) }));
                    }}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        commitColumnDraft(col.name, columnDrafts[col.name] ?? "");
                        setOpen(false);
                      }
                      if (e.key === "Escape") {
                        setOpen(false);
                      }
                    }}
                  />
                  <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">
                    %
                  </span>
                </div>
              </div>
            ))}
          </div>
        </PopoverContent>
      ) : (
        <PopoverContent
          className="w-56 p-3 space-y-3"
          align="end"
          onClick={(e) => e.stopPropagation()}
        >
          <p className="text-xs font-medium">{t("monitoredTables.thresholdPopoverTitle")}</p>
          <p className="text-[11px] text-muted-foreground">
            {hintOverride ?? t("monitoredTables.thresholdPopoverHint", { pct: effectiveDefault })}
          </p>
          <div className="relative">
            <Input
              type="number"
              min={0}
              max={100}
              step={1}
              value={draft}
              placeholder={String(effectiveDefault)}
              className="h-8 text-xs pr-6"
              onChange={(e) => setDraft(e.target.value)}
              onBlur={(e) => {
                commitDraft(e.target.value);
                setDraft(String(value ?? effectiveDefault));
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  commitDraft(draft);
                  setOpen(false);
                }
                if (e.key === "Escape") {
                  setOpen(false);
                }
              }}
            />
            <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">
              %
            </span>
          </div>
        </PopoverContent>
      )}
    </Popover>
  );
}
