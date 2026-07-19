// ThresholdPill — a reusable ⚠ <pct>% pill that opens a popover with a
// number input. Mirrors the SeverityDropdown badge-trigger pattern in
// RuleConfigCard.tsx so both controls look identical.
//
// Props:
//   value           — current per-rule/per-column override (null = no override)
//   effectiveDefault — resolved default to display when value is null
//   onChange         — called with the new value (number, always non-null)
//   readonly        — when true renders a non-interactive badge

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

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
   *  The popover still edits the rule-level value normally. */
  mixed?: boolean;
}

export function ThresholdPill({ value, effectiveDefault, onChange, readonly = false, hintOverride, mixed = false }: ThresholdPillProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  // Local draft string so the input stays editable without forcing an int on
  // every keystroke — committed on blur/Enter.
  const [draft, setDraft] = useState<string>("");

  const displayed = value ?? effectiveDefault;
  const isOverridden = value !== null;
  // Screen readers hear "Mixed …" when per-column overrides diverge, matching
  // the visible label instead of announcing a single rule-level percentage.
  const ariaLabel = mixed
    ? t("monitoredTables.thresholdPillMixedAria")
    : t("monitoredTables.thresholdPillAria", { pct: displayed });

  const badgeContent = (
    <>
      <AlertTriangle className="h-3 w-3 text-amber-500 shrink-0" aria-hidden />
      {mixed ? (
        <span>{t("monitoredTables.thresholdPillMixed")}</span>
      ) : (
        <span>{t("monitoredTables.thresholdPillLabel", { pct: displayed })}</span>
      )}
      {(isOverridden || mixed) && <span className="text-muted-foreground ml-0.5">*</span>}
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

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) {
          // Seed the draft from the current value when opening
          setDraft(String(value ?? effectiveDefault));
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
      <PopoverContent
        className="w-56 p-3 space-y-3"
        align="end"
        onClick={(e) => e.stopPropagation()}
      >
        <p className="text-xs font-medium">{t("monitoredTables.thresholdPopoverTitle")}</p>
        <p className="text-[11px] text-muted-foreground">
          {hintOverride ?? t("monitoredTables.thresholdPopoverHint", { pct: effectiveDefault })}
        </p>
        <Input
          type="number"
          min={0}
          max={100}
          step={1}
          value={draft}
          placeholder={String(effectiveDefault)}
          className="h-8 text-xs"
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
      </PopoverContent>
    </Popover>
  );
}
