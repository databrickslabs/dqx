// ThresholdPill — a reusable ⚠ <pct>% pill that opens a popover with a
// number input + "Use default" button. Mirrors the SeverityDropdown badge-
// trigger pattern in RuleConfigCard.tsx so both controls look identical.
//
// Props:
//   value           — current per-rule/per-column override (null = no override)
//   effectiveDefault — resolved default to show as placeholder when value is null
//   onChange         — called with the new value (null = clear override)
//   readonly        — when true renders a non-interactive badge

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
  /** Override the "reset to default" button label (defaults to per-rule label). */
  resetLabelOverride?: string;
}

export function ThresholdPill({ value, effectiveDefault, onChange, readonly = false, hintOverride, resetLabelOverride }: ThresholdPillProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  // Local draft string so the input stays editable without forcing an int on
  // every keystroke — committed on blur/Enter.
  const [draft, setDraft] = useState<string>("");

  const displayed = value ?? effectiveDefault;
  const isOverridden = value !== null;

  const badgeContent = (
    <>
      <AlertTriangle className="h-3 w-3 text-amber-500 shrink-0" aria-hidden />
      <span>{t("monitoredTables.thresholdPillLabel", { pct: displayed })}</span>
      {isOverridden && <span className="text-muted-foreground ml-0.5">*</span>}
    </>
  );

  if (readonly) {
    return (
      <Badge
        variant="outline"
        className={cn("text-[10px] gap-1 px-1.5 py-0.5 shrink-0", !isOverridden && "text-muted-foreground")}
        aria-label={t("monitoredTables.thresholdPillAria", { pct: displayed })}
      >
        {badgeContent}
      </Badge>
    );
  }

  const commitDraft = (raw: string) => {
    const trimmed = raw.trim();
    if (trimmed === "") {
      onChange(null);
      return;
    }
    const n = Number.parseInt(trimmed, 10);
    if (!Number.isNaN(n)) {
      onChange(Math.max(0, Math.min(100, n)));
    }
  };

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) {
          // Seed the draft from the current value when opening
          setDraft(value !== null ? String(value) : "");
        }
        setOpen(nextOpen);
      }}
    >
      <PopoverTrigger asChild>
        <button
          type="button"
          onClick={(e) => e.stopPropagation()}
          aria-label={t("monitoredTables.thresholdPillAria", { pct: displayed })}
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
            setDraft(value !== null ? String(value) : "");
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
        <Button
          variant="outline"
          size="sm"
          className="w-full text-xs"
          onClick={() => {
            onChange(null);
            setOpen(false);
          }}
        >
          {resetLabelOverride ?? t("monitoredTables.thresholdResetToDefault")}
        </Button>
      </PopoverContent>
    </Popover>
  );
}
