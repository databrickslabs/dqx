import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Check,
  ChevronDown,
  ChevronRight,
  Plus,
  Tag,
  Tags,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { formatLabel, labelToken, tokenToLabel } from "@/lib/format-utils";
import type { LabelDefinition } from "@/lib/api-custom";

// ─────────────────────────────────────────────────────────────────────────────
// LabelsEditor — author-side key/value editor
//
// Two operating modes:
//   1. Constrained — when ``definitions`` is provided & non-empty. The
//      "+ Add label" popover lets users search admin-curated keys or type a
//      free-form custom key (mirroring the value popover's search + custom
//      entry pattern), then pick/type a value the same way.
//   2. Free-form — fallback two-text-input UI when no definitions exist.
//
// Boolean labels (a definition with no values) commit ``"true"`` on save so
// the resulting map stays string→string.
// ─────────────────────────────────────────────────────────────────────────────

interface LabelsEditorProps {
  value: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
  disabled?: boolean;
  /** Default: "Labels (optional)". Set to a custom string to override. */
  title?: string;
  /** Start with the editor expanded, otherwise it's collapsed by default. */
  defaultOpen?: boolean;
  /** Layout variant. ``inline`` is denser (used inside check cards). */
  variant?: "inline" | "block";
  /** Hide the disclosure header — render rows directly. */
  showHeader?: boolean;
  /** Admin-managed catalog. Triggers constrained mode when non-empty. */
  definitions?: LabelDefinition[];
}

interface Row {
  id: string;
  key: string;
  value: string;
}

function recordToRows(rec: Record<string, string>): Row[] {
  return Object.entries(rec).map(([key, value]) => ({
    id: crypto.randomUUID(),
    key,
    value,
  }));
}

function rowsToRecord(rows: Row[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const r of rows) {
    const key = r.key.trim();
    const value = r.value.trim();
    if (!key) continue;
    out[key] = value || "true";
  }
  return out;
}

export function LabelsEditor({
  value,
  onChange,
  disabled,
  title,
  defaultOpen = false,
  variant = "inline",
  showHeader = true,
  definitions,
}: LabelsEditorProps) {
  const { t } = useTranslation();
  const editorTitle = title ?? t("labelsEditor.title");
  const [open, setOpen] = useState(defaultOpen);
  const [rows, setRows] = useState<Row[]>(() => recordToRows(value));

  // `defaultOpen` is often computed from data that loads asynchronously
  // (e.g. an existing rule's tags, hydrated after this component's first
  // render). `useState(defaultOpen)` only seeds the initial value, so if
  // the caller re-renders with `defaultOpen` flipping from false to true
  // once the data arrives, the disclosure would otherwise stay collapsed
  // forever even though there's now content to show. Auto-expand the one
  // time that happens; never force it back closed once the user has
  // interacted with it.
  useEffect(() => {
    if (defaultOpen) setOpen(true);
  }, [defaultOpen]);

  const definitionsMap = useMemo(() => {
    const m = new Map<string, LabelDefinition>();
    for (const d of definitions ?? []) m.set(d.key, d);
    return m;
  }, [definitions]);
  const constrained = definitionsMap.size > 0;

  const commit = (next: Row[]) => {
    setRows(next);
    onChange(rowsToRecord(next));
  };

  const addCustomRow = () =>
    commit([...rows, { id: crypto.randomUUID(), key: "", value: "" }]);

  const addDefinedRow = (key: string) => {
    const def = definitionsMap.get(key);
    // Boolean tag: prefill "true". Otherwise leave blank so the value picker
    // immediately opens.
    const initialValue = def && def.values.length === 0 ? "true" : "";
    commit([...rows, { id: crypto.randomUUID(), key, value: initialValue }]);
  };

  const updateRow = (id: string, patch: Partial<Row>) =>
    commit(rows.map((r) => (r.id === id ? { ...r, ...patch } : r)));

  const removeRow = (id: string) => commit(rows.filter((r) => r.id !== id));

  const count = Object.keys(value).length;
  const isInline = variant === "inline";

  const usedKeys = useMemo(
    () => new Set(rows.map((r) => r.key.trim()).filter(Boolean)),
    [rows],
  );

  const editor = (
    <div className="space-y-2">
      {rows.length > 0 ? (
        <div className="space-y-1.5">
          {rows.map((r) => (
            <LabelRow
              key={r.id}
              row={r}
              definition={definitionsMap.get(r.key.trim())}
              constrained={constrained}
              disabled={disabled}
              onChange={(patch) => updateRow(r.id, patch)}
              onRemove={() => removeRow(r.id)}
            />
          ))}
        </div>
      ) : (
        <p className="text-xs text-muted-foreground italic">
          {t("labelsEditor.noTagsApplied")}
        </p>
      )}
      {!disabled && (
        <div className="flex items-center gap-1">
          {constrained ? (
            <KeyPickerButton
              definitions={definitions ?? []}
              usedKeys={usedKeys}
              onPick={addDefinedRow}
              onPickCustom={(key) =>
                commit([...rows, { id: crypto.randomUUID(), key, value: "" }])
              }
            />
          ) : (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-7 text-xs gap-1"
              onClick={addCustomRow}
            >
              <Plus className="h-3.5 w-3.5" />
              {t("labelsEditor.addLabel")}
            </Button>
          )}
        </div>
      )}
    </div>
  );

  if (!showHeader) return editor;

  return (
    <div className={isInline ? "space-y-1.5" : "space-y-2 rounded-md border p-3"}>
      <button
        type="button"
        className="flex w-full items-center gap-2 text-left"
        onClick={() => setOpen((v) => !v)}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
        )}
        <Tags className="h-3.5 w-3.5 text-muted-foreground" />
        <Label className="text-xs cursor-pointer">{editorTitle}</Label>
        {count > 0 && (
          <Badge variant="secondary" className="text-[10px] h-4 px-1.5">
            {count}
          </Badge>
        )}
      </button>
      {/* Animated expand/collapse, symmetric both directions — same
          grid-template-rows technique used by RuleConfigCard/RulesByColumn's
          collapsibles elsewhere in the app. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">{editor}</div>
      </div>
    </div>
  );
}

// ─── LabelRow — one row, switches UI based on whether key is in catalog ─────

interface LabelRowProps {
  row: Row;
  definition: LabelDefinition | undefined;
  constrained: boolean;
  disabled?: boolean;
  onChange: (patch: Partial<Row>) => void;
  onRemove: () => void;
}

function LabelRow({
  row,
  definition,
  constrained,
  disabled,
  onChange,
  onRemove,
}: LabelRowProps) {
  const { t } = useTranslation();
  const isDefined = constrained && definition !== undefined;

  return (
    <div className="flex items-center gap-1.5">
      {isDefined ? (
        <Badge
          variant="secondary"
          className="h-7 px-2 gap-1 text-xs font-normal max-w-[40%] truncate"
          title={definition!.description || definition!.key}
        >
          <Tag className="h-3 w-3 opacity-60" />
          <span className="truncate">{definition!.key}</span>
        </Badge>
      ) : (
        <Input
          placeholder={t("labelsEditor.keyPlaceholder")}
          value={row.key}
          onChange={(e) => onChange({ key: e.target.value })}
          disabled={disabled}
          className="h-7 text-xs w-28 shrink-0"
        />
      )}
      <span className="text-muted-foreground text-xs">=</span>
      {isDefined ? (
        <ValuePickerButton
          definition={definition!}
          value={row.value}
          onChange={(value) => onChange({ value })}
          disabled={disabled}
        />
      ) : (
        <Input
          placeholder={t("labelsEditor.valuePlaceholder")}
          value={row.value}
          onChange={(e) => onChange({ value: e.target.value })}
          disabled={disabled}
          className="h-7 text-xs flex-1 min-w-0 max-w-40"
        />
      )}
      <Button
        type="button"
        variant="ghost"
        size="icon"
        className="h-7 w-7 shrink-0"
        onClick={onRemove}
        disabled={disabled}
        aria-label={t("labelsEditor.removeLabel")}
      >
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// ─── SearchPickerPopover — shared shell for the "search + pick + custom" ────
// popover pattern used by both the add-label key picker and the value picker.
// A trigger opens a popover containing a search input, a scrollable option
// list, and an optional "or enter a custom …" free-text section.

interface SearchPickerPopoverProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  trigger: ReactNode;
  searchPlaceholder: string;
  query: string;
  onQueryChange: (query: string) => void;
  widthClassName?: string;
  listMaxHeightClassName?: string;
  children: ReactNode;
  footer?: ReactNode;
}

function SearchPickerPopover({
  open,
  onOpenChange,
  trigger,
  searchPlaceholder,
  query,
  onQueryChange,
  widthClassName = "w-64",
  listMaxHeightClassName = "max-h-56",
  children,
  footer,
}: SearchPickerPopoverProps) {
  return (
    <Popover open={open} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>{trigger}</PopoverTrigger>
      <PopoverContent align="start" className={cn(widthClassName, "p-2")}>
        <div className="space-y-2">
          {/* `shouldFilter={false}` — each consumer (key/value picker) keeps
              its own substring-match filtering (`filtered` below), so cmdk's
              fuzzy scoring doesn't reorder results. */}
          <Command shouldFilter={false}>
            <CommandInput placeholder={searchPlaceholder} value={query} onValueChange={onQueryChange} className="h-8 text-xs" />
            <CommandList className={cn(listMaxHeightClassName, "space-y-0.5")}>{children}</CommandList>
          </Command>
          {footer}
        </div>
      </PopoverContent>
    </Popover>
  );
}

// ─── CustomEntrySection — "or enter a custom …" + [Set] free-text escape ────
// hatch shared by the key popover (custom label key) and the value popover
// (custom label value).

interface CustomEntrySectionProps {
  label: string;
  placeholder: string;
  submitLabel: string;
  value: string;
  onValueChange: (value: string) => void;
  onSubmit: () => void;
  note?: ReactNode;
}

function CustomEntrySection({
  label,
  placeholder,
  submitLabel,
  value,
  onValueChange,
  onSubmit,
  note,
}: CustomEntrySectionProps) {
  return (
    <div className="border-t pt-2 space-y-1.5">
      <p className="text-[10px] text-muted-foreground uppercase tracking-wide">
        {label}
      </p>
      <div className="flex gap-1">
        <Input
          placeholder={placeholder}
          value={value}
          onChange={(e) => onValueChange(e.target.value)}
          className="h-7 text-xs"
        />
        <Button
          type="button"
          size="sm"
          className="h-7 px-2 text-xs"
          disabled={!value.trim()}
          onClick={onSubmit}
        >
          {submitLabel}
        </Button>
      </div>
      {note}
    </div>
  );
}

// ─── KeyPickerButton — unified "+ Add label" trigger ────────────────────────
// Opens the shared search-picker popover to either pick an existing
// admin-catalog key or type a free-text custom key, replacing the previous
// pair of separate "+ Add label" / "+ Custom label" buttons.

interface KeyPickerButtonProps {
  definitions: LabelDefinition[];
  usedKeys: Set<string>;
  onPick: (key: string) => void;
  onPickCustom: (key: string) => void;
}

function KeyPickerButton({
  definitions,
  usedKeys,
  onPick,
  onPickCustom,
}: KeyPickerButtonProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [customDraft, setCustomDraft] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return definitions;
    return definitions.filter(
      (d) =>
        d.key.toLowerCase().includes(q) ||
        (d.description ?? "").toLowerCase().includes(q),
    );
  }, [definitions, query]);

  const reset = () => {
    setOpen(false);
    setQuery("");
    setCustomDraft("");
  };

  return (
    <SearchPickerPopover
      open={open}
      onOpenChange={setOpen}
      trigger={
        <Button type="button" variant="ghost" size="sm" className="h-7 text-xs gap-1">
          <Plus className="h-3.5 w-3.5" />
          {t("labelsEditor.addLabel")}
          <ChevronDown className="h-3 w-3 opacity-60" />
        </Button>
      }
      searchPlaceholder={t("labelsEditor.searchKeys")}
      query={query}
      onQueryChange={setQuery}
      widthClassName="w-72"
      listMaxHeightClassName="max-h-64"
      footer={
        <CustomEntrySection
          label={t("labelsEditor.orCustomKey")}
          placeholder={t("labelsEditor.customKeyPlaceholder")}
          submitLabel={t("labelsEditor.set")}
          value={customDraft}
          onValueChange={setCustomDraft}
          onSubmit={() => {
            onPickCustom(customDraft.trim());
            reset();
          }}
        />
      }
    >
      <CommandEmpty>
        <span className="text-xs text-muted-foreground italic">{t("labelsEditor.noMatchingKeys")}</span>
      </CommandEmpty>
      {filtered.map((d) => {
        const used = usedKeys.has(d.key);
        return (
          <CommandItem
            key={d.key}
            value={d.key}
            onSelect={() => {
              onPick(d.key);
              reset();
            }}
            className={cn("flex-col items-stretch gap-0.5 rounded px-2 py-1.5 text-xs", used && "opacity-60")}
          >
            <div className="flex items-center gap-2 w-full">
              <Tag className="h-3 w-3 opacity-60" />
              <span className="font-medium">{d.key}</span>
              {used && (
                <span className="text-[10px] text-muted-foreground">{t("labelsEditor.inUse")}</span>
              )}
              <span className="ml-auto text-[10px] text-muted-foreground shrink-0">
                {d.values.length === 0
                  ? t("labelsEditor.boolean")
                  : t("labelsEditor.valueCount", { count: d.values.length })}
                {d.allow_custom_values ? t("labelsEditor.plusCustom") : ""}
              </span>
            </div>
            {d.description && (
              <p className="mt-0.5 ml-5 text-[11px] text-muted-foreground line-clamp-2">
                {d.description}
              </p>
            )}
          </CommandItem>
        );
      })}
    </SearchPickerPopover>
  );
}

// ─── ValuePickerButton ──────────────────────────────────────────────────────

interface ValuePickerButtonProps {
  definition: LabelDefinition;
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
}

function ValuePickerButton({
  definition,
  value,
  onChange,
  disabled,
}: ValuePickerButtonProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [customDraft, setCustomDraft] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return definition.values;
    return definition.values.filter((v) => v.toLowerCase().includes(q));
  }, [definition.values, query]);

  // Boolean-style definition (no allowed values) — render a simple toggle.
  if (definition.values.length === 0) {
    const isTrue = value === "true" || value === "" || value === undefined;
    return (
      <button
        type="button"
        className={cn(
          "h-7 text-xs font-normal flex-1 min-w-0 max-w-40 rounded border px-2 inline-flex items-center justify-between gap-2 hover:bg-muted",
          disabled && "opacity-50 cursor-not-allowed",
        )}
        onClick={() => onChange(isTrue ? "false" : "true")}
        disabled={disabled}
        title={t("labelsEditor.clickToToggle")}
      >
        <span className="truncate">{value || "true"}</span>
        <span className="text-[10px] text-muted-foreground">{t("labelsEditor.clickToToggle")}</span>
      </button>
    );
  }

  const isCustomValue = value && !definition.values.includes(value);

  const reset = () => {
    setOpen(false);
    setQuery("");
    setCustomDraft("");
  };

  return (
    <SearchPickerPopover
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        // Pre-fill the custom draft with the existing custom value when
        // reopening, but keep it as the single source of truth from here on
        // so the Set button's disabled state always matches what onSubmit
        // will actually send.
        if (next) {
          setCustomDraft(isCustomValue ? value : "");
        }
      }}
      trigger={
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 text-xs flex-1 min-w-0 max-w-40 justify-between font-normal"
          disabled={disabled}
        >
          <span className="truncate">
            {value || (
              <span className="italic text-muted-foreground">{t("labelsEditor.pickValue")}</span>
            )}
          </span>
          <ChevronDown className="h-3 w-3 shrink-0 opacity-60" />
        </Button>
      }
      searchPlaceholder={t("labelsEditor.searchValues")}
      query={query}
      onQueryChange={setQuery}
      footer={
        definition.allow_custom_values && (
          <CustomEntrySection
            label={t("labelsEditor.orCustomValue")}
            placeholder={t("labelsEditor.customPlaceholder")}
            submitLabel={t("labelsEditor.set")}
            value={customDraft}
            onValueChange={setCustomDraft}
            onSubmit={() => {
              onChange(customDraft.trim());
              reset();
            }}
            note={
              isCustomValue && (
                <p className="text-[10px] text-muted-foreground italic">
                  {t("labelsEditor.currentNotInCatalog", { value })}
                </p>
              )
            }
          />
        )
      }
    >
      {filtered.map((v) => (
        <CommandItem
          key={v}
          value={v}
          onSelect={() => {
            onChange(v);
            reset();
          }}
          className={cn("rounded px-2 py-1 text-xs", v === value && "bg-accent/50 font-medium")}
        >
          <Check
            className={cn(
              "h-3.5 w-3.5",
              v === value ? "opacity-100 text-green-500" : "opacity-0",
            )}
          />
          <span className="truncate font-mono">{v}</span>
        </CommandItem>
      ))}
      <CommandEmpty>
        <span className="text-xs text-muted-foreground italic">{t("labelsEditor.noMatchingValues")}</span>
      </CommandEmpty>
    </SearchPickerPopover>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// LabelsBadges — read-side display
// ─────────────────────────────────────────────────────────────────────────────

interface LabelsBadgesProps {
  labels: Record<string, string>;
  /** Show at most N badges; the rest collapse into a "+N" pill with tooltip. */
  max?: number;
  size?: "xs" | "sm";
  className?: string;
}

export function LabelsBadges({
  labels,
  max = 3,
  size = "xs",
  className,
}: LabelsBadgesProps) {
  const entries = Object.entries(labels);
  if (entries.length === 0) return null;

  const visible = entries.slice(0, max);
  const overflow = entries.length - visible.length;

  const sizeClass = size === "xs" ? "text-[10px] px-1.5 py-0" : "text-xs";

  const badgeClass = `${sizeClass} border-blue-500/40 text-blue-700 bg-blue-50/50 font-normal`;

  return (
    <div className={["flex flex-wrap gap-1 items-center", className].filter(Boolean).join(" ")}>
      {visible.map(([k, v]) => (
        <Badge
          key={`${k}=${v}`}
          variant="outline"
          className={badgeClass}
          title={`${k}=${v}`}
        >
          <Tag className="h-2.5 w-2.5 mr-0.5 opacity-60" />
          {formatLabel(k, v)}
        </Badge>
      ))}
      {overflow > 0 && (
        <Popover>
          <PopoverTrigger asChild>
            {/* Stop propagation so clicking the pill inside a table row
                expands the labels instead of triggering row navigation. */}
            <button
              type="button"
              onClick={(e) => e.stopPropagation()}
              className={cn(
                "inline-flex items-center rounded-md border font-normal cursor-pointer transition-colors hover:bg-muted",
                sizeClass,
                "border-muted-foreground/30 text-muted-foreground",
              )}
            >
              +{overflow}
            </button>
          </PopoverTrigger>
          <PopoverContent
            align="start"
            className="w-auto max-w-xs p-2"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex flex-wrap gap-1 max-h-60 overflow-y-auto">
              {entries.map(([k, v]) => (
                <Badge
                  key={`${k}=${v}`}
                  variant="outline"
                  className={badgeClass}
                  title={`${k}=${v}`}
                >
                  <Tag className="h-2.5 w-2.5 mr-0.5 opacity-60" />
                  {formatLabel(k, v)}
                </Badge>
              ))}
            </div>
          </PopoverContent>
        </Popover>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// LabelFilter — multi-select filter for list pages
// ─────────────────────────────────────────────────────────────────────────────

interface LabelFilterProps {
  available: { key: string; value: string }[];
  selected: Set<string>;
  onChange: (selected: Set<string>) => void;
  className?: string;
}

export function LabelFilter({
  available,
  selected,
  onChange,
  className,
}: LabelFilterProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  const tokens = useMemo(() => {
    const set = new Map<string, { key: string; value: string }>();
    for (const { key, value } of available) {
      const tok = labelToken(key, value);
      if (!set.has(tok)) set.set(tok, { key, value });
    }
    return [...set.entries()].sort(([, a], [, b]) =>
      `${a.key}=${a.value}`.localeCompare(`${b.key}=${b.value}`),
    );
  }, [available]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return tokens;
    return tokens.filter(([, { key, value }]) =>
      `${key}=${value}`.toLowerCase().includes(q),
    );
  }, [tokens, query]);

  const toggle = (token: string) => {
    const next = new Set(selected);
    if (next.has(token)) next.delete(token);
    else next.add(token);
    onChange(next);
  };

  const clearAll = () => onChange(new Set());

  const triggerLabel =
    selected.size === 0
      ? t("labelFilter.allLabels")
      : selected.size === 1
        ? (() => {
            const tok = [...selected][0];
            const { key, value } = tokenToLabel(tok);
            return formatLabel(key, value);
          })()
        : t("labelFilter.labelsCount", { count: selected.size });

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className={["h-9 gap-1.5 text-xs justify-between font-normal", className]
            .filter(Boolean)
            .join(" ")}
        >
          <span className="flex items-center gap-1.5 truncate">
            <Tags className="h-3.5 w-3.5 shrink-0 opacity-70" />
            <span className="truncate">{triggerLabel}</span>
          </span>
          <ChevronDown className="h-3.5 w-3.5 shrink-0 opacity-60" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-2">
        <div className="space-y-2">
          <Input
            placeholder={t("labelFilter.searchLabels")}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="h-7 text-xs"
            autoFocus
          />
          <div className="max-h-64 overflow-y-auto space-y-0.5">
            {tokens.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-3 text-center">
                {t("labelFilter.noLabelsFound")}
              </p>
            )}
            {tokens.length > 0 && filtered.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-3 text-center">
                {t("labelFilter.noMatches", { query })}
              </p>
            )}
            {filtered.map(([token, { key, value }]) => (
              <label
                key={token}
                className="flex items-center gap-2 px-1.5 py-1 rounded hover:bg-muted cursor-pointer"
              >
                <Checkbox
                  checked={selected.has(token)}
                  onCheckedChange={() => toggle(token)}
                />
                <span className="text-xs font-mono truncate flex-1" title={`${key}=${value}`}>
                  {formatLabel(key, value)}
                </span>
              </label>
            ))}
          </div>
          {selected.size > 0 && (
            <div className="flex items-center justify-between border-t pt-2">
              <span className="text-[11px] text-muted-foreground">
                {selected.size} {t("labelFilter.selectedSuffix")}
              </span>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-[11px]"
                onClick={clearAll}
              >
                {t("labelFilter.clear")}
              </Button>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

/**
 * Helper used by list pages to test whether a labels map matches the current
 * filter selection. Returns true if no filter is active or if any selected
 * (key, value) pair appears in ``labels``.
 */
export function labelsMatchFilter(
  labels: Record<string, string>,
  selected: Set<string>,
): boolean {
  if (selected.size === 0) return true;
  for (const tok of selected) {
    const { key, value } = tokenToLabel(tok);
    if (labels[key] === value) return true;
  }
  return false;
}
