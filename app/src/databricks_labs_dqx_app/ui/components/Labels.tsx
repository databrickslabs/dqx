import { useMemo, useState } from "react";
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
//   1. Constrained — when ``definitions`` is provided & non-empty. Users pick
//      keys and values from admin-curated dropdowns. A "Custom label" escape
//      hatch is always available for free-form entries.
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
  title = "Labels (optional)",
  defaultOpen = false,
  variant = "inline",
  showHeader = true,
  definitions,
}: LabelsEditorProps) {
  const [open, setOpen] = useState(defaultOpen);
  const [rows, setRows] = useState<Row[]>(() => recordToRows(value));

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
      {rows.length > 0 && (
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
      )}
      <div className="flex items-center gap-1">
        {constrained ? (
          <KeyPickerButton
            definitions={definitions ?? []}
            usedKeys={usedKeys}
            onPick={addDefinedRow}
            disabled={disabled}
          />
        ) : (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-7 text-xs gap-1"
            onClick={addCustomRow}
            disabled={disabled}
          >
            <Plus className="h-3.5 w-3.5" />
            Add label
          </Button>
        )}
        {constrained && (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-7 text-xs gap-1 text-muted-foreground"
            onClick={addCustomRow}
            disabled={disabled}
            title="Add a label whose key isn't in the admin catalog"
          >
            <Plus className="h-3.5 w-3.5" />
            Custom label
          </Button>
        )}
      </div>
    </div>
  );

  if (!showHeader) return editor;

  return (
    <div className={isInline ? "space-y-1.5" : "space-y-2 rounded-md border p-3"}>
      <button
        type="button"
        className="flex w-full items-center gap-2 text-left"
        onClick={() => setOpen((v) => !v)}
        disabled={disabled}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
        )}
        <Tags className="h-3.5 w-3.5 text-muted-foreground" />
        <Label className="text-xs cursor-pointer">{title}</Label>
        {count > 0 && (
          <Badge variant="secondary" className="text-[10px] h-4 px-1.5">
            {count}
          </Badge>
        )}
      </button>
      {open && editor}
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
          placeholder="key"
          value={row.key}
          onChange={(e) => onChange({ key: e.target.value })}
          disabled={disabled}
          className="h-7 text-xs flex-1 min-w-0"
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
          placeholder="value (blank → true)"
          value={row.value}
          onChange={(e) => onChange({ value: e.target.value })}
          disabled={disabled}
          className="h-7 text-xs flex-1 min-w-0"
        />
      )}
      <Button
        type="button"
        variant="ghost"
        size="icon"
        className="h-7 w-7 shrink-0"
        onClick={onRemove}
        disabled={disabled}
        aria-label="Remove label"
      >
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// ─── KeyPickerButton ────────────────────────────────────────────────────────

interface KeyPickerButtonProps {
  definitions: LabelDefinition[];
  usedKeys: Set<string>;
  onPick: (key: string) => void;
  disabled?: boolean;
}

function KeyPickerButton({
  definitions,
  usedKeys,
  onPick,
  disabled,
}: KeyPickerButtonProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return definitions;
    return definitions.filter(
      (d) =>
        d.key.toLowerCase().includes(q) ||
        (d.description ?? "").toLowerCase().includes(q),
    );
  }, [definitions, query]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="h-7 text-xs gap-1"
          disabled={disabled || definitions.length === 0}
        >
          <Plus className="h-3.5 w-3.5" />
          Add label
          <ChevronDown className="h-3 w-3 opacity-60" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-2">
        <div className="space-y-2">
          <Input
            placeholder="Search label keys…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="h-7 text-xs"
            autoFocus
          />
          <div className="max-h-64 overflow-y-auto space-y-0.5">
            {filtered.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-3 text-center">
                No matching keys.
              </p>
            )}
            {filtered.map((d) => {
              const used = usedKeys.has(d.key);
              return (
                <button
                  key={d.key}
                  type="button"
                  onClick={() => {
                    onPick(d.key);
                    setOpen(false);
                    setQuery("");
                  }}
                  className={cn(
                    "w-full text-left rounded px-2 py-1.5 text-xs hover:bg-muted",
                    used && "opacity-60",
                  )}
                >
                  <div className="flex items-center gap-2">
                    <Tag className="h-3 w-3 opacity-60" />
                    <span className="font-medium">{d.key}</span>
                    {used && (
                      <span className="text-[10px] text-muted-foreground">(in use)</span>
                    )}
                    <span className="ml-auto text-[10px] text-muted-foreground shrink-0">
                      {d.values.length === 0
                        ? "boolean"
                        : `${d.values.length} value${d.values.length === 1 ? "" : "s"}`}
                      {d.allow_custom_values ? " + custom" : ""}
                    </span>
                  </div>
                  {d.description && (
                    <p className="mt-0.5 ml-5 text-[11px] text-muted-foreground line-clamp-2">
                      {d.description}
                    </p>
                  )}
                </button>
              );
            })}
          </div>
        </div>
      </PopoverContent>
    </Popover>
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
          "h-7 text-xs flex-1 min-w-0 rounded border px-2 inline-flex items-center justify-between gap-2 hover:bg-muted",
          disabled && "opacity-50 cursor-not-allowed",
        )}
        onClick={() => onChange(isTrue ? "false" : "true")}
        disabled={disabled}
        title="Click to toggle"
      >
        <span className="truncate font-mono">{value || "true"}</span>
        <span className="text-[10px] text-muted-foreground">click to toggle</span>
      </button>
    );
  }

  const isCustomValue = value && !definition.values.includes(value);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 text-xs flex-1 min-w-0 justify-between font-normal"
          disabled={disabled}
        >
          <span className="truncate">
            {value || (
              <span className="italic text-muted-foreground">Pick a value…</span>
            )}
          </span>
          <ChevronDown className="h-3 w-3 shrink-0 opacity-60" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-64 p-2">
        <div className="space-y-2">
          <Input
            placeholder="Search values…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="h-7 text-xs"
            autoFocus
          />
          <div className="max-h-56 overflow-y-auto space-y-0.5">
            {filtered.map((v) => (
              <button
                key={v}
                type="button"
                onClick={() => {
                  onChange(v);
                  setOpen(false);
                  setQuery("");
                }}
                className={cn(
                  "w-full text-left rounded px-2 py-1 text-xs hover:bg-muted flex items-center gap-2",
                  v === value && "bg-accent/50 font-medium",
                )}
              >
                <Check
                  className={cn(
                    "h-3.5 w-3.5",
                    v === value ? "opacity-100 text-green-500" : "opacity-0",
                  )}
                />
                <span className="truncate font-mono">{v}</span>
              </button>
            ))}
            {filtered.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-2 text-center">
                No matching values.
              </p>
            )}
          </div>
          {definition.allow_custom_values && (
            <div className="border-t pt-2 space-y-1.5">
              <p className="text-[10px] text-muted-foreground uppercase tracking-wide">
                Or enter custom value
              </p>
              <div className="flex gap-1">
                <Input
                  placeholder="custom…"
                  value={isCustomValue && !customDraft ? value : customDraft}
                  onChange={(e) => setCustomDraft(e.target.value)}
                  className="h-7 text-xs"
                />
                <Button
                  type="button"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  disabled={!customDraft.trim()}
                  onClick={() => {
                    onChange(customDraft.trim());
                    setCustomDraft("");
                    setOpen(false);
                    setQuery("");
                  }}
                >
                  Set
                </Button>
              </div>
              {isCustomValue && (
                <p className="text-[10px] text-muted-foreground italic">
                  Current value <code>{value}</code> isn&apos;t in the catalog.
                </p>
              )}
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
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
  const overflowLabel = entries
    .slice(max)
    .map(([k, v]) => formatLabel(k, v))
    .join(", ");

  const sizeClass = size === "xs" ? "text-[10px] px-1.5 py-0" : "text-xs";

  return (
    <div className={["flex flex-wrap gap-1 items-center", className].filter(Boolean).join(" ")}>
      {visible.map(([k, v]) => (
        <Badge
          key={`${k}=${v}`}
          variant="outline"
          className={`${sizeClass} border-blue-500/40 text-blue-700 bg-blue-50/50 font-normal`}
          title={`${k}=${v}`}
        >
          <Tag className="h-2.5 w-2.5 mr-0.5 opacity-60" />
          {formatLabel(k, v)}
        </Badge>
      ))}
      {overflow > 0 && (
        <Badge
          variant="outline"
          className={`${sizeClass} border-muted-foreground/30 text-muted-foreground font-normal`}
          title={overflowLabel}
        >
          +{overflow}
        </Badge>
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
      ? "All Labels"
      : selected.size === 1
        ? (() => {
            const tok = [...selected][0];
            const { key, value } = tokenToLabel(tok);
            return formatLabel(key, value);
          })()
        : `${selected.size} labels`;

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
            placeholder="Search labels…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="h-7 text-xs"
            autoFocus
          />
          <div className="max-h-64 overflow-y-auto space-y-0.5">
            {tokens.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-3 text-center">
                No labels found in the current view.
              </p>
            )}
            {tokens.length > 0 && filtered.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-2 py-3 text-center">
                No matches for &quot;{query}&quot;.
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
                {selected.size} selected
              </span>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-[11px]"
                onClick={clearAll}
              >
                Clear
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
