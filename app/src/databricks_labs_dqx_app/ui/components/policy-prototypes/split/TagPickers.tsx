// THROWAWAY prototype — governed-tag picker lookalike fed by MOCK_GOVERNED_TAGS
// (the real TagPicker calls useListGovernedTags). Same two-level drilldown:
// root lists tag keys; keys with values drill into KEY=value rows.
import { useState } from "react";
import { ArrowLeft, ArrowRight, Plus, Tag, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import { MOCK_GOVERNED_TAGS } from "../mock";

type PickMode = "any" | "keys" | "values";

/** Popover body. *mode* "keys" = bare keys only, "values" = KEY=value only, "any" = both. */
function TagPickerList({ selected, onSelect, mode = "any" }: { selected: string[]; onSelect: (tag: string) => void; mode?: PickMode }) {
  const [view, setView] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const q = search.trim().toLowerCase();
  const groups = MOCK_GOVERNED_TAGS.filter((g) => (mode === "values" ? g.values.length > 0 : true)).filter(
    (g) => !q || g.key.toLowerCase().includes(q) || g.values.some((v) => v.toLowerCase().includes(q)),
  );
  const active = view ? MOCK_GOVERNED_TAGS.find((g) => g.key === view) : undefined;
  const drill = (key: string) => {
    setSearch("");
    setView(key);
  };

  return (
    <Command shouldFilter={false}>
      <CommandInput value={search} onValueChange={setSearch} placeholder="Search governed tags" className="h-9 text-xs" />
      <CommandList className="max-h-64">
        {view === null ? (
          groups.length === 0 ? (
            <CommandEmpty>
              <span className="text-xs text-muted-foreground">No governed tags match.</span>
            </CommandEmpty>
          ) : (
            <CommandGroup>
              {groups.map((g) => {
                const canDrill = mode !== "keys" && g.values.length > 0;
                const bareSelectable = mode !== "values";
                const isSel = selected.includes(g.key);
                return (
                  <CommandItem
                    key={g.key}
                    value={g.key}
                    onSelect={() => (bareSelectable ? onSelect(g.key) : drill(g.key))}
                    className="items-center gap-2 font-mono text-xs"
                  >
                    <span className={cn("min-w-0 flex-1 truncate", isSel && "font-semibold text-primary")}>{g.key}</span>
                    {g.description && <span className="truncate font-sans text-[10px] text-muted-foreground">{g.description}</span>}
                    {canDrill && (
                      <button
                        type="button"
                        aria-label={`Show values for ${g.key}`}
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          drill(g.key);
                        }}
                        className="shrink-0 text-muted-foreground hover:text-foreground"
                      >
                        <ArrowRight className="h-3 w-3" />
                      </button>
                    )}
                  </CommandItem>
                );
              })}
            </CommandGroup>
          )
        ) : (
          <>
            <button
              type="button"
              onClick={() => {
                setView(null);
                setSearch("");
              }}
              className="flex w-full items-center gap-1.5 border-b px-2 py-1.5 text-[11px] font-medium text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="h-3 w-3 shrink-0" />
              <span className="truncate font-mono">{view}</span>
            </button>
            <CommandGroup>
              {(active?.values ?? [])
                .filter((v) => !q || v.toLowerCase().includes(q))
                .map((v) => {
                  const full = `${view}=${v}`;
                  return (
                    <CommandItem key={full} value={full} onSelect={() => onSelect(full)} className="font-mono text-xs">
                      <span className={cn(selected.includes(full) && "font-semibold text-primary")}>{v}</span>
                    </CommandItem>
                  );
                })}
            </CommandGroup>
          </>
        )}
      </CommandList>
    </Command>
  );
}

export function TagChip({ tag, onRemove }: { tag: string; onRemove?: () => void }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md border bg-muted/50 py-0.5 pl-1.5 pr-1 font-mono text-[11px]">
      <Tag className="h-3 w-3 text-muted-foreground" />
      {tag}
      {onRemove && (
        <button type="button" onClick={onRemove} aria-label={`Remove ${tag}`} className="rounded text-muted-foreground hover:text-foreground">
          <X className="h-3 w-3" />
        </button>
      )}
    </span>
  );
}

/** Chips + "Add tag" popover. Bare key matches any value of that key. */
export function TagMultiPicker({ value, onChange, invalid }: { value: string[]; onChange: (tags: string[]) => void; invalid?: boolean }) {
  const [open, setOpen] = useState(false);
  const toggle = (tag: string) => {
    onChange(value.includes(tag) ? value.filter((t) => t !== tag) : [...value, tag]);
    setOpen(false);
  };
  return (
    <div
      className={cn(
        "flex min-h-9 flex-wrap items-center gap-1.5 rounded-md border bg-background px-2 py-1.5 dark:bg-input/30",
        invalid && "border-amber-500/60",
      )}
    >
      {value.map((t) => (
        <TagChip key={t} tag={t} onRemove={() => onChange(value.filter((x) => x !== t))} />
      ))}
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button type="button" variant="ghost" size="sm" className="h-6 gap-1 px-1.5 text-xs text-muted-foreground">
            <Plus className="h-3 w-3" /> {value.length === 0 ? "Add tag" : "Add"}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-80 p-0">
          <TagPickerList selected={value} onSelect={toggle} />
        </PopoverContent>
      </Popover>
      {value.length > 1 && <span className="ml-auto text-[10px] text-muted-foreground">matches any</span>}
    </div>
  );
}

/** Single-value tag button, used inside condition rows and for "pair by tag". */
export function TagSinglePicker({
  value,
  onChange,
  mode,
  placeholder = "Pick tag",
  className,
}: {
  value: string;
  onChange: (tag: string) => void;
  mode: PickMode;
  placeholder?: string;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className={cn("h-8 w-full justify-start gap-1.5 px-2 font-mono text-xs", !value && "text-muted-foreground", className)}
        >
          <Tag className="h-3 w-3 shrink-0" />
          <span className="truncate">{value || placeholder}</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80 p-0">
        <TagPickerList
          selected={value ? [value] : []}
          mode={mode}
          onSelect={(t) => {
            onChange(t);
            setOpen(false);
          }}
        />
      </PopoverContent>
    </Popover>
  );
}
