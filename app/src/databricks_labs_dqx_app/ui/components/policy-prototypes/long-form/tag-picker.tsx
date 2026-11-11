// THROWAWAY prototype — mock lookalike of apply-rules/TagPicker (no API; reads MOCK_GOVERNED_TAGS).
import { useState } from "react";
import { ArrowLeft, ArrowRight, Check, ChevronsUpDown, Plus, X } from "lucide-react";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { MOCK_GOVERNED_TAGS } from "../mock";

/** any = bare key or key=value; key = keys only; keyValue = key=value only. */
export type TagPickMode = "any" | "key" | "keyValue";

function TagListBody({
  selected,
  onSelect,
  mode,
}: {
  selected: string[];
  onSelect: (tag: string) => void;
  mode: TagPickMode;
}) {
  const [view, setView] = useState<string | null>(null);
  const [rootSearch, setRootSearch] = useState("");
  const [valueSearch, setValueSearch] = useState("");

  const groups = MOCK_GOVERNED_TAGS.filter((g) => mode !== "keyValue" || g.values.length > 0).filter((g) =>
    g.key.toLowerCase().includes(rootSearch.trim().toLowerCase()),
  );
  const active = MOCK_GOVERNED_TAGS.find((g) => g.key === view);
  const values = (active?.values ?? []).filter((v) => v.toLowerCase().includes(valueSearch.trim().toLowerCase()));
  const isSelected = (tag: string) => selected.includes(tag);

  return (
    <Command shouldFilter={false}>
      <CommandInput
        value={view === null ? rootSearch : valueSearch}
        onValueChange={view === null ? setRootSearch : setValueSearch}
        placeholder={view === null ? "Search governed tags" : `Search ${view} values`}
        className="h-9 text-xs"
      />
      <CommandList className="max-h-64">
        {view === null ? (
          groups.length === 0 ? (
            <CommandEmpty>
              <span className="text-xs text-muted-foreground">No governed tags match</span>
            </CommandEmpty>
          ) : (
            <CommandGroup>
              {groups.map((g) => {
                const canDrill = mode !== "key" && g.values.length > 0;
                const bareSelectable = mode !== "keyValue";
                return (
                  <CommandItem
                    key={g.key}
                    value={g.key}
                    onSelect={() => {
                      if (bareSelectable) onSelect(g.key);
                      else {
                        setValueSearch("");
                        setView(g.key);
                      }
                    }}
                    className="items-center gap-2 text-xs"
                  >
                    <Check className={cn("h-3 w-3 shrink-0", isSelected(g.key) ? "opacity-100" : "opacity-0")} />
                    <span className="font-mono">{g.key}</span>
                    {g.description && <span className="min-w-0 flex-1 truncate text-muted-foreground">{g.description}</span>}
                    {!g.description && <span className="flex-1" />}
                    {canDrill && (
                      <button
                        type="button"
                        aria-label={`Show values for ${g.key}`}
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          setValueSearch("");
                          setView(g.key);
                        }}
                        className="inline-flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
                      >
                        {g.values.length}
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
                setValueSearch("");
              }}
              className="flex w-full items-center gap-1.5 border-b px-2 py-1.5 text-[11px] font-medium text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="h-3 w-3 shrink-0" />
              <span className="font-mono">{view}</span>
            </button>
            {values.length === 0 ? (
              <CommandEmpty>
                <span className="text-xs text-muted-foreground">No values match</span>
              </CommandEmpty>
            ) : (
              <CommandGroup>
                {values.map((v) => {
                  const full = `${view}=${v}`;
                  return (
                    <CommandItem key={full} value={full} onSelect={() => onSelect(full)} className="gap-2 text-xs font-mono">
                      <Check className={cn("h-3 w-3 shrink-0", isSelected(full) ? "opacity-100" : "opacity-0")} />
                      {v}
                    </CommandItem>
                  );
                })}
              </CommandGroup>
            )}
          </>
        )}
      </CommandList>
    </Command>
  );
}

export function TagChip({ tag, onRemove }: { tag: string; onRemove?: () => void }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md border bg-muted/50 py-0.5 pr-1 pl-2 font-mono text-xs">
      {tag}
      {onRemove && (
        <button
          type="button"
          onClick={onRemove}
          aria-label={`Remove ${tag}`}
          className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <X className="h-3 w-3" />
        </button>
      )}
    </span>
  );
}

/** Multi-select: chips + "Add tag" popover. Bare keys match any value of that key. */
export function TagMultiSelect({
  value,
  onChange,
  invalid,
}: {
  value: string[];
  onChange: (next: string[]) => void;
  invalid?: boolean;
}) {
  const toggle = (tag: string) => onChange(value.includes(tag) ? value.filter((t) => t !== tag) : [...value, tag]);
  return (
    <div
      className={cn(
        "flex min-h-10 flex-wrap items-center gap-1.5 rounded-md border bg-background px-2 py-1.5",
        invalid && "border-destructive",
      )}
    >
      {value.map((t) => (
        <TagChip key={t} tag={t} onRemove={() => toggle(t)} />
      ))}
      <Popover>
        <PopoverTrigger asChild>
          <Button type="button" variant="ghost" size="sm" className="h-7 gap-1 px-2 text-xs text-muted-foreground">
            <Plus className="h-3.5 w-3.5" />
            {value.length === 0 ? "Select governed tags" : "Add tag"}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-80 p-0">
          <TagListBody selected={value} onSelect={toggle} mode="any" />
        </PopoverContent>
      </Popover>
    </div>
  );
}

/** Single-select lookalike of a SelectTrigger, used inside condition rows and for "Pair columns by tag". */
export function TagSingleSelect({
  value,
  onChange,
  mode,
  placeholder,
  clearable,
  className,
}: {
  value: string;
  onChange: (next: string) => void;
  mode: TagPickMode;
  placeholder: string;
  clearable?: boolean;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className={cn("flex items-center gap-1", className)}>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <button
            type="button"
            className="flex h-8 w-full min-w-0 items-center justify-between gap-2 rounded-md border bg-background px-3 text-left text-xs shadow-xs hover:bg-muted/30"
          >
            <span className={cn("truncate font-mono", !value && "font-sans text-muted-foreground")}>
              {value || placeholder}
            </span>
            <ChevronsUpDown className="h-3.5 w-3.5 shrink-0 opacity-50" />
          </button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-80 p-0">
          <TagListBody
            selected={value ? [value] : []}
            mode={mode}
            onSelect={(t) => {
              onChange(t);
              setOpen(false);
            }}
          />
        </PopoverContent>
      </Popover>
      {clearable && value && (
        <Button type="button" variant="ghost" size="icon-sm" aria-label="Clear" onClick={() => onChange("")}>
          <X className="h-4 w-4" />
        </Button>
      )}
    </div>
  );
}
