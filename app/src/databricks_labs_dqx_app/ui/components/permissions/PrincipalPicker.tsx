/**
 * PrincipalPicker — a Popover + Command combobox for picking a workspace
 * user or group, backed by the debounced `useSearchPrincipals` API.
 *
 * Ported from dqlake's `StewardPicker`, adapted to the Permissions API's
 * principal shape (`kind: 'user' | 'group'`, lowercase) and generalised so
 * it can back both the free-text steward field (stores `display_name`) and
 * the grant editor's principal selection.
 */
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Check, ChevronDown, Loader2, User as UserIcon, Users } from "lucide-react";
import { cn } from "@/lib/utils";
import { useSearchPrincipals, type PrincipalSearchOut } from "@/lib/api";

/** The picked principal, in the shape the grant/steward callers store. */
export interface PickedPrincipal {
  principal_id: string;
  principal_type: string;
  principal_name: string;
}

// Local debounce — delays the query so we don't fire a SCIM call on every
// keystroke. 300ms of perceived latency for a big reduction in workspace-wide
// SCIM scans.
function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(id);
  }, [value, delayMs]);
  return debounced;
}

const SEARCH_DEBOUNCE_MS = 300;
const MIN_SEARCH_LENGTH = 2;

export interface PrincipalPickerProps {
  value: PickedPrincipal | null;
  onSelect: (p: PrincipalSearchOut) => void;
  onClear: () => void;
  disabled?: boolean;
  /** Optional pinned "Suggested" option (e.g. default-to-creator). Clicking
   *  it fires `suggestion.onPick`; the picker doesn't commit it itself. */
  suggestion?: { displayName: string; onPick: () => void } | null;
  className?: string;
}

export function PrincipalPicker({
  value,
  onSelect,
  onClear,
  disabled,
  suggestion,
  className,
}: PrincipalPickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const debouncedQuery = useDebouncedValue(query, SEARCH_DEBOUNCE_MS);

  const { data: results, isFetching } = useSearchPrincipals(
    { q: debouncedQuery, limit: 10 },
    {
      query: {
        enabled: debouncedQuery.length >= MIN_SEARCH_LENGTH,
        select: (d) => d.data,
      },
    },
  );

  const handleSelect = useCallback(
    (p: PrincipalSearchOut) => {
      onSelect(p);
      setOpen(false);
      setQuery("");
    },
    [onSelect],
  );

  const isGroup = value?.principal_type === "group";

  return (
    <Popover open={open} onOpenChange={disabled ? undefined : setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className={cn("w-72 justify-between", className)}
        >
          <span
            className={cn("truncate", !value && "text-muted-foreground")}
            title={value?.principal_name ?? undefined}
          >
            {value ? (
              <span className="flex items-center gap-1.5">
                {isGroup ? (
                  <Users className="h-3.5 w-3.5 shrink-0" />
                ) : (
                  <UserIcon className="h-3.5 w-3.5 shrink-0" />
                )}
                {value.principal_name}
              </span>
            ) : suggestion ? (
              <span className="italic">{suggestion.displayName}</span>
            ) : (
              t("permissions.selectPrincipal")
            )}
          </span>
          {isFetching ? (
            <Loader2 className="ml-2 h-4 w-4 shrink-0 animate-spin opacity-50" />
          ) : (
            <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-72 p-0" align="start">
        <Command shouldFilter={false}>
          <CommandInput
            placeholder={t("permissions.searchPlaceholder")}
            value={query}
            onValueChange={setQuery}
          />
          <CommandList onWheel={(e) => e.stopPropagation()}>
            {suggestion && !value && (
              <CommandGroup heading={t("permissions.suggested")}>
                <CommandItem
                  value="__suggested__"
                  onSelect={() => {
                    suggestion.onPick();
                    setOpen(false);
                    setQuery("");
                  }}
                  className="flex items-center gap-2"
                >
                  <UserIcon className="h-4 w-4 text-muted-foreground shrink-0" />
                  <span className="truncate flex-1">{suggestion.displayName}</span>
                  <span className="text-[10px] text-muted-foreground">{t("permissions.suggestedNote")}</span>
                </CommandItem>
              </CommandGroup>
            )}
            {value && (
              <CommandGroup>
                <CommandItem
                  value="__clear__"
                  onSelect={() => {
                    onClear();
                    setOpen(false);
                    setQuery("");
                  }}
                  className="text-muted-foreground italic"
                >
                  {t("permissions.clearSelection")}
                </CommandItem>
              </CommandGroup>
            )}
            {query.length === 0 ? (
              <CommandEmpty>{t("permissions.startTyping")}</CommandEmpty>
            ) : query.length < MIN_SEARCH_LENGTH ? (
              <CommandEmpty>{t("permissions.typeMore", { count: MIN_SEARCH_LENGTH })}</CommandEmpty>
            ) : isFetching ? (
              <CommandEmpty>{t("permissions.searching")}</CommandEmpty>
            ) : !results || results.length === 0 ? (
              <CommandEmpty>{t("permissions.noResults")}</CommandEmpty>
            ) : (
              <CommandGroup>
                {results.map((p) => (
                  <CommandItem
                    key={p.workspace_principal_id}
                    value={p.workspace_principal_id}
                    onSelect={() => handleSelect(p)}
                    className="flex items-center gap-2"
                  >
                    {p.kind === "group" ? (
                      <Users className="h-4 w-4 text-muted-foreground shrink-0" />
                    ) : (
                      <UserIcon className="h-4 w-4 text-muted-foreground shrink-0" />
                    )}
                    <span className="truncate flex-1" title={p.display_name}>
                      {p.display_name}
                    </span>
                    <Badge variant="outline" className="ml-auto shrink-0 text-xs capitalize">
                      {p.kind === "group" ? t("permissions.group") : t("permissions.user")}
                    </Badge>
                    {value?.principal_id === p.workspace_principal_id && (
                      <Check className="h-4 w-4 shrink-0" />
                    )}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
