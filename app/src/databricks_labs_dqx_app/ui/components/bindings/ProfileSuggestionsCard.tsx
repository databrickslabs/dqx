/**
 * Profile-page profiler rule suggestions (B2-82 placement, B2-109 UI).
 *
 * Surfaces the DQX profiler's generated checks on a monitored table's Profile
 * view (dqlake-style AI banner) grouped by check kind. Listing is
 * side-effect-free; each check is toggle-selectable (selecting creates
 * NOTHING). A single "Add checks" action applies the whole selected set in one
 * request via `applyProfilingSuggestions`, which resolves-or-creates + approves
 * each registry rule and binds it — the ONLY path that mints rules (B2-91).
 * Collapsed by default so stewards read the column stats first, then expand
 * when ready to add rules.
 */
import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Check, ChevronRight, Loader2, Plus, Sparkles } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  AI_BANNER_BG,
  AI_BANNER_BORDER,
  AI_BUTTON_BG,
  AI_GRADIENT_URL,
  AI_ICON_COLOR,
  AI_TEXT_GRADIENT,
} from "@/lib/ai-style";
import { cn } from "@/lib/utils";
import {
  getListProfilingSuggestionsQueryKey,
  useApplyProfilingSuggestions,
  useListProfilingSuggestions,
  type AppliedRuleOut,
  type ProfilingSuggestionOut,
} from "@/lib/api";

interface Props {
  bindingId: string;
  canApply: boolean;
  onStageRules: (rows: AppliedRuleOut[]) => void;
  /** Staged applied-rule rows (unsaved + saved baseline) from the Apply Rules
   *  tab. Used to hide suggestions that are already in the working set so they
   *  disappear immediately on "Add checks" rather than waiting for a server
   *  re-fetch.  A suggestion is considered staged when a row exists with the
   *  same (rule_name, column_mapping) pair. */
  stagedRows?: AppliedRuleOut[];
}

function columnsText(mapping: ProfilingSuggestionOut["column_mapping"]): string {
  return Object.values(mapping ?? {}).join(", ");
}

/** Canonical identity key for a profiling suggestion: (rule_name, sorted column_mapping).
 *
 *  ``rule_name`` is the humanized check-function name derived from the profiler
 *  metadata (e.g. "Not Null").  ``column_mapping`` is sorted by key so that
 *  ``{"column": "x"}`` and ``{"column": "x"}`` in different key orders compare
 *  equal.  This key is used both on the suggestion side (``keyForSuggestion``)
 *  and on the staged-row side (``keyForStagedRow``) so a suggestion disappears
 *  as soon as its counterpart lands in the staged working set.
 *
 *  Reliability: profiler suggestions for builtin DQX checks always have a
 *  non-null ``rule_name`` (set by ``build_builtin_metadata`` via
 *  ``humanize_function_name``).  After bug-fix C3/B1, staged rows from the
 *  profiler path also carry ``rule_name`` in the API response.  The composite
 *  key is therefore stable for all practically relevant suggestions.
 */
function suggestionStageKey(ruleName: string | null | undefined, mapping: Record<string, string> | null | undefined): string {
  const sortedMapping = Object.fromEntries(Object.entries(mapping ?? {}).sort());
  return `${ruleName ?? ""}::${JSON.stringify(sortedMapping)}`;
}

/** Group suggestions by check function, preserving first-seen order. */
function groupByKind(suggestions: ProfilingSuggestionOut[]): [string, ProfilingSuggestionOut[]][] {
  const byKind = new Map<string, ProfilingSuggestionOut[]>();
  for (const s of suggestions) {
    const group = byKind.get(s.function);
    if (group) group.push(s);
    else byKind.set(s.function, [s]);
  }
  return [...byKind.entries()];
}

export function ProfileSuggestionsCard({ bindingId, canApply, onStageRules, stagedRows }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());

  const { data } = useListProfilingSuggestions(bindingId);
  const serverSuggestions = data?.data ?? [];

  // Client-side filter: hide suggestions already present in the staged working
  // set (unsaved + saved baseline) so they disappear immediately when "Add
  // checks" is clicked, without waiting for a server re-fetch.
  const stagedKeySet = useMemo((): Set<string> => {
    if (!stagedRows || stagedRows.length === 0) return new Set();
    const keys = new Set<string>();
    for (const row of stagedRows) {
      // A staged row may have multiple column_mapping groups (one per slot
      // group after normalizeStagedRows splits them).  We add a key for each
      // group so every mapping variant is covered.
      for (const group of row.column_mapping ?? []) {
        keys.add(suggestionStageKey(row.rule_name, group));
      }
      // Also cover the case where column_mapping is empty (staging with no
      // mapping yet — key falls back to rule_name alone).
      if ((row.column_mapping ?? []).length === 0) {
        keys.add(suggestionStageKey(row.rule_name, {}));
      }
    }
    return keys;
  }, [stagedRows]);

  const suggestions = useMemo(
    () => serverSuggestions.filter((s) => !stagedKeySet.has(suggestionStageKey(s.rule_name, s.column_mapping))),
    [serverSuggestions, stagedKeySet],
  );

  const applyMutation = useApplyProfilingSuggestions();

  if (suggestions.length === 0) return null;

  const groups = groupByKind(suggestions);
  const allIndices = suggestions.map((s) => s.index);
  const allSelected = selected.size === allIndices.length;
  const someSelected = selected.size > 0 && !allSelected;

  const toggle = (index: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  };

  const toggleAll = () => {
    setSelected((prev) => (prev.size === allIndices.length ? new Set() : new Set(allIndices)));
  };

  const handleApply = () => {
    const indices = [...selected];
    if (indices.length === 0) return;
    applyMutation.mutate(
      { bindingId, data: { indices } },
      {
        onSuccess: async (resp) => {
          const applied = resp.data.applied ?? [];
          const appliedCount = applied.length;
          const failedCount = resp.data.failed?.length ?? 0;
          if (appliedCount > 0) {
            toast.success(t("monitoredTables.toastSuggestionsApplied", { count: appliedCount }));
          }
          if (failedCount > 0) {
            const message = t("monitoredTables.toastSuggestionsFailed", { count: failedCount });
            if (appliedCount > 0) toast.warning(message);
            else toast.error(message);
          }
          setSelected(new Set());
          onStageRules(applied);
          await queryClient.invalidateQueries({ queryKey: getListProfilingSuggestionsQueryKey(bindingId) });
        },
        onError: () => {
          toast.error(t("monitoredTables.toastApplyFailed"));
        },
      },
    );
  };

  const kindCount = groups.length;
  const checkCount = suggestions.length;

  return (
    <div className={cn("rounded-md", AI_BANNER_BG, AI_BANNER_BORDER)}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-3 py-2 text-sm font-medium text-left hover:bg-fuchsia-500/5"
      >
        <ChevronRight
          className={cn("h-3.5 w-3.5 transition-transform duration-200", AI_ICON_COLOR, open && "rotate-90")}
        />
        <Sparkles className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
        <span className={AI_TEXT_GRADIENT}>
          {t("monitoredTables.suggestedRulesCount", { count: kindCount })}
          {" / "}
          {t("monitoredTables.suggestedChecksCount", { count: checkCount })}
        </span>
      </button>
      {/* Animated collapse via grid-template-rows 0fr <-> 1fr. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="min-h-0 overflow-hidden">
          <div className="px-3 pb-3 space-y-3">
            {canApply && (
              <div className="flex items-center justify-between gap-3 border-b border-fuchsia-500/20 pb-2">
                <button
                  type="button"
                  onClick={toggleAll}
                  className="flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground"
                >
                  <Checkbox
                    checked={allSelected ? true : someSelected ? "indeterminate" : false}
                    className="pointer-events-none"
                    tabIndex={-1}
                    aria-hidden
                  />
                  <span>{t("monitoredTables.selectAllSuggestions")}</span>
                </button>
                <Button
                  size="sm"
                  className={cn("shrink-0 gap-1.5", AI_BUTTON_BG)}
                  disabled={selected.size === 0 || applyMutation.isPending}
                  onClick={handleApply}
                >
                  {applyMutation.isPending ? (
                    <>
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      {t("monitoredTables.addingSuggestions")}
                    </>
                  ) : (
                    <>
                      <Plus className="h-3.5 w-3.5" />
                      {t("monitoredTables.addSelectedChecksButton", { count: selected.size })}
                    </>
                  )}
                </Button>
              </div>
            )}
            {groups.map(([kind, items]) => (
              <div key={kind} className="space-y-1.5">
                <div className="flex items-center gap-2">
                  <Badge variant="secondary" className="font-mono text-[10px]">
                    {kind}
                  </Badge>
                  <span className="text-xs text-muted-foreground">
                    {t("monitoredTables.suggestedChecksCount", { count: items.length })}
                  </span>
                </div>
                <ul className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-1.5">
                  {items.map((s) => {
                    const cols = columnsText(s.column_mapping);
                    const isSelected = selected.has(s.index);
                    const cardBody = (
                      <>
                        {canApply && (
                          <span
                            aria-hidden
                            className={cn(
                              "mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-[4px] border",
                              isSelected
                                ? "border-fuchsia-500 bg-fuchsia-500 text-white dark:border-fuchsia-400 dark:bg-fuchsia-400"
                                : "border-muted-foreground/40",
                            )}
                          >
                            {isSelected && <Check className="h-3 w-3" strokeWidth={3} />}
                          </span>
                        )}
                        <span className="min-w-0 space-y-0.5">
                          <span className="block font-mono font-semibold truncate">
                            {cols || s.rule_name || s.function}
                          </span>
                        </span>
                      </>
                    );
                    return (
                      <li key={s.index}>
                        {canApply ? (
                          <button
                            type="button"
                            role="checkbox"
                            aria-checked={isSelected}
                            aria-label={t("monitoredTables.selectSuggestionLabel", {
                              name: cols || s.rule_name || s.function,
                            })}
                            onClick={() => toggle(s.index)}
                            className={cn(
                              "flex w-full items-start gap-2 rounded border bg-card/80 px-2 py-1.5 text-left text-xs backdrop-blur transition-colors",
                              "hover:border-fuchsia-500/50 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring",
                              isSelected && "border-fuchsia-500/60 bg-fuchsia-500/5 dark:border-fuchsia-400/60",
                            )}
                          >
                            {cardBody}
                          </button>
                        ) : (
                          <div className="flex items-start gap-2 rounded border bg-card/80 px-2 py-1.5 text-xs backdrop-blur">
                            {cardBody}
                          </div>
                        )}
                      </li>
                    );
                  })}
                </ul>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
