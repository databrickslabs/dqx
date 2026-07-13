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
import { useState } from "react";
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
  getGetMonitoredTableQueryKey,
  getListProfilingSuggestionsQueryKey,
  useApplyProfilingSuggestions,
  useListProfilingSuggestions,
  type ProfilingSuggestionOut,
} from "@/lib/api";

interface Props {
  bindingId: string;
  canApply: boolean;
}

function columnsText(mapping: ProfilingSuggestionOut["column_mapping"]): string {
  return Object.values(mapping ?? {}).join(", ");
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

export function ProfileSuggestionsCard({ bindingId, canApply }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());

  const { data } = useListProfilingSuggestions(bindingId);
  const suggestions = data?.data ?? [];

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
          const appliedCount = resp.data.applied?.length ?? 0;
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
          await Promise.all([
            queryClient.invalidateQueries({ queryKey: getListProfilingSuggestionsQueryKey(bindingId) }),
            queryClient.invalidateQueries({ queryKey: getGetMonitoredTableQueryKey(bindingId) }),
          ]);
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
                          {s.dimension && (
                            <Badge variant="outline" className="text-[10px]">
                              {s.dimension}
                            </Badge>
                          )}
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
