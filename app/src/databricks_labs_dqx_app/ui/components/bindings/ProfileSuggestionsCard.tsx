/**
 * Profile-page profiler rule suggestions (B2-82).
 *
 * Surfaces the DQX profiler's generated checks on a monitored table's Profile
 * view (dqlake-style placement) and lets a user apply one to the table. Listing
 * is side-effect-free; applying calls `applyProfilingSuggestion`, which resolves
 * or creates + approves the registry rule and binds it. Collapsed by default so
 * stewards read the column stats first, then expand when ready to add rules.
 */
import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { ChevronRight, Lightbulb, Loader2, Plus } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
  getGetMonitoredTableQueryKey,
  getListProfilingSuggestionsQueryKey,
  useApplyProfilingSuggestion,
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

export function ProfileSuggestionsCard({ bindingId, canApply }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);

  const { data } = useListProfilingSuggestions(bindingId);
  const suggestions = data?.data ?? [];

  const applyMutation = useApplyProfilingSuggestion();
  const [applyingIndex, setApplyingIndex] = useState<number | null>(null);

  if (suggestions.length === 0) return null;

  const handleApply = (suggestion: ProfilingSuggestionOut) => {
    setApplyingIndex(suggestion.index);
    applyMutation.mutate(
      { bindingId, index: suggestion.index },
      {
        onSuccess: async () => {
          toast.success(
            t("monitoredTables.profileSuggestionAppliedToast", {
              defaultValue: "Rule applied to the table.",
            }),
          );
          await Promise.all([
            queryClient.invalidateQueries({ queryKey: getListProfilingSuggestionsQueryKey(bindingId) }),
            queryClient.invalidateQueries({ queryKey: getGetMonitoredTableQueryKey(bindingId) }),
          ]);
        },
        onError: () => {
          toast.error(t("monitoredTables.toastApplyFailed"));
        },
        onSettled: () => {
          setApplyingIndex(null);
        },
      },
    );
  };

  return (
    <div className="rounded-md border bg-muted/30">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-3 py-2 text-sm font-medium text-left hover:bg-muted/50 rounded-md"
      >
        <ChevronRight className={cn("h-3.5 w-3.5 transition-transform duration-200", open && "rotate-90")} />
        <Lightbulb className="h-3.5 w-3.5 text-amber-500" />
        <span>{t("monitoredTables.suggestedColumnsHeading")}</span>
        <Badge variant="secondary" className="ml-1">
          {suggestions.length}
        </Badge>
      </button>
      {/* Animated collapse via grid-template-rows 0fr <-> 1fr. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="min-h-0 overflow-hidden">
          <ul className="px-3 pb-3 space-y-1.5">
            {suggestions.map((s) => {
              const cols = columnsText(s.column_mapping);
              const applying = applyingIndex === s.index;
              return (
                <li
                  key={s.index}
                  className="flex items-center justify-between gap-3 rounded border bg-card/80 px-2.5 py-1.5"
                >
                  <div className="min-w-0 space-y-0.5">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-medium truncate">{s.rule_name || s.function}</span>
                      {s.dimension && (
                        <Badge variant="outline" className="text-[10px]">
                          {s.dimension}
                        </Badge>
                      )}
                    </div>
                    {cols && <div className="font-mono text-xs text-muted-foreground truncate">{cols}</div>}
                  </div>
                  {canApply && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="shrink-0 gap-1.5"
                      disabled={applying || applyMutation.isPending}
                      onClick={() => handleApply(s)}
                    >
                      {applying ? (
                        <>
                          <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          {t("monitoredTables.applying")}
                        </>
                      ) : (
                        <>
                          <Plus className="h-3.5 w-3.5" />
                          {t("monitoredTables.applyButton")}
                        </>
                      )}
                    </Button>
                  )}
                </li>
              );
            })}
          </ul>
        </div>
      </div>
    </div>
  );
}
