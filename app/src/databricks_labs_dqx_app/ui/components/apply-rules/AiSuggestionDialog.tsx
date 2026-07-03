// AiSuggestionDialog — AI-suggested registry rules for a monitored table,
// ported (structure/interactions) from dqlake's `bindings/AiSuggestionDialog.tsx`:
// a checkbox list of suggestions (dimension/severity tags, explanation,
// mapping chips) with a single "Add N" action that applies every checked
// suggestion to the binding. Always renders (never blocks the tab) — a
// missing/misconfigured AI backend degrades to an "unavailable" empty state
// via `available`/`reason` on the response, matching DQX's AI kill-switch
// semantics (see `useAiAvailability`).

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Loader2, Sparkles } from "lucide-react";
import {
  useApplyRuleToTable,
  useSuggestRulesForTable,
  type SuggestedRuleMappingOut,
} from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BANNER_BORDER, AI_BUTTON_BG, AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";
import { MappingChips } from "./MappingChips";
import { RESERVED_DIMENSION_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor } from "./shared";

interface SuggestRulesState {
  available: boolean;
  reason?: string;
  suggestions: SuggestedRuleMappingOut[];
}

interface AiSuggestionDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  labelDefinitions: LabelDefinition[];
  onApplied: () => void;
  onAiUnavailable: (reason: string) => void;
}

export function AiSuggestionDialog({
  open,
  onOpenChange,
  bindingId,
  labelDefinitions,
  onApplied,
  onAiUnavailable,
}: AiSuggestionDialogProps) {
  const { t } = useTranslation();
  const suggestMutation = useSuggestRulesForTable();
  const applyMutation = useApplyRuleToTable();
  const [state, setState] = useState<SuggestRulesState | null>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [applying, setApplying] = useState(false);
  const fetchedForRef = useRef<string | null>(null);
  // `mutate` is referentially stable across renders (TanStack Query
  // guarantee); the wrapping `suggestMutation` object is NOT, so it must
  // stay out of the effect's dependency array below — including it caused
  // an infinite render loop (the object identity changes every render,
  // re-triggering the effect's unconditional `setSelected(new Set())` reset
  // branch on every render while the dialog is closed).
  const { mutate: suggestRules } = suggestMutation;

  useEffect(() => {
    if (!open) {
      setState(null);
      setSelected(new Set());
      fetchedForRef.current = null;
      return;
    }
    if (fetchedForRef.current === bindingId) return;
    fetchedForRef.current = bindingId;
    suggestRules(
      { bindingId },
      {
        onSuccess: (resp) => {
          const suggestions = resp.data.suggestions ?? [];
          setState({ available: resp.data.available, reason: resp.data.reason, suggestions });
          setSelected(new Set(suggestions.map((_, i) => i)));
        },
        onError: (err) => {
          const reason = aiUnavailableReason(err);
          if (reason) onAiUnavailable(reason);
          toast.error(t("monitoredTables.suggestRulesFetchFailed"));
          setState({ available: false, reason: reason ?? undefined, suggestions: [] });
        },
      },
    );
  }, [open, bindingId, suggestRules, onAiUnavailable, t]);

  const toggle = (idx: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  };

  const handleAdd = async () => {
    if (!state) return;
    const chosen = state.suggestions.filter((_, i) => selected.has(i));
    if (chosen.length === 0) {
      toast.error(t("monitoredTables.suggestRulesNoneSelected"));
      return;
    }
    setApplying(true);
    let failures = 0;
    for (const suggestion of chosen) {
      try {
        await applyMutation.mutateAsync({
          bindingId,
          data: { rule_id: suggestion.rule_id, column_mapping: [suggestion.column_mapping] },
        });
      } catch {
        failures += 1;
      }
    }
    setApplying(false);
    const addedCount = chosen.length - failures;
    if (addedCount > 0) {
      toast.success(t("monitoredTables.suggestRulesAddedToast", { count: addedCount }));
      onApplied();
    }
    if (failures > 0) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
    }
    if (failures === 0) {
      onOpenChange(false);
    }
  };

  const loading = suggestMutation.isPending && state === null;

  return (
    <Dialog open={open} onOpenChange={(next) => !applying && onOpenChange(next)}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className={`h-4 w-4 ${AI_ICON_COLOR}`} />
            <span className={AI_TEXT_GRADIENT}>{t("monitoredTables.suggestRulesDialogTitle")}</span>
          </DialogTitle>
          <DialogDescription>{t("monitoredTables.suggestRulesDialogDescription")}</DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground">{t("monitoredTables.suggestRulesLoading")}</p>
          </div>
        ) : state && !state.available ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Sparkles className="h-8 w-8 text-muted-foreground/30" />
            <p className="text-sm font-medium text-muted-foreground">
              {t("monitoredTables.suggestRulesUnavailableTitle")}
            </p>
            {state.reason && <p className="text-xs text-muted-foreground/70 max-w-sm">{state.reason}</p>}
          </div>
        ) : state && state.suggestions.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
            <Sparkles className="h-8 w-8 text-muted-foreground/30" />
            <p className="text-sm font-medium text-muted-foreground">
              {t("monitoredTables.suggestRulesEmptyTitle")}
            </p>
            <p className="text-xs text-muted-foreground/70 max-w-sm">
              {t("monitoredTables.suggestRulesEmptyDescription")}
            </p>
          </div>
        ) : state ? (
          <div className="max-h-96 overflow-y-auto space-y-2">
            {state.suggestions.map((s, idx) => {
              const checked = selected.has(idx);
              return (
                <label
                  key={`${s.rule_id}-${idx}`}
                  className={`flex items-start gap-3 rounded-md border p-3 cursor-pointer transition-colors ${
                    checked ? AI_BANNER_BORDER : "hover:bg-muted/40"
                  }`}
                >
                  <Checkbox checked={checked} onCheckedChange={() => toggle(idx)} className="mt-0.5" />
                  <div className="min-w-0 flex-1 space-y-1.5">
                    <div className="flex items-center gap-2 flex-wrap">
                      <p className="text-sm font-medium">{s.rule_name || s.rule_id}</p>
                      {s.dimension && (
                        <TagBadge label={s.dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, s.dimension)} />
                      )}
                      {s.severity && (
                        <TagBadge label={s.severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, s.severity)} />
                      )}
                    </div>
                    {s.explanation && (
                      <p className="text-xs text-muted-foreground">{s.explanation}</p>
                    )}
                    <MappingChips columnMapping={s.column_mapping ? [s.column_mapping] : []} />
                  </div>
                </label>
              );
            })}
          </div>
        ) : null}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={applying}>
            {t("common.cancel")}
          </Button>
          {state && state.available && state.suggestions.length > 0 && (
            <Button onClick={handleAdd} disabled={applying || selected.size === 0} className={`gap-2 ${AI_BUTTON_BG}`}>
              {applying && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("monitoredTables.suggestRulesAddButton", { count: selected.size })}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
