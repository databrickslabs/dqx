// AiSuggestionDialog — AI-suggested registry rules for a monitored table,
// ported (structure/interactions) from dqlake's `bindings/AiSuggestionDialog.tsx`:
// a checkbox list of suggestions (dimension/severity tags, explanation,
// mapping chips) with a single "Add N" action that stages every checked
// suggestion onto the tab's LOCAL editor state (no network write — see
// `AddRulesDialog.tsx`'s header for why). Always renders (never blocks the
// tab) — a missing/misconfigured AI backend degrades to an "unavailable"
// empty state via `available`/`reason` on the response, matching DQX's AI
// kill-switch semantics (see `useAiAvailability`).

import { useEffect, useMemo, useRef, useState } from "react";
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
  useListRegistryRules,
  useSuggestRulesForTable,
  type RegistryRuleOut,
  type SuggestedRuleMappingOut,
} from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BANNER_BORDER, AI_BUTTON_BG, AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";
import { MappingChips } from "./MappingChips";
import { RESERVED_DIMENSION_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor, newStagedRow } from "./shared";

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
  /** Appends one locally-staged row per accepted suggestion. Pure
   *  local-state mutation — no network call. */
  onAdd: (rows: ReturnType<typeof newStagedRow>[]) => void;
  onApplied: () => void;
  onAiUnavailable: (reason: string) => void;
}

export function AiSuggestionDialog({
  open,
  onOpenChange,
  bindingId,
  labelDefinitions,
  onAdd,
  onApplied,
  onAiUnavailable,
}: AiSuggestionDialogProps) {
  const { t } = useTranslation();
  const suggestMutation = useSuggestRulesForTable();
  // Suggestions only carry `rule_id` — resolving it back to a full
  // `RegistryRuleOut` is what lets `newStagedRow` denormalize the rule's
  // name/dimension/severity tags onto the staged row the same way every
  // other staging path does.
  const { data: registryData } = useListRegistryRules({ status: "approved" });
  const ruleById = useMemo(() => {
    const map = new Map<string, RegistryRuleOut>();
    for (const r of registryData?.data ?? []) map.set(r.rule_id, r);
    return map;
  }, [registryData]);
  const [state, setState] = useState<SuggestRulesState | null>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
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

  const handleAdd = () => {
    if (!state) return;
    const chosen = state.suggestions.filter((_, i) => selected.has(i));
    if (chosen.length === 0) {
      toast.error(t("monitoredTables.suggestRulesNoneSelected"));
      return;
    }
    // A suggestion whose rule_id isn't in the (approved) registry snapshot
    // fetched above is dropped rather than staged with missing display
    // metadata — this can only happen if the rule was unpublished between
    // the suggestion call and Add, an edge case rare enough not to warrant
    // its own toast copy.
    const rows = chosen
      .map((suggestion) => {
        const rule = ruleById.get(suggestion.rule_id);
        if (!rule) return null;
        return newStagedRow(bindingId, rule, [suggestion.column_mapping]);
      })
      .filter((row): row is NonNullable<typeof row> => row !== null);
    if (rows.length === 0) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
      return;
    }
    onAdd(rows);
    toast.success(t("monitoredTables.suggestRulesAddedToast", { count: rows.length }));
    onApplied();
    if (rows.length < chosen.length) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
    }
    onOpenChange(false);
  };

  const loading = suggestMutation.isPending && state === null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
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
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t("common.cancel")}
          </Button>
          {state && state.available && state.suggestions.length > 0 && (
            <Button onClick={handleAdd} disabled={selected.size === 0} className={`gap-2 ${AI_BUTTON_BG}`}>
              {t("monitoredTables.suggestRulesAddButton", { count: selected.size })}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
