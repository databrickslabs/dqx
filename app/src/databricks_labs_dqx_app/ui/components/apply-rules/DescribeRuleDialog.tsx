/**
 * DescribeRuleDialog — NL "describe a rule" flow on a monitored table's
 * Apply Rules tab.
 *
 * 1. Steward describes the check in plain language.
 * 2. POST …/match-rules → if published registry rules match, confirm → stage
 *    (same local staging as Suggest rules; user still hits Save).
 * 3. If nothing matches (or steward picks "Create new instead") →
 *    POST /ai/generate-rule → confirm proposal → sessionStorage + navigate
 *    to /registry-rules/new with returnTo back to this binding's Apply Rules.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "@tanstack/react-router";
import { toast } from "sonner";
import { ArrowRight, Loader2, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  aiGenerateRule,
  matchRulesForTable,
  useListRegistryRules,
  type AiGenerateRuleOut,
  type MatchedRuleOut,
  type RegistryRuleOut,
} from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { aiUnavailableReason } from "@/hooks/use-ai-availability";
import { useDefaultAutoUpgrade } from "@/hooks/use-default-auto-upgrade";
import { AI_BUTTON_BG, AI_GRADIENT_URL, AI_TEXT_GRADIENT } from "@/lib/ai-style";
import { SeverityBadge } from "@/components/RegistryRuleBadges";
import { cn } from "@/lib/utils";
import {
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  TagBadge,
  colorFor,
  newStagedRow,
} from "./shared";
import { storeDescribeRuleProposal } from "./describe-rule-handoff";

type Phase = "prompt" | "matching" | "matches" | "generating" | "proposal" | "unavailable";

interface DescribeRuleDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  tableFqn: string;
  columns: string[];
  labelDefinitions: LabelDefinition[];
  onAdd: (rows: ReturnType<typeof newStagedRow>[]) => void;
  onApplied: () => void;
  reportUnavailable: (reason: string) => void;
}

export function DescribeRuleDialog({
  open,
  onOpenChange,
  bindingId,
  tableFqn,
  columns,
  labelDefinitions,
  onAdd,
  onApplied,
  reportUnavailable,
}: DescribeRuleDialogProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const defaultAutoUpgrade = useDefaultAutoUpgrade();
  const titleRef = useRef<HTMLHeadingElement>(null);

  const [prompt, setPrompt] = useState("");
  const [phase, setPhase] = useState<Phase>("prompt");
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [matches, setMatches] = useState<MatchedRuleOut[]>([]);
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);
  const [proposal, setProposal] = useState<AiGenerateRuleOut | null>(null);
  const [matchReason, setMatchReason] = useState<string | null>(null);

  const { data: registryData } = useListRegistryRules({ status: "approved" });
  const ruleById = useMemo(() => {
    const map = new Map<string, RegistryRuleOut>();
    for (const r of registryData?.data ?? []) map.set(r.rule_id, r);
    return map;
  }, [registryData]);

  // Reset when the dialog closes so the next open starts clean.
  useEffect(() => {
    if (!open) {
      setPrompt("");
      setPhase("prompt");
      setUnavailableReason(null);
      setMatches([]);
      setSelectedRuleId(null);
      setProposal(null);
      setMatchReason(null);
    }
  }, [open]);

  const busy = phase === "matching" || phase === "generating";

  const handleUnavailable = (reason: string) => {
    setUnavailableReason(reason);
    setPhase("unavailable");
    reportUnavailable(reason);
  };

  const runGenerate = async (description: string) => {
    setPhase("generating");
    try {
      const resp = await aiGenerateRule({
        description,
        table_fqn: tableFqn,
        columns: columns.length > 0 ? columns : null,
      });
      setProposal(resp.data);
      setPhase("proposal");
    } catch (err) {
      const unavailable = aiUnavailableReason(err);
      if (unavailable) {
        handleUnavailable(unavailable);
        toast.error(unavailable);
      } else {
        toast.error(t("monitoredTables.describeRuleGenerateFailed"));
        setPhase("prompt");
      }
    }
  };

  const handleSearch = async () => {
    const query = prompt.trim();
    if (!query) return;
    setPhase("matching");
    setMatches([]);
    setSelectedRuleId(null);
    setProposal(null);
    setMatchReason(null);
    try {
      const resp = await matchRulesForTable(bindingId, { query });
      const data = resp.data;
      if (!data.available) {
        handleUnavailable(data.reason || t("monitoredTables.describeRuleUnavailableFallback"));
        return;
      }
      const hits = data.matches ?? [];
      const stageable = hits.filter((m) => m.column_mapping && Object.keys(m.column_mapping).length > 0);
      if (stageable.length > 0) {
        setMatches(hits);
        setSelectedRuleId(stageable[0]?.rule_id ?? hits[0]?.rule_id ?? null);
        setMatchReason(data.reason || null);
        setPhase("matches");
        return;
      }
      // No stageable match → fall through to generate a new proposal.
      await runGenerate(query);
    } catch (err) {
      const unavailable = aiUnavailableReason(err);
      if (unavailable) {
        handleUnavailable(unavailable);
        toast.error(unavailable);
      } else {
        toast.error(t("monitoredTables.describeRuleMatchFailed"));
        setPhase("prompt");
      }
    }
  };

  const handleStageSelected = () => {
    const match = matches.find((m) => m.rule_id === selectedRuleId);
    if (!match?.column_mapping) {
      toast.error(t("monitoredTables.describeRuleNeedsMapping"));
      return;
    }
    const rule = ruleById.get(match.rule_id);
    if (!rule) {
      toast.error(t("monitoredTables.describeRuleAddFailed"));
      return;
    }
    const row = newStagedRow(bindingId, rule, [match.column_mapping], defaultAutoUpgrade);
    onAdd([row]);
    toast.success(t("monitoredTables.describeRuleAddedToast"));
    onApplied();
    onOpenChange(false);
  };

  const handleCreateInstead = async () => {
    const query = prompt.trim();
    if (!query) return;
    await runGenerate(query);
  };

  const handleGoCreate = () => {
    if (!proposal) return;
    storeDescribeRuleProposal(proposal);
    const returnTo = `/monitored-tables/${bindingId}?tab=apply-rules`;
    onOpenChange(false);
    void navigate({
      to: "/registry-rules/new",
      search: { tab: "implementation", returnTo },
    });
  };

  const dimColor = (dim?: string | null) =>
    dim ? colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dim) : undefined;

  const selectedMatch = matches.find((m) => m.rule_id === selectedRuleId) ?? null;
  const canStage = Boolean(selectedMatch?.column_mapping);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="sm:max-w-xl max-h-[85vh] flex flex-col"
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          titleRef.current?.focus();
        }}
      >
        <DialogHeader>
          <DialogTitle
            ref={titleRef}
            tabIndex={-1}
            className="flex items-center gap-2 outline-none"
          >
            <Sparkles className="h-4 w-4" stroke={AI_GRADIENT_URL} />
            <span className={cn(AI_TEXT_GRADIENT, "leading-normal pb-0.5")}>
              {t("monitoredTables.describeRuleDialogTitle")}
            </span>
          </DialogTitle>
          <DialogDescription>{t("monitoredTables.describeRuleDialogDescription")}</DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 py-1">
          {(phase === "prompt" || phase === "matching") && (
            <div className="space-y-2">
              <Label htmlFor="describe-rule-prompt">{t("monitoredTables.describeRulePromptLabel")}</Label>
              <Textarea
                id="describe-rule-prompt"
                value={prompt}
                onChange={(e) => setPrompt(e.target.value)}
                placeholder={t("monitoredTables.describeRulePlaceholder")}
                className="min-h-[96px] text-sm"
                disabled={busy}
              />
            </div>
          )}

          {phase === "matching" && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" />
              {t("monitoredTables.describeRuleMatching")}
            </div>
          )}

          {phase === "unavailable" && (
            <div className="rounded-lg border border-dashed p-6 text-center space-y-2">
              <p className="text-sm font-medium">{t("monitoredTables.describeRuleUnavailableTitle")}</p>
              <p className="text-xs text-muted-foreground">
                {unavailableReason || t("monitoredTables.describeRuleUnavailableFallback")}
              </p>
            </div>
          )}

          {phase === "matches" && (
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">{t("monitoredTables.describeRuleMatchesIntro")}</p>
              {matchReason && (
                <p className="text-xs text-amber-700 dark:text-amber-400">{matchReason}</p>
              )}
              <div className="space-y-2">
                {matches.map((m) => {
                  const selected = m.rule_id === selectedRuleId;
                  const stageable = Boolean(m.column_mapping);
                  return (
                    <button
                      key={m.rule_id}
                      type="button"
                      onClick={() => setSelectedRuleId(m.rule_id)}
                      className={cn(
                        "w-full text-left rounded-lg border p-3 transition-colors",
                        selected ? "border-primary bg-primary/5" : "hover:bg-muted/40",
                        !stageable && "opacity-70",
                      )}
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0 space-y-1">
                          <div className="font-medium text-sm truncate">
                            {m.rule_name || m.rule_id}
                          </div>
                          <div className="flex flex-wrap items-center gap-1.5">
                            {m.dimension && (
                              <TagBadge label={m.dimension} color={dimColor(m.dimension)} />
                            )}
                            {m.severity && (
                              <SeverityBadge
                                severity={m.severity}
                                color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, m.severity)}
                              />
                            )}
                            <span className="text-[10px] text-muted-foreground">
                              {t("monitoredTables.describeRuleScore", {
                                score: Math.round(m.score * 100),
                              })}
                            </span>
                          </div>
                          {m.explanation && (
                            <p className="text-xs text-muted-foreground line-clamp-2">{m.explanation}</p>
                          )}
                          {m.column_mapping ? (
                            <p className="text-[11px] font-mono text-muted-foreground">
                              {Object.entries(m.column_mapping)
                                .map(([slot, col]) => `${slot} → ${col}`)
                                .join(", ")}
                            </p>
                          ) : (
                            <p className="text-[11px] text-amber-700 dark:text-amber-400">
                              {t("monitoredTables.describeRuleNeedsMapping")}
                            </p>
                          )}
                        </div>
                        {selected && <Sparkles className="h-3.5 w-3.5 shrink-0 mt-0.5" stroke={AI_GRADIENT_URL} />}
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {phase === "generating" && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" />
              {t("monitoredTables.describeRuleGenerating")}
            </div>
          )}

          {phase === "proposal" && proposal && (
            <div className="space-y-3 rounded-lg border p-3">
              <p className="text-sm text-muted-foreground">{t("monitoredTables.describeRuleProposalIntro")}</p>
              <div className="space-y-1">
                <div className="font-medium text-sm">{proposal.name}</div>
                {proposal.description && (
                  <p className="text-xs text-muted-foreground whitespace-pre-wrap">{proposal.description}</p>
                )}
                <div className="flex flex-wrap gap-1.5 pt-1">
                  <span className="text-[10px] uppercase tracking-wide text-muted-foreground border rounded px-1.5 py-0.5">
                    {proposal.mode}
                  </span>
                  {proposal.dimension && (
                    <TagBadge label={proposal.dimension} color={dimColor(proposal.dimension)} />
                  )}
                  {proposal.severity && (
                    <SeverityBadge
                      severity={proposal.severity}
                      color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, proposal.severity)}
                    />
                  )}
                </div>
              </div>
            </div>
          )}
        </div>

        <DialogFooter className="gap-2 sm:gap-2 flex-wrap">
          {phase === "prompt" && (
            <Button
              size="sm"
              className={cn("gap-2", AI_BUTTON_BG)}
              disabled={!prompt.trim() || busy}
              onClick={() => void handleSearch()}
            >
              <Sparkles className="h-3.5 w-3.5" />
              {t("monitoredTables.describeRuleSearchButton")}
            </Button>
          )}

          {phase === "matches" && (
            <>
              <Button
                size="sm"
                variant="outline"
                disabled={busy}
                onClick={() => void handleCreateInstead()}
              >
                {t("monitoredTables.describeRuleCreateInstead")}
              </Button>
              <Button
                size="sm"
                className={cn("gap-2", AI_BUTTON_BG)}
                disabled={!canStage || busy}
                onClick={handleStageSelected}
              >
                {t("monitoredTables.describeRuleAddExisting")}
              </Button>
            </>
          )}

          {phase === "proposal" && (
            <Button size="sm" className={cn("gap-2", AI_BUTTON_BG)} onClick={handleGoCreate}>
              {t("monitoredTables.describeRuleCreateButton")}
              <ArrowRight className="h-3.5 w-3.5" />
            </Button>
          )}

          {(phase === "matching" || phase === "generating") && (
            <Button size="sm" disabled className="gap-2">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              {phase === "matching"
                ? t("monitoredTables.describeRuleMatching")
                : t("monitoredTables.describeRuleGenerating")}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
