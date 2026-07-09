import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { MessageSquare, Sparkles, Wand2, X, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useAiWriteSql, useAiImproveSql, useAiExplainSql, type RuleSlot } from "@/lib/api";
import { type AiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_GRADIENT_URL, AI_BANNER_BG, AI_BANNER_BORDER } from "@/lib/ai-style";
import { cn } from "@/lib/utils";

type Polarity = "pass" | "fail";

interface SqlAiAssistMenuProps {
  /** Current SQL predicate text in the editor. */
  predicate: string;
  /** Declared `{{slot}}` columns the AI may reference (forwarded as names). */
  slots: RuleSlot[];
  /** Replace the editor's predicate with AI-written/improved SQL. */
  onPredicateReplace: (next: string) => void;
  /** Sync the PASS/FAIL polarity switch when the AI infers one. */
  onPolarityChange: (polarity: Polarity) => void;
  /** Shared AI availability gate — the toolbar is hidden entirely when AI is unavailable. */
  aiAvailability: AiAvailability;
  disabled?: boolean;
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { status?: number; data?: { detail?: string } } };
  if (axErr?.response?.status === 429) return fallback;
  return axErr?.response?.data?.detail ?? fallback;
}

/**
 * SQL predicate AI assistants — write / improve / explain — ported from dqlake's
 * `AiAssistMenu`. A right-aligned toolbar sitting directly above the SQL editor
 * (matching dqlake's ImplementationTab layout): three plain outline buttons with
 * gradient-stroked icons (the house AI motif). "Write with AI" and "Improve" open
 * a description modal and replace the predicate inline; "Explain" fires immediately
 * and shows a non-destructive explanation panel below the toolbar (DQX validates
 * the predicate, so — unlike dqlake — the explanation is never written back into
 * the editor text where a stray DDL keyword in prose could trip the save guard).
 *
 * Every AI-written predicate is re-validated with `is_sql_query_safe` server-side
 * before it can reach the editor (AGENTS.md 11-SEC). The whole toolbar is hidden
 * when AI is unavailable (kill-switch off / unconfigured), consistent with the
 * Build-with-AI banner; a mid-use 503 hides it via `reportUnavailable`.
 */
export function SqlAiAssistMenu({
  predicate,
  slots,
  onPredicateReplace,
  onPolarityChange,
  aiAvailability,
  disabled,
}: SqlAiAssistMenuProps) {
  const { t } = useTranslation();
  const writeMutation = useAiWriteSql();
  const improveMutation = useAiImproveSql();
  const explainMutation = useAiExplainSql();

  // Which description modal is open (write | improve), or null.
  const [modal, setModal] = useState<"write" | "improve" | null>(null);
  const [modalText, setModalText] = useState("");
  const [explanation, setExplanation] = useState<string | null>(null);

  if (!aiAvailability.available || disabled) return null;

  const columns = slots.map((s) => s.name);
  const busy = writeMutation.isPending || improveMutation.isPending || explainMutation.isPending;
  const hasPredicate = predicate.trim().length > 0;

  const handleAiError = (err: unknown, fallbackKey: string) => {
    const reason = aiUnavailableReason(err);
    if (reason) {
      aiAvailability.reportUnavailable(reason);
      return;
    }
    const axErr = err as { response?: { status?: number } };
    toast.error(
      axErr?.response?.status === 429 ? t("rulesRegistry.aiRateLimited") : extractApiError(err, t(fallbackKey)),
      { duration: 6000 },
    );
  };

  const applyResult = (result: { predicate: string; polarity?: string | null }, toastKey: string) => {
    onPredicateReplace(result.predicate);
    if (result.polarity === "pass" || result.polarity === "fail") onPolarityChange(result.polarity);
    setExplanation(null);
    toast.success(t(toastKey));
  };

  const submitModal = async () => {
    const text = modalText.trim();
    if (!text) return;
    try {
      if (modal === "write") {
        const resp = await writeMutation.mutateAsync({
          data: { description: text, columns: columns.length > 0 ? columns : null },
        });
        applyResult(resp.data, "rulesRegistry.sqlAiWritten");
      } else if (modal === "improve") {
        const resp = await improveMutation.mutateAsync({
          data: { predicate, instruction: text, columns: columns.length > 0 ? columns : null },
        });
        applyResult(resp.data, "rulesRegistry.sqlAiImproved");
      }
      setModal(null);
      setModalText("");
    } catch (err) {
      handleAiError(err, "rulesRegistry.sqlAiFailed");
    }
  };

  const handleExplain = async () => {
    if (!hasPredicate) return;
    try {
      const resp = await explainMutation.mutateAsync({ data: { predicate } });
      setExplanation(resp.data.explanation);
    } catch (err) {
      handleAiError(err, "rulesRegistry.sqlAiFailed");
    }
  };

  const openModal = (which: "write" | "improve") => {
    setModalText("");
    setModal(which);
  };

  const modalTitle = modal === "improve" ? t("rulesRegistry.sqlAiImproveTitle") : t("rulesRegistry.sqlAiWriteTitle");
  const modalDescription =
    modal === "improve" ? t("rulesRegistry.sqlAiImproveDescription") : t("rulesRegistry.sqlAiWriteDescription");
  const modalPlaceholder =
    modal === "improve" ? t("rulesRegistry.sqlAiImprovePlaceholder") : t("rulesRegistry.sqlAiWritePlaceholder");

  return (
    <>
      <div className="flex flex-wrap items-center justify-end gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 gap-1.5 text-xs"
          onClick={() => openModal("write")}
          disabled={busy}
        >
          <Sparkles className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
          {t("rulesRegistry.sqlAiWrite")}
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 gap-1.5 text-xs"
          onClick={handleExplain}
          disabled={busy || !hasPredicate}
        >
          {explainMutation.isPending ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <MessageSquare className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
          )}
          {t("rulesRegistry.sqlAiExplain")}
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 gap-1.5 text-xs"
          onClick={() => openModal("improve")}
          disabled={busy || !hasPredicate}
        >
          <Wand2 className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
          {t("rulesRegistry.sqlAiImprove")}
        </Button>
      </div>

      {explanation !== null && (
        <div
          className={cn(
            "flex items-start gap-2 rounded-md px-3 py-2 text-xs",
            AI_BANNER_BG,
            AI_BANNER_BORDER,
          )}
        >
          <Sparkles className="mt-0.5 h-3.5 w-3.5 shrink-0" stroke={AI_GRADIENT_URL} />
          <p className="flex-1 leading-relaxed">{explanation}</p>
          <button
            type="button"
            aria-label={t("rulesRegistry.sqlAiExplainDismiss")}
            onClick={() => setExplanation(null)}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      <Dialog open={modal !== null} onOpenChange={(next) => !next && setModal(null)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Sparkles className="h-4 w-4" stroke={AI_GRADIENT_URL} />
              {modalTitle}
            </DialogTitle>
            <DialogDescription>{modalDescription}</DialogDescription>
          </DialogHeader>
          <Textarea
            autoFocus
            rows={4}
            value={modalText}
            onChange={(e) => setModalText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
                e.preventDefault();
                void submitModal();
              }
            }}
            placeholder={modalPlaceholder}
            className="text-xs"
          />
          <DialogFooter>
            <Button variant="outline" onClick={() => setModal(null)} disabled={busy}>
              {t("common.cancel")}
            </Button>
            <Button onClick={() => void submitModal()} disabled={busy || !modalText.trim()} className="gap-2">
              {busy && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("rulesRegistry.sqlAiSubmit")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
