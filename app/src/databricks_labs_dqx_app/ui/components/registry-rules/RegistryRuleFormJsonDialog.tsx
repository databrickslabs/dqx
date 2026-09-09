import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { AlertCircle, Sparkles } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useAIAssistant } from "@/components/AIAssistantProvider";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import {
  parseDqxCheckJson,
  type ParsedCheckDefinition,
} from "@/lib/registry-rule-conversion";
import type { CheckFunctionDef as ApiCheckFunctionDef, RuleDefinition } from "@/lib/api";

interface RegistryRuleFormJsonDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** The native DQX check JSON derived from the in-progress form state. */
  checkJson: Record<string, unknown>;
  /** Current form definition/metadata/mode — the fallbacks `parseDqxCheckJson` needs. */
  currentDefinition: RuleDefinition;
  currentUserMetadata: Record<string, unknown>;
  currentMode: "dqx_native" | "lowcode" | "sql";
  checkFunctions: ApiCheckFunctionDef[];
  /** Apply the parsed JSON straight back into the form state. */
  onApply: (parsed: ParsedCheckDefinition) => void;
  /** Whether the "or generate from a description" affordance is offered. */
  aiAvailable: boolean;
  /** Dialog description copy — differs slightly for the create vs. edit-in-place caller. */
  description: string;
}

/**
 * "As JSON" surface for the rule form (item 11 originally; extended to
 * editing an existing rule in P24-C item 11). Unlike
 * {@link RegistryRuleJsonDialog} (which is now only used for the read-only
 * view and the "save as new draft" clone flow, both of which persist via the
 * CRUD endpoints), this operates purely on in-progress form state: the JSON
 * is derived from the current form via `buildDqxCheckJson`, and applying an
 * edit round-trips back INTO the form state via `parseDqxCheckJson` + the
 * caller's `onApply` — nothing is persisted until the user saves the form
 * itself (the normal Save/Submit buttons).
 *
 * Known caveat (carried over from `parseDqxCheckJson`): editing the raw JSON of
 * a `dqx_native` rule re-derives canonical `{{column_N}}` slot names from the
 * check function's signature, discarding any author-renamed slots — a registry
 * rule's native arguments are always canonical placeholders. SQL safety
 * (`is_sql_query_safe`) is still enforced server-side when the form is saved.
 *
 * Also hosts the relocated "generate rules" entry (item 11): the AI
 * generate-checks assistant, moved out of the global app header, appears here as
 * the natural "or generate from a description" affordance.
 */
export function RegistryRuleFormJsonDialog({
  open,
  onOpenChange,
  checkJson,
  currentDefinition,
  currentUserMetadata,
  currentMode,
  checkFunctions,
  onApply,
  aiAvailable,
  description,
}: RegistryRuleFormJsonDialogProps) {
  const { t } = useTranslation();
  const { setOpen: setAiAssistantOpen } = useAIAssistant();
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setText(JSON.stringify(checkJson, null, 2));
    setError(null);
    // Only re-seed when the dialog transitions to open — later `checkJson`
    // identity changes (parent re-renders) must not clobber the user's edits.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const handleApply = () => {
    let parsed: ParsedCheckDefinition;
    try {
      parsed = parseDqxCheckJson(text, currentDefinition, currentUserMetadata, checkFunctions, t, currentMode);
    } catch (err) {
      setError(err instanceof Error ? err.message : t("rulesRegistry.jsonParseError"));
      return;
    }
    onApply(parsed);
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t("rulesRegistry.jsonDialogTitle")}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>

        <Textarea
          className="font-mono text-xs min-h-[320px]"
          value={text}
          onChange={(e) => {
            setText(e.target.value);
            if (error) setError(null);
          }}
          spellCheck={false}
        />

        {error && (
          <p className="flex items-start gap-1.5 text-xs text-destructive">
            <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
            {error}
          </p>
        )}

        {aiAvailable && (
          <button
            type="button"
            onClick={() => setAiAssistantOpen(true)}
            className="flex items-center gap-1.5 self-start text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            <Sparkles className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
            {t("rulesRegistry.jsonGenerateFromDescription")}
          </button>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t("common.cancel")}
          </Button>
          <Button onClick={handleApply}>{t("rulesRegistry.jsonApplyToForm")}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
