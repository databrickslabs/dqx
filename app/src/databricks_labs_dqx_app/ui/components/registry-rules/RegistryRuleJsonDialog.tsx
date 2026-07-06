import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
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
import { AlertCircle, Loader2 } from "lucide-react";
import {
  useCreateRegistryRule,
  useListCheckFunctions,
  useUpdateRegistryRule,
  type RegistryRuleOut,
} from "@/lib/api";
import { buildDqxCheckJson, parseDqxCheckJson } from "@/lib/registry-rule-conversion";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

interface RegistryRuleJsonDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  rule: RegistryRuleOut;
  /** True when the rule is a `draft` and the user can edit it in place. */
  canEdit: boolean;
  /** True when the rule is not a `draft` but the user can still clone it into a new editable draft. */
  canEditAsNewDraft: boolean;
  /** Called after a successful save (in-place update) or new-draft creation. */
  onSaved: (newRuleId?: string) => void;
}

/**
 * View/edit the rule's native DQX-compatible check JSON — the exact
 * `{ criticality, check: { function, arguments }, user_metadata, name?,
 * message_expr? }` dict shape `materializer.render_check` stamps into the
 * materialized `dq_quality_rules.check` row, derived from the rule's stored
 * `definition` and `user_metadata` (see `lib/registry-rule-conversion.ts`;
 * no separate stored copy — the `definition` already IS the persisted
 * structured form, per the Rules Registry design). `user_metadata` is
 * included so the JSON faithfully mirrors what flows downstream, and
 * round-trips: editing it here and saving persists back into the rule's
 * `user_metadata` (the same store the About-tab tag fields write to).
 *
 * Saving reuses the exact same endpoints/lifecycle the visual form uses:
 * a `draft` rule updates in place (`useUpdateRegistryRule`), any other
 * status clones into a new editable draft (`useCreateRegistryRule`) —
 * mirroring the "Edit as new draft" action on the detail page. SQL safety
 * (`is_sql_query_safe`) is enforced server-side on save; an unsafe or
 * malformed edit surfaces as an inline error and is never persisted.
 */
export function RegistryRuleJsonDialog({
  open,
  onOpenChange,
  rule,
  canEdit,
  canEditAsNewDraft,
  onSaved,
}: RegistryRuleJsonDialogProps) {
  const { t } = useTranslation();
  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);

  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const editable = canEdit || canEditAsNewDraft;

  useEffect(() => {
    if (!open) return;
    setText(JSON.stringify(buildDqxCheckJson(rule), null, 2));
    setError(null);
  }, [open, rule]);

  const updateMutation = useUpdateRegistryRule();
  const createMutation = useCreateRegistryRule();

  const handleSave = async () => {
    setError(null);
    let parsed;
    try {
      parsed = parseDqxCheckJson(text, rule.definition, rule.user_metadata, checkFunctions, t);
    } catch (err) {
      setError(err instanceof Error ? err.message : t("rulesRegistry.jsonParseError"));
      return;
    }
    setSaving(true);
    try {
      if (canEdit) {
        await updateMutation.mutateAsync({
          ruleId: rule.rule_id,
          data: {
            mode: parsed.mode,
            definition: parsed.definition,
            polarity: parsed.polarity,
            user_metadata: parsed.userMetadata,
            steward: rule.steward ?? null,
            author_kind: rule.author_kind ?? null,
          },
        });
        toast.success(t("rulesRegistry.toastUpdated"));
        onSaved(rule.rule_id);
      } else {
        const resp = await createMutation.mutateAsync({
          data: {
            mode: parsed.mode,
            definition: parsed.definition,
            polarity: parsed.polarity,
            user_metadata: parsed.userMetadata,
            steward: rule.steward ?? null,
            author_kind: rule.author_kind ?? "human",
          },
        });
        toast.success(t("rulesRegistry.toastDraftCopyCreated"));
        onSaved(resp.data.rule.rule_id);
      }
      onOpenChange(false);
    } catch (err) {
      setError(extractApiError(err, t("rulesRegistry.saveFailed")));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t("rulesRegistry.jsonDialogTitle")}</DialogTitle>
          <DialogDescription>
            {editable ? t("rulesRegistry.jsonDialogDescriptionEditable") : t("rulesRegistry.jsonDialogDescriptionReadOnly")}
          </DialogDescription>
        </DialogHeader>

        <Textarea
          className="font-mono text-xs min-h-[320px]"
          value={text}
          onChange={(e) => {
            setText(e.target.value);
            if (error) setError(null);
          }}
          readOnly={!editable}
          spellCheck={false}
        />

        {error && (
          <p className="flex items-start gap-1.5 text-xs text-destructive">
            <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
            {error}
          </p>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {editable ? t("common.cancel") : t("common.close")}
          </Button>
          {editable && (
            <Button onClick={handleSave} disabled={saving} className="gap-2">
              {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {canEdit ? t("rulesRegistry.jsonSaveButton") : t("rulesRegistry.jsonSaveAsNewDraftButton")}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
