import { useState, useRef, useMemo } from "react";
import { useTranslation } from "react-i18next";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import {
  Upload,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Save,
  Send,
  ExternalLink,
} from "lucide-react";
import { isAxiosError } from "axios";
import { toast } from "sonner";
import { useLabelDefinitions, useValidateChecks } from "@/lib/api-custom";
import { useListCheckFunctions } from "@/lib/api";
import { LabelsBadges } from "@/components/Labels";
import {
  colorFor,
  type LabelColorDefinition,
  RESERVED_SEVERITY_KEY,
  SeverityBadge,
} from "@/components/RegistryRuleBadges";
import { defaultSeverityForCriticality } from "@/lib/registry-rule-conversion";
import { getUserMetadata } from "@/lib/format-utils";
import {
  importChecksAsRegistryDrafts,
  parseImportYamlText,
  SQL_CHECK_PREFIX,
} from "@/lib/import-registry-rules";
import { IMPORT_EXAMPLES, ImportExampleLinks } from "@/components/imports/ImportExampleLinks";

const CHECKS_DOCS_URL = "https://databrickslabs.github.io/dqx/docs/guide/quality_checks_definition/";

// Rules imports only create reusable rule templates from DQX YAML. ODCS
// contracts, which also register tables, are imported from /monitored-tables/import.
export function ImportRulesWorkspace({ onDone }: { onDone: () => void }) {
  const { t } = useTranslation();

  return (
    <>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("rulesImport.title")}</h1>
          <p className="mt-1 text-sm text-muted-foreground">{t("rulesImport.scopeNoteRules")}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <a
            href={CHECKS_DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex h-8 items-center gap-1.5 rounded-md px-2 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            {t("rulesFromContract.viewDocs")}
            <ExternalLink className="h-3 w-3" />
          </a>
          <ImportExampleLinks examplePath={IMPORT_EXAMPLES.rulesYaml} />
        </div>
      </div>

      <YamlImportCard onDone={onDone} />
    </>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// YAML Import
// ──────────────────────────────────────────────────────────────────────────────

/** The reason a save request failed: the API's ``detail`` when there is one,
 *  otherwise the transport error (network, timeout, proxy status). */
function saveErrorDetail(err: unknown): string | null {
  if (isAxiosError(err)) {
    const detail = (err.response?.data as { detail?: unknown } | undefined)?.detail;
    if (typeof detail === "string" && detail) return detail;
    if (err.response) return `HTTP ${err.response.status}`;
    return err.message || null;
  }
  return err instanceof Error ? err.message : null;
}

function YamlImportCard({ onDone }: { onDone: () => void }) {
  const { t } = useTranslation();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [yamlText, setYamlText] = useState("");
  const [parsedChecks, setParsedChecks] = useState<Record<string, unknown>[] | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [parseHint, setParseHint] = useState<string | null>(null);
  const [validationResult, setValidationResult] = useState<{ valid: boolean; errors: string[] } | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const validateMutation = useValidateChecks();
  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);
  const { data: labelDefsData } = useLabelDefinitions();
  const severityDefs = (labelDefsData?.definitions ?? []) as LabelColorDefinition[];

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target;
    const file = input.files?.[0];
    input.value = "";
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target?.result as string;
      setYamlText(text);
      parseYaml(text);
    };
    reader.readAsText(file);
  };

  const parseYaml = (text: string) => {
    setValidationResult(null);
    const result = parseImportYamlText(text, {
      yamlMustBeList: t("rulesImport.yamlMustBeList"),
      commentsOnly: t("rulesImport.yamlCommentsOnly"),
      emptyList: t("rulesImport.yamlEmptyList"),
      invalidEntry: t("rulesImport.yamlInvalidEntry"),
    });
    setParsedChecks(result.checks);
    setParseError(result.error);
    setParseHint(result.hint);
  };

  const syncYamlFromTextarea = () => {
    const value = textareaRef.current?.value ?? yamlText;
    if (value !== yamlText) {
      setYamlText(value);
    }
    parseYaml(value);
  };

  const handleValidate = async () => {
    if (!parsedChecks) return;
    try {
      const resp = await validateMutation.mutateAsync({ data: { checks: parsedChecks } });
      setValidationResult(resp.data);
      if (resp.data.valid) {
        toast.success(t("rulesImport.allValid"));
      } else {
        toast.error(t("rulesImport.validationFoundErrors", { count: resp.data.errors.length }));
      }
    } catch {
      toast.error(t("rulesImport.failedValidate"));
    }
  };

  const importChecks = async (alsoSubmit: boolean) => {
    if (!parsedChecks) return;
    if (checkFunctions.length === 0) {
      toast.error(t("rulesImport.functionsNotLoaded"));
      return;
    }
    if (alsoSubmit) setIsSubmitting(true);
    else setIsSaving(true);
    try {
      const toastId = toast.loading(
        alsoSubmit
          ? t("rulesImport.importingAndSubmitting", { count: parsedChecks.length })
          : t("rulesImport.importing", { count: parsedChecks.length }),
      );
      const result = await importChecksAsRegistryDrafts({
        checks: parsedChecks,
        checkFunctions,
        t,
        alsoSubmit,
      });
      toast.dismiss(toastId);
      if (result.saved > 0) {
        toast.success(t("rulesImport.savedDrafts", { count: result.saved }));
      }
      if (result.reused > 0) {
        toast.success(t("rulesImport.reusedExisting", { count: result.reused }));
      }
      if (alsoSubmit && result.submitted > 0) {
        toast.success(t("rulesImport.submittedReview", { count: result.submitted }));
      }
      if (result.submitFailed > 0) {
        toast.error(t("rulesImport.submitFailed", { count: result.submitFailed }));
      }
      if (result.failed > 0) {
        const detail = result.errors[0];
        toast.error(
          detail
            ? `${t("rulesImport.importPartialFailed", { count: result.failed })}: ${detail}`
            : t("rulesImport.importPartialFailed", { count: result.failed }),
        );
      }
      if (result.saved === 0 && result.reused === 0 && result.failed === 0) {
        toast.error(t("rulesImport.failedSaveRules"));
      }
      if (result.saved > 0 || result.reused > 0) onDone();
    } catch (err) {
      toast.dismiss();
      const detail = saveErrorDetail(err);
      toast.error(detail ? `${t("rulesImport.failedSaveRules")}: ${detail}` : t("rulesImport.failedSaveRules"));
    } finally {
      setIsSaving(false);
      setIsSubmitting(false);
    }
  };

  const busy = isSaving || isSubmitting;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <Upload className="h-4 w-4" />
          {t("rulesImport.importFromYaml")}
        </CardTitle>
        <CardDescription>
          {t("rulesImport.importDescription")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-2">
          <Label>{t("rulesImport.uploadYamlFile")}</Label>
          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              size="sm"
              onClick={() => fileInputRef.current?.click()}
              className="gap-2"
            >
              <Upload className="h-4 w-4" />
              {t("rulesImport.chooseFile")}
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml"
              onChange={handleFileUpload}
              className="hidden"
            />
            <span className="text-xs text-muted-foreground">{t("rulesImport.orPasteBelow")}</span>
          </div>
        </div>

        <div className="space-y-2">
          <Label>{t("rulesImport.yamlContent")}</Label>
          <Textarea
            ref={textareaRef}
            value={yamlText}
            onChange={(e) => {
              setYamlText(e.target.value);
              if (e.target.value.trim()) parseYaml(e.target.value);
              else {
                setParsedChecks(null);
                setParseError(null);
                setParseHint(null);
                setValidationResult(null);
              }
            }}
            onPaste={() => {
              // Some browser extensions update the textarea after onChange;
              // re-parse on the next frame so paste reliably enables actions.
              requestAnimationFrame(syncYamlFromTextarea);
            }}
            onBlur={syncYamlFromTextarea}
            placeholder={`- criticality: error\n  check:\n    function: is_not_null\n    arguments:\n      column: id`}
            className="font-mono text-xs min-h-[200px]"
          />
        </div>

        {parseError && (
          <div className="flex items-start gap-2 p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
            <XCircle className="h-4 w-4 mt-0.5 shrink-0" />
            <pre className="whitespace-pre-wrap font-mono text-xs">{parseError}</pre>
          </div>
        )}

        {parseHint && !parseError && (
          <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-950/30 text-amber-700 dark:text-amber-400 text-sm">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            <p className="text-xs">{parseHint}</p>
          </div>
        )}

        {parsedChecks && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-green-50 dark:bg-green-950/30 text-green-700 dark:text-green-400 text-sm">
            <CheckCircle2 className="h-4 w-4 shrink-0" />
            {t("rulesImport.loadedChecks", { count: parsedChecks.length })}
          </div>
        )}

        {parsedChecks && parsedChecks.length > 0 && (
          <div className="space-y-2">
            <Label>{t("rulesImport.preview")}</Label>
            <div className="border rounded-lg overflow-auto max-h-[200px]">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerHash")}</th>
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerName")}</th>
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerFunction")}</th>
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerArguments")}</th>
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerSeverity")}</th>
                    <th className="text-left p-2 font-medium">{t("rulesImport.headerLabels")}</th>
                  </tr>
                </thead>
                <tbody>
                  {parsedChecks.map((c, i) => {
                    const entry = c ?? {};
                    const check = (entry.check as Record<string, unknown>) ?? entry;
                    const fnRaw = String(check.function ?? "—");
                    const isSqlCheck = fnRaw.startsWith(SQL_CHECK_PREFIX);
                    const fn = isSqlCheck ? "sql_query" : fnRaw;
                    const argsObj = (check.arguments as Record<string, unknown>) ?? {};
                    const args = check.arguments ? JSON.stringify(check.arguments) : "—";
                    const labels = getUserMetadata(entry as Record<string, unknown>);
                    const userMeta = (entry.user_metadata as Record<string, unknown>) ?? {};
                    const severity =
                      (typeof userMeta.severity === "string" && userMeta.severity) ||
                      defaultSeverityForCriticality(entry.criticality ?? check.criticality);
                    const name =
                      (typeof userMeta.name === "string" && userMeta.name) ||
                      (typeof entry.name === "string" && entry.name) ||
                      (typeof argsObj.name === "string" && (argsObj.name as string)) ||
                      (isSqlCheck ? fnRaw.slice(SQL_CHECK_PREFIX.length) : null);

                    return (
                      <tr key={i} className="border-b last:border-b-0">
                        <td className="p-2 text-muted-foreground">{i + 1}</td>
                        <td className="p-2 font-mono max-w-[200px] truncate">
                          {name ? (
                            name
                          ) : (
                            <span className="italic text-muted-foreground/60">—</span>
                          )}
                        </td>
                        <td className="p-2 font-mono">{fn}</td>
                        <td className="p-2 font-mono text-muted-foreground max-w-[280px] truncate">{args}</td>
                        <td className="p-2">
                          <SeverityBadge
                            severity={severity}
                            color={colorFor(severityDefs, RESERVED_SEVERITY_KEY, severity)}
                          />
                        </td>
                        <td className="p-2">
                          {Object.keys(labels).length === 0 ? (
                            <span className="text-[10px] italic text-muted-foreground/60">—</span>
                          ) : (
                            <LabelsBadges labels={labels} max={3} size="sm" />
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {validationResult && !validationResult.valid && (
          <div className="space-y-1 p-3 rounded-lg bg-amber-50 dark:bg-amber-950/30">
            <div className="flex items-center gap-2 text-amber-700 dark:text-amber-400 text-sm font-medium">
              <AlertTriangle className="h-4 w-4" />
              {t("rulesImport.validationErrors")}
            </div>
            <ul className="text-xs text-amber-600 dark:text-amber-400/80 list-disc pl-5 space-y-0.5">
              {validationResult.errors.map((err, i) => (
                <li key={i}>{err}</li>
              ))}
            </ul>
          </div>
        )}

        <Separator />

        <p className="text-xs text-muted-foreground">{t("rulesImport.registryHint")}</p>

        <div className="flex items-center gap-3 flex-wrap">
          <Button
            variant="outline"
            onClick={handleValidate}
            disabled={!parsedChecks || validateMutation.isPending || busy}
            className="gap-2"
            size="sm"
          >
            {validateMutation.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <CheckCircle2 className="h-4 w-4" />
            )}
            {t("rulesImport.validate")}
          </Button>

          <div className="ml-auto flex items-center gap-2">
            <Button
              variant="outline"
              onClick={() => importChecks(false)}
              disabled={!parsedChecks || busy}
              className="gap-2"
              size="sm"
            >
              {isSaving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Save className="h-4 w-4" />
              )}
              {t("rulesImport.saveAsDrafts")}
            </Button>
            <Button
              onClick={() => importChecks(true)}
              disabled={!parsedChecks || busy}
              className="gap-2"
              size="sm"
            >
              {isSubmitting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Send className="h-4 w-4" />
              )}
              {t("rulesImport.submitForReview")}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
