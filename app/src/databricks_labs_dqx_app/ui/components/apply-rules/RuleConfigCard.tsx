// RuleConfigCard — one applied registry rule's card in the by-rule lens.
// Shows the rule's slot->column mapping (via MappingChips), pinned version,
// severity override, and (when editable) controls to change pin/severity or
// remove the application. All controls STAGE changes on the monitored-table
// binding — they never touch the live checks until the table is published.

import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Loader2, Trash2 } from "lucide-react";
import type { AppliedRuleOut, RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { MappingChips } from "./MappingChips";
import { RESERVED_DIMENSION_KEY, RESERVED_SEVERITY_KEY, TagBadge, colorFor } from "./shared";

interface RuleConfigCardProps {
  rule: AppliedRuleOut;
  registryRule: RegistryRuleOut | undefined;
  labelDefinitions: LabelDefinition[];
  severityValues: string[];
  canEdit: boolean;
  busy: boolean;
  onPinChange: (value: string) => void;
  onSeverityChange: (value: string) => void;
  onRemove: () => void;
}

export function RuleConfigCard({
  rule,
  registryRule,
  labelDefinitions,
  severityValues,
  canEdit,
  busy,
  onPinChange,
  onSeverityChange,
  onRemove,
}: RuleConfigCardProps) {
  const { t } = useTranslation();
  const dimension = rule.rule_dimension || "";
  const severity = rule.rule_severity || "";

  return (
    <Card>
      <CardContent className="py-3 space-y-2">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="min-w-0">
            <p className="font-medium text-sm truncate">{rule.rule_name || rule.rule_id}</p>
            <div className="flex flex-wrap gap-1 mt-1">
              <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
              <TagBadge label={severity} color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, severity)} />
            </div>
            <MappingChips columnMapping={rule.column_mapping ?? []} className="mt-2" />
          </div>
          {canEdit && (
            <div className="flex items-center gap-2 shrink-0">
              {busy ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
              ) : (
                <>
                  <Select value={rule.pinned_version == null ? "latest" : "pinned"} onValueChange={onPinChange}>
                    <SelectTrigger className="h-7 w-36 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="latest" className="text-xs">
                        {t("monitoredTables.pinFollowLatest")}
                      </SelectItem>
                      <SelectItem value="pinned" className="text-xs">
                        {t("monitoredTables.pinVersion", {
                          version: rule.pinned_version ?? registryRule?.version ?? 1,
                        })}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <Select value={rule.severity_override ?? "none"} onValueChange={onSeverityChange}>
                    <SelectTrigger className="h-7 w-32 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none" className="text-xs">
                        {t("monitoredTables.severityOverrideNone")}
                      </SelectItem>
                      {severityValues.map((v) => (
                        <SelectItem key={v} value={v} className="text-xs">
                          {v}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-destructive"
                    title={t("monitoredTables.removeRuleButton")}
                    onClick={onRemove}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                </>
              )}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
