// RulesByColumn — the "by column" lens for a monitored table's applied
// rules: pivots the same AppliedRuleOut[] so each real table column shows
// which rules (and slots) are checking it. Read-only pivot; edits still
// happen via the by-rule lens / Add rules dialog.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Plus } from "lucide-react";
import type { AppliedRuleOut } from "@/lib/api";

interface ColumnUse {
  ruleName: string;
  slot: string;
}

interface RulesByColumnProps {
  appliedRules: AppliedRuleOut[];
  canEdit: boolean;
  onAddRule?: () => void;
}

export function RulesByColumn({ appliedRules, canEdit, onAddRule }: RulesByColumnProps) {
  const { t } = useTranslation();

  const byColumn = useMemo(() => {
    const map = new Map<string, ColumnUse[]>();
    for (const rule of appliedRules) {
      const name = rule.rule_name || rule.rule_id;
      for (const group of rule.column_mapping ?? []) {
        for (const [slot, column] of Object.entries(group)) {
          const list = map.get(column) ?? [];
          list.push({ ruleName: name, slot });
          map.set(column, list);
        }
      }
    }
    return map;
  }, [appliedRules]);

  return (
    <div className="space-y-2">
      {Array.from(byColumn.entries()).map(([column, uses]) => (
        <Card key={column}>
          <CardContent className="py-3">
            <p className="font-mono font-medium text-sm">{column}</p>
            <div className="flex flex-wrap gap-1 mt-1">
              {uses.map((u, i) => (
                <Badge key={i} variant="outline" className="text-[10px]">
                  {u.ruleName} ({u.slot})
                </Badge>
              ))}
            </div>
          </CardContent>
        </Card>
      ))}

      {canEdit && onAddRule && (
        <button
          type="button"
          onClick={onAddRule}
          className="w-full border border-dashed rounded-lg py-3 text-xs text-muted-foreground hover:text-foreground hover:border-foreground/40 transition-colors flex items-center justify-center gap-1.5"
        >
          <Plus className="h-3.5 w-3.5" />
          {t("monitoredTables.addRulesButton")}
        </button>
      )}
    </div>
  );
}
