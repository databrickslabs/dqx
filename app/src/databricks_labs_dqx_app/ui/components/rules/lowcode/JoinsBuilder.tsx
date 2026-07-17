import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Plus } from "lucide-react";
import { JoinBlock } from "./JoinBlock";
import type { JoinAst, LowcodeAstV2 } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  ast: LowcodeAstV2;
  onChange: (next: LowcodeAstV2) => void;
  declaredColumns: LowcodeColumnRef[];
  readOnly?: boolean;
};

// Ported from dqlake's JoinsBuilder. Adds join blocks (each with its own
// inline table picker via CatalogBrowser inside JoinBlock).
export function JoinsBuilder({ ast, onChange, declaredColumns, readOnly }: Props) {
  const { t } = useTranslation();

  const setJoin = (i: number, j: JoinAst) => {
    const next = ast.joins.slice();
    next[i] = j;
    onChange({ ...ast, joins: next });
  };

  const deleteJoin = (i: number) => {
    const next = ast.joins.slice();
    next.splice(i, 1);
    onChange({ ...ast, joins: next });
  };

  const addJoin = () => {
    onChange({
      ...ast,
      joins: [
        ...ast.joins,
        { join_type: "LEFT", target_table: "", keys: [{ joined_column: "", column_ref: declaredColumns[0]?.name ?? "" }] },
      ],
    });
  };

  return (
    <div className="space-y-3">
      <Label className="text-xs">{t("rulesRegistry.lowcodeJoinsLabel")}</Label>
      {ast.joins.map((j, i) => (
        <JoinBlock
          key={i}
          join={j}
          declaredColumns={declaredColumns}
          onChange={(next) => setJoin(i, next)}
          onDelete={() => deleteJoin(i)}
        />
      ))}
      {!readOnly && (
        <Button type="button" variant="outline" size="sm" className="text-xs" onClick={addJoin}>
          <Plus className="mr-1.5 h-3.5 w-3.5" /> {t("rulesRegistry.lowcodeAddJoin")}
        </Button>
      )}
    </div>
  );
}
