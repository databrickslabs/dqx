import { useQueries } from "@tanstack/react-query";
import { getTableColumns, getGetTableColumnsQueryKey } from "@/lib/api";
import type { JoinAst } from "@/lib/lowcodeAst";
import { slotFamilyToLowcode, type LowcodeColumnRef } from "@/lib/lowcodeCompile";
import { familyForType } from "@/components/apply-rules/ColumnPicker";

/**
 * Fetches each join's target-table columns and folds them into a flat list of
 * "virtual" column refs, qualified as `<catalog>.<schema>.<table>.<col>` so
 * the low-code AST treats them as joined-table references (compiled as raw SQL,
 * not as `{{name}}` placeholders). Ported from dqlake's `useJoinedColumns`,
 * sourcing columns from DQX's discovery API instead of UC directly.
 */
export function useJoinedColumns(joins: JoinAst[]): LowcodeColumnRef[] {
  const fqns = Array.from(new Set(joins.map((j) => j.target_table).filter(Boolean)));

  const results = useQueries({
    queries: fqns.map((fqn) => {
      const parts = fqn.split(".");
      const [catalog, schema, table] = parts.length === 3 ? parts : ["", "", ""];
      return {
        queryKey: getGetTableColumnsQueryKey(catalog, schema, table),
        queryFn: () => getTableColumns(catalog, schema, table),
        enabled: Boolean(catalog && schema && table),
        staleTime: 5 * 60 * 1000,
      };
    }),
  });

  const out: LowcodeColumnRef[] = [];
  fqns.forEach((fqn, i) => {
    const cols = results[i]?.data?.data ?? [];
    for (const c of cols) {
      out.push({ name: `${fqn}.${c.name}`, family: slotFamilyToLowcode(familyForType(c.type_name)) });
    }
  });
  return out;
}
