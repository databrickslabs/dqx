/** Advisory grant prompt for the tables a rule joins or reads.
 *
 *  A cross-table rule runs as the task runner service principal, which reads each
 *  joined table itself. This checks those tables when the author picks or types
 *  them and offers the same one-click grants as the schedule editor. It never
 *  blocks saving the rule. Raw SQL is checked after the author pauses typing.
 */
import { ScheduleGrantWarning } from "@/components/common/ScheduleGrantWarning";
import { useReferenceGrantPreflight } from "@/components/schedules/useScheduleGrantPreflight";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { findReferenceTables } from "@/lib/refTableColumns";

/** How long a raw SQL rule must stay unchanged before its tables are checked. */
const SQL_CHECK_DELAY_MS = 800;

interface Props {
  /** Join targets the low-code builder picked (three-part names). */
  tableFqns?: string[];
  /** A raw SQL rule's text; its FROM / JOIN tables are checked too. */
  sqlQuery?: string;
}

export function ReferenceGrantPrompt({ tableFqns = [], sqlQuery = "" }: Props) {
  const debouncedSql = useDebouncedValue(sqlQuery, SQL_CHECK_DELAY_MS);
  // Only ask the server when there is something to check, so a plain predicate costs nothing.
  const hasTables = tableFqns.length > 0 || findReferenceTables(debouncedSql).length > 0;
  const preflight = useReferenceGrantPreflight(tableFqns, debouncedSql, hasTables);
  if (!hasTables || !preflight.hasGrantIssue) return null;
  return <ScheduleGrantWarning entity="reference" preflight={preflight} />;
}
