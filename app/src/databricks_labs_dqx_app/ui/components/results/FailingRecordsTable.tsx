import { useTranslation } from "react-i18next";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { FailingRecordOut } from "@/lib/api";
import {
  deriveRecordColumns,
  displayCellValue,
  failedCellClass,
  failureMessageForCell,
  wholeRowFailureClass,
  wholeRowFailureMessage,
} from "@/lib/results-display";

interface FailingRecordsTableProps {
  records: FailingRecordOut[];
  /**
   * True when the source table carries fine-grained access controls (row
   * filter / column mask) so the failing sample is withheld — renders an
   * explanatory notice instead of rows.
   */
  suppressed: boolean;
}

/**
 * Failing-records sample table: one row per quarantined record, failed
 * cells tinted red with the attributed rule message as a tooltip. A record
 * whose failure carries no column attribution is a whole-row rule — the
 * ENTIRE row is tinted (dqlake parity) with the failure message as a
 * row-level tooltip. All cell/column resolution lives in
 * lib/results-display.ts (unit-tested).
 */
export function FailingRecordsTable({ records, suppressed }: FailingRecordsTableProps) {
  const { t } = useTranslation();
  if (suppressed) {
    return <p className="text-sm text-muted-foreground">{t("results.suppressedFineGrainedControls")}</p>;
  }
  if (records.length === 0) {
    return <p className="text-sm text-muted-foreground">{t("results.noFailingRecords")}</p>;
  }
  const columns = deriveRecordColumns(records);
  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            {columns.map((col) => (
              <TableHead key={col}>{col}</TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {records.map((record) => (
            <TableRow
              key={record.record_key}
              className={wholeRowFailureClass(record)}
              title={wholeRowFailureMessage(record)}
            >
              {columns.map((col) => (
                <TableCell
                  key={col}
                  className={failedCellClass(record, col)}
                  title={failureMessageForCell(record, col)}
                >
                  {displayCellValue(record, col)}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
