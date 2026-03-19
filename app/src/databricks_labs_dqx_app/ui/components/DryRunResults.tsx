import { Badge } from "@/components/ui/badge";
import { CheckCircle2, XCircle, AlertTriangle } from "lucide-react";
import type { DryRunOut } from "@/lib/api";

interface DryRunResultsProps {
  result: DryRunOut;
}

export function DryRunResults({ result }: DryRunResultsProps) {
  const passRate =
    result.total_rows > 0
      ? Math.round((result.valid_rows / result.total_rows) * 100)
      : 0;

  return (
    <div className="space-y-4">
      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {result.total_rows}
          </div>
          <div className="text-xs text-muted-foreground">Total Rows</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-green-600">
            {result.valid_rows}
          </div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <CheckCircle2 className="h-3 w-3" />
            Valid
          </div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-red-600">
            {result.invalid_rows}
          </div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <XCircle className="h-3 w-3" />
            Invalid
          </div>
        </div>
      </div>

      {/* Pass rate bar */}
      <div className="space-y-1">
        <div className="flex items-center justify-between text-xs">
          <span className="text-muted-foreground">Pass rate</span>
          <span className="font-medium">{passRate}%</span>
        </div>
        <div className="h-2 rounded-full bg-muted overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${
              passRate >= 90
                ? "bg-green-500"
                : passRate >= 70
                  ? "bg-amber-500"
                  : "bg-red-500"
            }`}
            style={{ width: `${passRate}%` }}
          />
        </div>
      </div>

      {/* Error summary */}
      {result.error_summary.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium flex items-center gap-1.5">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            Error Summary
          </h4>
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-2 font-medium">Error</th>
                  <th className="text-right p-2 font-medium">Count</th>
                </tr>
              </thead>
              <tbody>
                {result.error_summary.map((item, idx) => (
                  <tr
                    key={idx}
                    className="border-b last:border-b-0"
                  >
                    <td className="p-2 text-muted-foreground">
                      {String(item.error ?? "")}
                    </td>
                    <td className="p-2 text-right tabular-nums">
                      <Badge variant="secondary" className="text-xs">
                        {String(item.count ?? 0)}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Sample invalid rows */}
      {result.sample_invalid.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium">
            Sample Invalid Rows ({result.sample_invalid.length})
          </h4>
          <div className="border rounded-lg overflow-auto max-h-64">
            <pre className="p-3 text-xs font-mono whitespace-pre-wrap">
              {JSON.stringify(result.sample_invalid, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
