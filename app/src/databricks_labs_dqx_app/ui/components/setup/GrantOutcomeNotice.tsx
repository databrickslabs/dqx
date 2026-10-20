import { AlertCircle } from "lucide-react";
import { useTranslation } from "react-i18next";

import { SqlCodeBlock } from "@/components/common/SqlCodeBlock";
import { grantErrorMessageKey, type GrantOutcomeCode, type GrantRunOutcome } from "@/lib/setup-grants";

type GrantOutcomeNoticeProps = {
  outcome: GrantRunOutcome;
  /** i18n key for the "Done: N of M." summary. */
  summaryKey?: string;
  /** Maps an error code to the i18n key of its message. */
  messageKey?: (code: GrantOutcomeCode) => string;
  /** Repeat the failed statements. Only when no code cell above already shows them. */
  showFailedSql?: boolean;
};

/** What happened when GRANT statements ran: how many ran and, in plain words, why the rest didn't. */
export function GrantOutcomeNotice({
  outcome,
  summaryKey = "setup.grants.partial",
  messageKey = grantErrorMessageKey,
  showFailedSql = false,
}: GrantOutcomeNoticeProps) {
  const { t } = useTranslation();

  return (
    <div
      role="alert"
      className="flex items-start gap-2 rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-foreground"
    >
      <AlertCircle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden="true" />
      <div className="min-w-0 flex-1 space-y-2">
        {outcome.summary && <p className="font-medium">{t(summaryKey, outcome.summary)}</p>}
        {outcome.errorCodes.map((code) => (
          <p key={code}>{t(messageKey(code))}</p>
        ))}
        {showFailedSql && outcome.failedSql && <SqlCodeBlock sql={outcome.failedSql} />}
      </div>
    </div>
  );
}
