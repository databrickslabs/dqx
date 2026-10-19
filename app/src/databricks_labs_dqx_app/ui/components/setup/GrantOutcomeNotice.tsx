import { useTranslation } from "react-i18next";

import { SqlCodeBlock } from "@/components/common/SqlCodeBlock";
import { grantErrorMessageKey, type GrantRunOutcome } from "@/lib/setup-grants";

/** What happened when GRANT statements ran: how many ran, which failed and why. */
export function GrantOutcomeNotice({ outcome }: { outcome: GrantRunOutcome }) {
  const { t } = useTranslation();

  return (
    <div
      role="alert"
      className="space-y-2 rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive"
    >
      {outcome.summary && (
        <p className="font-medium">
          {t("setup.grants.partial", outcome.summary)}
        </p>
      )}
      {outcome.failedSql && (
        <div className="text-foreground">
          <SqlCodeBlock sql={outcome.failedSql} />
        </div>
      )}
      {outcome.errorCodes.map((code) => (
        <p key={code}>{t(grantErrorMessageKey(code))}</p>
      ))}
    </div>
  );
}
