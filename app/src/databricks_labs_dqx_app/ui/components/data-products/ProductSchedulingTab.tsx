/** Ported from dqlake's `components/products/ProductSchedulingTab.tsx`.
 *  Adapted: the buffer is `useEditProductState`'s flat `scheduleCron` /
 *  `scheduleTz` pair (Task 7) rather than dqlake's nested `ScheduleBuffer`,
 *  and "Add schedule" seeds a concrete 5-field cron via `simpleToCron`
 *  instead of an empty-cron placeholder object. Invalid custom cron is
 *  reported up to `useEditProductState` (`setScheduleCronInvalid`) so the
 *  header's Save buttons stay disabled until it's fixed — see Task 9 brief.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { SchedulePicker } from "@/components/common/SchedulePicker";
import { simpleToCron } from "@/lib/cron";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

const DEFAULT_CADENCE_CRON = simpleToCron("daily", "06:00");
const DEFAULT_TZ = "UTC";

export function ProductSchedulingTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { scheduleCron, scheduleTz, setSchedule, setScheduleCronInvalid } = editState;
  const [editing, setEditing] = useState(false);

  const showEditor = scheduleCron !== null || editing;

  if (!showEditor) {
    return (
      <div className="space-y-4 max-w-xl">
        <p className="text-sm text-muted-foreground">{t("dataProducts.scheduleEmptyText")}</p>
        {canEdit && (
          <Button
            size="sm"
            onClick={() => {
              setEditing(true);
              setSchedule(DEFAULT_CADENCE_CRON, DEFAULT_TZ);
              setScheduleCronInvalid(false);
            }}
          >
            {t("dataProducts.scheduleAddButton")}
          </Button>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-xl">
      <SchedulePicker
        cron={scheduleCron ?? DEFAULT_CADENCE_CRON}
        timezone={scheduleTz}
        onChange={(cron, tz) => setSchedule(cron, tz)}
        onValidityChange={(valid) => setScheduleCronInvalid(!valid)}
        canEdit={canEdit}
      />

      {canEdit && (
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            setSchedule(null);
            setScheduleCronInvalid(false);
            setEditing(false);
          }}
        >
          {t("dataProducts.scheduleRemoveButton")}
        </Button>
      )}
      <p className="text-xs text-muted-foreground">{t("dataProducts.scheduleFooterNote")}</p>
    </div>
  );
}
